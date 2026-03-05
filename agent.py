import os
import json
import time
import hashlib
import argparse
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import feedparser


# =========================
# Load config / state
# =========================

CFG = json.load(open("config.json", "r", encoding="utf-8"))
ALIASES = json.load(open("aliases.json", "r", encoding="utf-8"))

MSK = ZoneInfo(CFG.get("timezone", "Europe/Moscow"))

STATE_PATH = "state.json"
state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

state.setdefault("seen", {})
state.setdefault("scan_index", 0)
state.setdefault("daily_signals", [])
state.setdefault("name_cache", {})  # {ticker: {"name": str, "ts": int}}

WATCHLIST = CFG["watchlist"]
MIN_SCORE = int(CFG.get("min_score_to_alert", 7))
MAX_ALERTS = int(CFG.get("max_alerts_per_run", 8))

SCAN_PER_RUN = int(CFG.get("scan_per_run", 10))
CANDLE_INTERVAL = int(CFG.get("candle_interval_minutes", 10))
ANOMALY_VALUE_RATIO = float(CFG.get("anomaly_value_ratio", 3.0))
ANOMALY_CHANGE_PCT = float(CFG.get("anomaly_change_pct", 0.7))

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

UA = {"User-Agent": "rf-signal-agent/3.0 (+github actions)"}


# =========================
# Company names (чтобы не ориентироваться по тикеру)
# =========================

COMPANY_NAMES = {
    "SBER": "Сбербанк",
    "SBERP": "Сбербанк (преф)",
    "GAZP": "Газпром",
    "LKOH": "ЛУКОЙЛ",
    "ROSN": "Роснефть",
    "NVTK": "НОВАТЭК",
    "GMKN": "Норникель",
    "PLZL": "Полюс",
    "CHMF": "Северсталь",
    "NLMK": "НЛМК",
    "MGNT": "Магнит",
    "MTSS": "МТС",
    "MOEX": "Московская биржа",
    "YDEX": "Яндекс",
    "T": "Т-Технологии",
    "VTBR": "ВТБ",
    "AFKS": "АФК Система",
    "SNGS": "Сургутнефтегаз",
    "SNGSP": "Сургутнефтегаз (преф)",
    "TATN": "Татнефть",
    "TATNP": "Татнефть (преф)",
    "RUAL": "Русал",
    "ALRS": "АЛРОСА",
    "HYDR": "РусГидро",
    "IRAO": "Интер РАО",
    "FLOT": "Совкомфлот",
    "TRNFP": "Транснефть (преф)",
    "OZON": "Ozon",
    "VKCO": "VK",
    "POSI": "Positive Technologies"
}


# =========================
# Telegram
# =========================

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    limit = 3800
    parts = [text[i:i+limit] for i in range(0, len(text), limit)]
    for part in parts:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": part}, timeout=30)
        r.raise_for_status()


# =========================
# State helpers
# =========================

def seen(key: str) -> bool:
    return key in state["seen"]

def mark_seen(key: str):
    state["seen"][key] = int(time.time())
    if len(state["seen"]) > 4000:
        items = sorted(state["seen"].items(), key=lambda x: x[1])
        for k, _ in items[:800]:
            state["seen"].pop(k, None)

def add_daily_signal(entry: dict):
    state["daily_signals"].append(entry)
    cutoff = datetime.now(MSK).date() - timedelta(days=7)
    state["daily_signals"] = [
        e for e in state["daily_signals"]
        if datetime.fromisoformat(e["ts_msk"]).date() >= cutoff
    ]

def save_state():
    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


# =========================
# MOEX sources
# =========================

def fetch_moex_rss_all_news():
    urls = [
        "https://www.moex.com/export/news.aspx?cat=100",
        "https://moex.com/export/news.aspx?cat=100"
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=UA, timeout=30)
            r.raise_for_status()
            return feedparser.parse(r.text).entries
        except Exception:
            pass
    return []

def stable_key_from_news(title: str, link: str, published: str):
    base = f"{title}|{link}|{published}".encode("utf-8")
    return hashlib.sha1(base).hexdigest()


# =========================
# Company name (fallback from MOEX ISS)
# =========================

def get_company_name(ticker: str) -> str:
    # 1) локально (быстро и стабильно)
    if ticker in COMPANY_NAMES:
        return COMPANY_NAMES[ticker]

    # 2) кэш ISS на 30 дней
    cache = state.get("name_cache", {})
    item = cache.get(ticker)
    now_ts = int(time.time())
    if item and (now_ts - int(item.get("ts", 0)) < 30 * 24 * 3600) and item.get("name"):
        return item["name"]

    try:
        url = f"https://iss.moex.com/iss/securities/{ticker}.json"
        params = {"iss.only": "securities"}
        r = requests.get(url, params=params, headers=UA, timeout=30)
        r.raise_for_status()
        j = r.json()
        block = j.get("securities", {})
        cols = block.get("columns", [])
        rows = block.get("data", []) or []
        if not rows:
            name = ticker
        else:
            d = dict(zip(cols, rows[0]))
            name = d.get("shortname") or d.get("secname") or d.get("name") or ticker
            name = str(name).strip()

        cache[ticker] = {"name": name, "ts": now_ts}
        state["name_cache"] = cache
        return name
    except Exception:
        return ticker


# =========================
# Matching tickers (главный фикс: "ВК" больше не матчится в "включит")
# =========================

ALNUM = r"A-ZА-ЯЁ0-9"  # используем uppercase строку

def needs_word_boundary(alias: str) -> bool:
    """
    Для коротких алиасов (2-5 символов) обязательно границы слова,
    иначе будет куча ложных срабатываний ("включит" -> "ВК").
    """
    a = alias.strip()
    if len(a) <= 1:
        return False
    if len(a) <= 5:
        # если только буквы/цифры (латиница/кириллица) — считаем "коротким словом"
        return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁё0-9]+", a))
    return False

def match_tickers(text: str):
    src = text.upper()
    hits = []

    for sec in WATCHLIST:
        aliases = ALIASES.get(sec, [sec])
        for a in aliases:
            a_up = a.upper().strip()
            if not a_up:
                continue

            if needs_word_boundary(a_up):
                # отдельное слово: слева/справа не должно быть букв/цифр
                pattern = rf"(?<![{ALNUM}]){re.escape(a_up)}(?![{ALNUM}])"
                if re.search(pattern, src):
                    hits.append(sec)
                    break
            else:
                if a_up in src:
                    hits.append(sec)
                    break

    return list(dict.fromkeys(hits))


# =========================
# Classification / scoring
# =========================

POS_KW = ["дивиденд", "рекомендац", "выкуп", "байбек", "отчет", "результат", "прибыл", "рост", "прогноз повыш"]
NEG_KW = ["санкц", "огранич", "убыт", "дефолт", "банкрот", "арест", "суд", "иск", "допэмисс", "spo", "размещение"]

TECH_KW = [
    "ценового коридора репо", "репо", "ставки переноса", "диапазона оценки",
    "процентных рисков", "параметров риск", "риск-параметр",
    "сектор роста", "включении ценных бумаг в сектор роста",
    "список ценных бумаг, допущенных к торгам",
    "о внесении изменений в список ценных бумаг",
    "о включении ценных бумаг в сектор компаний повышенного инвестиционного риска",
    "регламент", "клиринг"
]

def classify(title: str, summary: str):
    s = (title + " " + summary).lower()

    if any(k in s for k in TECH_KW):
        return "tech", "Это служебная (техническая) новость биржи — как правило, не торговый сигнал."

    pos = any(k in s for k in POS_KW)
    neg = any(k in s for k in NEG_KW)

    if pos and not neg:
        return "positive", "Новость выглядит позитивной — может поддержать рост."
    if neg and not pos:
        return "negative", "Новость выглядит негативной — может усилить продажи."
    if pos and neg:
        return "mixed", "Новость смешанная: есть и плюсы, и риски."
    return "unknown", "Эффект новости неочевиден."

def score_news(kind: str) -> int:
    if kind in ("positive", "negative"):
        return 7
    if kind == "mixed":
        return 6
    if kind == "tech":
        return 2
    return 5


# =========================
# Market data (candles)
# =========================

def fetch_candles(ticker: str, interval_minutes: int, lookback_days: int = 2):
    till = datetime.now(timezone.utc)
    frm = till - timedelta(days=lookback_days)

    url = f"https://iss.moex.com/iss/engines/stock/markets/shares/securities/{ticker}/candles.json"
    params = {
        "from": frm.strftime("%Y-%m-%d %H:%M:%S"),
        "till": till.strftime("%Y-%m-%d %H:%M:%S"),
        "interval": interval_minutes
    }
    r = requests.get(url, params=params, headers=UA, timeout=30)
    r.raise_for_status()
    j = r.json()

    block = j.get("candles", {})
    cols = block.get("columns", [])
    rows = block.get("data", []) or []
    return [dict(zip(cols, row)) for row in rows]

def market_reaction_now(ticker: str):
    """
    Реакция сейчас (примерно за ~40-50 минут):
    - изменение цены
    - насколько активность выше обычной (по обороту value)
    """
    try:
        candles = fetch_candles(ticker, CANDLE_INTERVAL, lookback_days=2)
    except Exception:
        return None

    if len(candles) < 30:
        return None

    last = candles[-1]
    last_close = float(last.get("close") or 0)
    last_value = float(last.get("value") or 0)

    # ~40 минут назад (4 свечи по 10м + текущая)
    start = candles[-5]
    start_close = float(start.get("close") or 0)
    if start_close <= 0 or last_close <= 0:
        return None

    change_pct = (last_close - start_close) / start_close * 100.0

    values = []
    for c in candles[-25:-2]:
        v = float(c.get("value") or 0)
        if v > 0:
            values.append(v)

    if len(values) < 10:
        return {"change_pct": change_pct, "ratio": None}

    values_sorted = sorted(values)
    median = values_sorted[len(values_sorted)//2]
    ratio = (last_value / median) if median > 0 else None

    return {"change_pct": change_pct, "ratio": ratio}

def market_anomaly(ticker: str):
    rx = market_reaction_now(ticker)
    if not rx or rx["ratio"] is None:
        return None

    change_pct = rx["change_pct"]
    ratio = rx["ratio"]

    if ratio >= ANOMALY_VALUE_RATIO and abs(change_pct) >= ANOMALY_CHANGE_PCT:
        score = 7
        if ratio >= 6: score += 1
        if abs(change_pct) >= 1.2: score += 1
        if abs(change_pct) >= 2.0: score += 1
        return {
            "score": min(10, score),
            "change_pct": change_pct,
            "ratio": ratio,
            "direction": "вверх" if change_pct > 0 else "вниз"
        }
    return None


# =========================
# Messages (понятно что делать)
# =========================

def header_for(ticker: str) -> str:
    return f"{get_company_name(ticker)} ({ticker})"

def fmt_rx_line(rx: dict) -> str:
    if not rx:
        return "Реакция рынка сейчас: данных мало."
    ch = rx.get("change_pct")
    ratio = rx.get("ratio")
    if ratio is None:
        return f"Реакция рынка сейчас: {ch:+.1f}% за ~40 мин, активность оценить не удалось."
    return f"Реакция рынка сейчас: {ch:+.1f}% за ~40 мин, активность ~x{ratio:.1f}."

def decide_action(kind: str, rx: dict):
    """
    Делаем решение более “земным”:
    - Если новость позитивная, но рынок не подтверждает — ждём.
    - Если подтверждает (движение + активность) — можно действовать.
    """
    if kind == "tech":
        return "Сейчас: ничего не делать (служебная новость)."

    if not rx:
        return "Сейчас: подождать подтверждения рынком."

    ch = rx.get("change_pct", 0.0) or 0.0
    ratio = rx.get("ratio")
    strong = (ratio is not None and ratio >= 1.7 and abs(ch) >= 0.4)

    if kind == "positive":
        return "Сейчас: можно рассмотреть покупку." if (strong and ch > 0) else "Сейчас: подождать подтверждения (рынок пока не поддержал)."
    if kind == "negative":
        return "Сейчас: лучше не входить / если держишь — подумать о сокращении." if (strong and ch < 0) else "Сейчас: подождать подтверждения (без паники)."
    if kind == "mixed":
        return "Сейчас: подождать (смешанный сигнал)."
    return "Сейчас: подождать."

def build_news_message(ticker: str, title: str, url: str, kind: str, why_line: str, score: int, rx: dict):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    action = decide_action(kind, rx)
    rx_line = fmt_rx_line(rx)

    if "покупку" in action:
        how = (
            "- Если НЕ держишь: вход только если рост удерживается 30–60 минут.\n"
            "- Если УЖЕ держишь: можно держать, на резких рывках вверх — фиксировать часть."
        )
    elif "лучше не входить" in action:
        how = (
            "- Если НЕ держишь: не покупай “дешево” сразу — дождись стабилизации.\n"
            "- Если УЖЕ держишь: если падение продолжается — сократи часть."
        )
    else:
        how = (
            "- Если НЕ держишь: пока ничего не делай.\n"
            "- Если УЖЕ держишь: не суетись, жди ясной реакции."
        )

    return (
        f"📰 {header_for(ticker)}\n"
        f"{action} • {score}/10\n"
        f"Время (MSK): {now}\n\n"
        f"Что случилось:\n{title}\n\n"
        f"Почему так:\n- {why_line}\n- {rx_line}\n\n"
        f"Как действовать:\n{how}\n\n"
        f"Когда это похоже на ошибку:\n"
        f"- Вышло уточнение/опровержение.\n"
        f"- Цена вернулась к уровню до новости и там держится.\n\n"
        f"Источник: {url}"
    )

def build_anomaly_message(ticker: str, sig: dict):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    return (
        f"⚡ {header_for(ticker)}\n"
        f"Сейчас: повод открыть график • {sig['score']}/10\n"
        f"Время (MSK): {now}\n\n"
        f"Что видно:\n"
        f"- Цена резко пошла {sig['direction']} ({sig['change_pct']:+.1f}%).\n"
        f"- Активность сделок выше обычного (~x{sig['ratio']:.1f}).\n\n"
        f"Как действовать:\n"
        f"- Если НЕ в позиции: не входи сразу. Подожди, что движение не откатилось.\n"
        f"- Если в позиции: в плюс — можно фиксировать часть, в минус — подумай о сокращении."
    )


# =========================
# Runs
# =========================

def run_monitor():
    sent = 0

    # 1) News
    rss_entries = fetch_moex_rss_all_news()
    for e in rss_entries:
        title = (e.get("title") or "").strip()
        summary = (e.get("summary") or "").strip()
        link = (e.get("link") or "").strip()
        published = (e.get("published") or "").strip()

        key = "news:" + stable_key_from_news(title, link, published)
        if seen(key):
            continue
        mark_seen(key)

        tickers = match_tickers(title + " " + summary)
        if not tickers:
            continue

        kind, why_line = classify(title, summary)
        if kind == "tech":
            continue  # тех. новости не шлём как сигнал

        base_score = score_news(kind)
        if base_score < MIN_SCORE:
            continue

        # для “понятности” считаем реакцию рынка на каждую бумагу
        for t in tickers[:3]:
            rx = market_reaction_now(t)
            msg = build_news_message(t, title, link, kind, why_line, base_score, rx)
            tg_send(msg)

            add_daily_signal({
                "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
                "ticker": t,
                "company": get_company_name(t),
                "type": "news",
                "score": base_score,
                "title": title,
                "url": link
            })

            sent += 1
            if sent >= MAX_ALERTS:
                save_state()
                return

    # 2) Market anomalies (rotate)
    n = len(WATCHLIST)
    start = int(state.get("scan_index", 0)) % max(1, n)
    chunk = [WATCHLIST[(start + i) % n] for i in range(min(SCAN_PER_RUN, n))]
    state["scan_index"] = (start + SCAN_PER_RUN) % max(1, n)

    for t in chunk:
        key = f"anomaly:{t}:{datetime.now(MSK).strftime('%Y-%m-%d %H:%M')}"
        if seen(key):
            continue

        sig = market_anomaly(t)
        if not sig or sig["score"] < MIN_SCORE:
            continue

        mark_seen(key)
        tg_send(build_anomaly_message(t, sig))

        add_daily_signal({
            "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
            "ticker": t,
            "company": get_company_name(t),
            "type": "anomaly",
            "score": sig["score"],
            "title": f"Движение {sig['direction']} {sig['change_pct']:+.1f}% (активность x{sig['ratio']:.1f})",
            "url": ""
        })

        sent += 1
        if sent >= MAX_ALERTS:
            break

    save_state()

def run_digest():
    today = datetime.now(MSK).date()
    items = [e for e in state["daily_signals"] if datetime.fromisoformat(e["ts_msk"]).date() == today]

    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    if not items:
        tg_send(f"📌 Дайджест (MSK) {now}\nСегодня сигналов не было.")
        save_state()
        return

    items_sorted = sorted(items, key=lambda x: (x["score"], x["ts_msk"]), reverse=True)
    top = items_sorted[:12]

    lines = [f"📌 Дайджест (MSK) {now}", f"Сигналов за день: {len(items)}", ""]
    for e in top:
        ts = datetime.fromisoformat(e["ts_msk"]).strftime("%H:%M")
        header = f"{e.get('company', e['ticker'])} ({e['ticker']})"
        url = f" | {e['url']}" if e.get("url") else ""
        lines.append(f"- {ts} {header} — {e['type']} ({e['score']}/10): {e['title']}{url}")

    tg_send("\n".join(lines))
    save_state()

def run_demo():
    # чтобы ты мог сразу увидеть новый формат, даже если новостей нет
    t = "VKCO" if "VKCO" in WATCHLIST else WATCHLIST[0]
    demo_msg = (
        f"🧪 ДЕМО-ФОРМАТ\n\n"
        f"📰 {header_for(t)}\n"
        f"Сейчас: подождать подтверждения • 7/10\n"
        f"Время (MSK): {datetime.now(MSK).strftime('%Y-%m-%d %H:%M')}\n\n"
        f"Что случилось:\n(пример) Компания опубликовала новость.\n\n"
        f"Почему так:\n- (пример) Это может повлиять на спрос.\n- Реакция рынка сейчас: +0.6% за ~40 мин, активность ~x2.1.\n\n"
        f"Как действовать:\n"
        f"- Если НЕ держишь: вход только если рост удерживается 30–60 минут.\n"
        f"- Если УЖЕ держишь: можно держать, на рывках вверх — фиксировать часть."
    )
    tg_send(demo_msg)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--digest", action="store_true")
    ap.add_argument("--demo", action="store_true")
    args = ap.parse_args()

    if args.demo:
        run_demo()
    elif args.digest:
        run_digest()
    else:
        run_monitor()

if __name__ == "__main__":
    main()
