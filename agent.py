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
MSK = ZoneInfo(CFG.get("timezone", "Europe/Moscow"))

STATE_PATH = "state.json"
state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

state.setdefault("seen", {})
state.setdefault("scan_index", 0)
state.setdefault("daily_signals", [])
state.setdefault("name_cache", {})

WATCHLIST = set(CFG["watchlist"])

MIN_SCORE = int(CFG.get("min_score_to_alert", 7))
MAX_ALERTS = int(CFG.get("max_alerts_per_run", 8))

SCAN_PER_RUN = int(CFG.get("scan_per_run", 10))
CANDLE_INTERVAL = int(CFG.get("candle_interval_minutes", 10))
ANOMALY_VALUE_RATIO = float(CFG.get("anomaly_value_ratio", 3.0))
ANOMALY_CHANGE_PCT = float(CFG.get("anomaly_change_pct", 0.7))

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

UA = {"User-Agent": "rf-signal-agent/5.0 (+github actions)"}


# =========================
# Telegram
# =========================

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    limit = 3800
    parts = [text[i:i + limit] for i in range(0, len(text), limit)]
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
# Company names (для удобного заголовка)
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
    "POSI": "Positive Technologies",
}

def get_company_name(ticker: str) -> str:
    if ticker in COMPANY_NAMES:
        return COMPANY_NAMES[ticker]

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

def header_for(ticker: str) -> str:
    return f"{get_company_name(ticker)} ({ticker})"


# =========================
# News sources
# =========================

def fetch_moex_rss_all_news():
    urls = [
        "https://www.moex.com/export/news.aspx?cat=100",
        "https://moex.com/export/news.aspx?cat=100",
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
# Extract explicit tickers ONLY (гарантия от ВК/включит и T/T2)
# =========================

PAREN_TICKER = re.compile(r"$begin:math:text$\\s\*\(\[A\-Z0\-9\]\{1\,10\}\)\\s\*$end:math:text$")
TICKER_WORD = re.compile(r"\b(?:ТИКЕР|TICKER|SECID)\s*[:=]?\s*([A-Z0-9]{1,10})\b", re.IGNORECASE)

def extract_explicit_tickers(text: str):
    t = text.upper()

    found = set()

    # 1) (TICKER)
    for m in PAREN_TICKER.finditer(t):
        found.add(m.group(1).strip())

    # 2) ТИКЕР: XXX / SECID XXX
    for m in TICKER_WORD.finditer(t):
        found.add(m.group(1).strip().upper())

    # ВАЖНО: тикер T допускаем только если он реально был в скобках (T)
    if "T" in found:
        if "(T)" not in t.replace(" ", ""):
            found.discard("T")

    # Оставляем только то, что есть в watchlist
    out = [x for x in found if x in WATCHLIST]
    out.sort()
    return out


# =========================
# Classification
# =========================

POS_KW = ["дивиденд", "рекомендац", "выкуп", "байбек", "отчет", "результат", "прибыл", "рост", "прогноз повыш"]
NEG_KW = ["санкц", "убыт", "дефолт", "банкрот", "арест", "суд", "иск", "допэмисс", "spo", "размещение"]

# Режем мусор/служебку, которую ты показал
TECH_KW = [
    "кодов расчетов", "код(ов) расчет",
    "free-float", "коэффициент free-float", "коэффициентов free-float",
    "тестового полигона", "тестовый полигон", "spectra", "tks", "срочного рынка",
    "список ценных бумаг, допущенных к торгам", "о внесении изменений в список ценных бумаг",
    "о включении ценных бумаг в сектор компаний повышенного инвестиционного риска",
    "репо", "ставки переноса", "диапазона оценки", "процентных рисков", "риск-параметр", "клиринг",
    "сектор роста", "регламент"
]

def classify(title: str, summary: str):
    s = (title + " " + summary).lower()

    if any(k in s for k in TECH_KW):
        return "tech", "Служебная/техническая новость биржи — не торговый сигнал."

    pos = any(k in s for k in POS_KW)
    neg = any(k in s for k in NEG_KW)

    if pos and not neg:
        return "positive", "Похоже на позитив (может поддержать рост)."
    if neg and not pos:
        return "negative", "Похоже на негатив (может усилить продажи)."
    if pos and neg:
        return "mixed", "Смешанный сигнал: есть и плюсы, и риски."
    return "unknown", "Эффект неочевиден."

def score_news(kind: str) -> int:
    if kind in ("positive", "negative"):
        return 7
    if kind == "mixed":
        return 6
    if kind == "tech":
        return 2
    return 5


# =========================
# Market reaction
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
    try:
        candles = fetch_candles(ticker, CANDLE_INTERVAL, lookback_days=2)
    except Exception:
        return None
    if len(candles) < 30:
        return None

    last = candles[-1]
    start = candles[-5]  # ~40 минут назад

    last_close = float(last.get("close") or 0)
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

    median = sorted(values)[len(values)//2]
    ratio = (float(last.get("value") or 0) / median) if median > 0 else None
    return {"change_pct": change_pct, "ratio": ratio}

def fmt_rx(rx: dict) -> str:
    if not rx:
        return "Рынок: данных мало."
    ch = rx.get("change_pct", 0.0)
    ratio = rx.get("ratio")
    if ratio is None:
        return f"Рынок: {ch:+.1f}% за ~40 мин."
    return f"Рынок: {ch:+.1f}% за ~40 мин, активность ~x{ratio:.1f}."

def decide_action(kind: str, rx: dict) -> str:
    # максимально простое решение
    if kind == "positive":
        if rx and rx.get("ratio") and rx["ratio"] >= 1.7 and rx.get("change_pct", 0) > 0.4:
            return "Решение: ПОКУПКА (рынок подтверждает)"
        return "Решение: ЖДАТЬ (нет подтверждения рынком)"
    if kind == "negative":
        if rx and rx.get("ratio") and rx["ratio"] >= 1.7 and rx.get("change_pct", 0) < -0.4:
            return "Решение: НЕ ВХОДИТЬ / СОКРАТИТЬ (рынок подтверждает)"
        return "Решение: ЖДАТЬ (без паники)"
    if kind == "mixed":
        return "Решение: ЖДАТЬ (смешанный сигнал)"
    return "Решение: ЖДАТЬ"

def build_news_message(ticker: str, title: str, url: str, kind: str, why_line: str, score: int, rx: dict):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    decision = decide_action(kind, rx)

    if "ПОКУПКА" in decision:
        next_step = "Следующий шаг: если рост держится 30–60 минут — можно входить небольшой частью."
    elif "НЕ ВХОДИТЬ" in decision:
        next_step = "Следующий шаг: если падение продолжается и объём высокий — не входить / сократить часть позиции."
    else:
        next_step = "Следующий шаг: ничего не делать 30–60 минут, дождаться ясной реакции."

    return (
        f"📰 {header_for(ticker)} • {score}/10\n"
        f"{decision}\n"
        f"Время (MSK): {now}\n\n"
        f"Событие: {title}\n"
        f"Почему: {why_line}\n"
        f"{fmt_rx(rx)}\n\n"
        f"{next_step}\n"
        f"Стоп-сценарий: цена вернулась к уровню до новости и там удерживается.\n\n"
        f"Источник: {url}"
    )


# =========================
# Anomalies
# =========================

def market_anomaly(ticker: str):
    rx = market_reaction_now(ticker)
    if not rx or rx.get("ratio") is None:
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

def build_anomaly_message(ticker: str, sig: dict):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    return (
        f"⚡ {header_for(ticker)} • {sig['score']}/10\n"
        f"Решение: ОТКРЫТЬ ГРАФИК\n"
        f"Время (MSK): {now}\n\n"
        f"Сигнал: цена {sig['direction']} {sig['change_pct']:+.1f}%, активность ~x{sig['ratio']:.1f}\n"
        f"Следующий шаг: если движение не откатилось — можно рассмотреть вход/выход по твоей стратегии."
    )


# =========================
# Runs
# =========================

def run_monitor():
    sent = 0
    sent_tickers = set()

    # 1) News (ТОЛЬКО явные тикеры)
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

        tickers = extract_explicit_tickers(title + " " + summary)
        if not tickers:
            continue

        kind, why_line = classify(title, summary)
        if kind == "tech":
            continue

        score = score_news(kind)
        if score < MIN_SCORE:
            continue

        for t in tickers[:3]:
            if t in sent_tickers:
                continue

            rx = market_reaction_now(t)
            tg_send(build_news_message(t, title, link, kind, why_line, score, rx))

            add_daily_signal({
                "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
                "ticker": t,
                "company": get_company_name(t),
                "type": "news",
                "score": score,
                "title": title,
                "url": link
            })

            sent_tickers.add(t)
            sent += 1
            if sent >= MAX_ALERTS:
                save_state()
                return

    # 2) Anomaly scan (реальные рыночные сигналы)
    wl = sorted(list(WATCHLIST))
    n = len(wl)
    start = int(state.get("scan_index", 0)) % max(1, n)
    chunk = [wl[(start + i) % n] for i in range(min(SCAN_PER_RUN, n))]
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--digest", action="store_true")
    args = ap.parse_args()
    if args.digest:
        run_digest()
    else:
        run_monitor()

if __name__ == "__main__":
    main()
