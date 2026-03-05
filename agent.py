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

SCAN_PER_RUN = int(CFG.get("scan_per_run", 12))
CANDLE_INTERVAL = int(CFG.get("candle_interval_minutes", 10))

ANOMALY_VALUE_RATIO = float(CFG.get("anomaly_value_ratio", 3.0))
ANOMALY_CHANGE_PCT = float(CFG.get("anomaly_change_pct", 0.7))

# Главное требование: “закрепление = 2 свечи”
CONFIRM_CANDLES = int(CFG.get("confirm_candles", 2))

# Буфер вокруг уровней (в процентах)
RISK_BUFFER_PCT = float(CFG.get("risk_buffer_pct", 0.15))  # 0.15%

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]
UA = {"User-Agent": "rf-signal-agent/7.0 (+github actions)"}


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
    if len(state["seen"]) > 6000:
        items = sorted(state["seen"].items(), key=lambda x: x[1])
        for k, _ in items[:1500]:
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
# Names
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
# RSS (MOEX)
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
# Extract explicit tickers only (защита от ВК/включит и T/T2)
# =========================

PAREN_TICKER = re.compile(r"\(\s*([A-Z0-9]{1,10})\s*\)")
TICKER_WORD = re.compile(r"\b(?:ТИКЕР|TICKER|SECID)\s*[:=]?\s*([A-Z0-9]{1,10})\b", re.IGNORECASE)

def extract_explicit_tickers(text: str):
    t = text.upper()
    found = set()

    for m in PAREN_TICKER.finditer(t):
        found.add(m.group(1).strip())

    for m in TICKER_WORD.finditer(t):
        found.add(m.group(1).strip().upper())

    # тикер T допускаем только как (T)
    if "T" in found:
        if "(T)" not in t.replace(" ", ""):
            found.discard("T")

    out = [x for x in found if x in WATCHLIST]
    out.sort()
    return out


# =========================
# Classification (режем служебку)
# =========================

POS_KW = ["дивиденд", "рекомендац", "выкуп", "байбек", "отчет", "результат", "прибыл", "рост", "прогноз повыш"]
NEG_KW = ["санкц", "убыт", "дефолт", "банкрот", "арест", "суд", "иск", "допэмисс", "spo", "размещение"]

TECH_KW = [
    "кодов расчетов", "free-float", "коэффициент free-float", "коэффициентов free-float",
    "тестового полигона", "spectra", "tks", "срочного рынка",
    "список ценных бумаг, допущенных к торгам", "о внесении изменений в список ценных бумаг",
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
        return "positive", "Похоже на позитив."
    if neg and not pos:
        return "negative", "Похоже на негатив."
    if pos and neg:
        return "mixed", "Смешанный эффект."
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

def fmt_price(p: float) -> str:
    if p is None:
        return "—"
    s = f"{p:.2f}"
    s = s.rstrip("0").rstrip(".")
    return s

def price_with_buffer(price: float, pct: float, up: bool) -> float:
    return price * (1 + pct / 100.0) if up else price * (1 - pct / 100.0)

def floor_time_bucket(dt: datetime, minutes: int) -> str:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=m, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

def market_snapshot(ticker: str):
    """
    Возвращает уровни + подтверждение.
    base = close ~40 минут назад (4 свечи по 10м + текущая)
    confirm = последние CONFIRM_CANDLES закрылись выше/ниже base
    """
    try:
        candles = fetch_candles(ticker, CANDLE_INTERVAL, lookback_days=2)
    except Exception:
        return None

    if len(candles) < 40:
        return None

    last = candles[-1]
    base_c = candles[-5]

    price_now = float(last.get("close") or 0)
    base = float(base_c.get("close") or 0)
    if price_now <= 0 or base <= 0:
        return None

    change_pct = (price_now - base) / base * 100.0

    recent = candles[-6:]  # ~50-60 минут
    highs = [float(c.get("high") or 0) for c in recent if float(c.get("high") or 0) > 0]
    lows = [float(c.get("low") or 0) for c in recent if float(c.get("low") or 0) > 0]
    high_recent = max(highs) if highs else price_now
    low_recent = min(lows) if lows else price_now

    # активность
    values = []
    for c in candles[-25:-2]:
        v = float(c.get("value") or 0)
        if v > 0:
            values.append(v)

    ratio = None
    if len(values) >= 10:
        median = sorted(values)[len(values) // 2]
        if median > 0:
            ratio = float(last.get("value") or 0) / median

    # подтверждение 2 свечи
    n = max(1, CONFIRM_CANDLES)
    last_closes = [float(c.get("close") or 0) for c in candles[-n:]]
    confirm_up = all(c > base for c in last_closes if c > 0)
    confirm_down = all(c < base for c in last_closes if c > 0)

    return {
        "price_now": price_now,
        "base": base,
        "high_recent": high_recent,
        "low_recent": low_recent,
        "change_pct": change_pct,
        "ratio": ratio,
        "confirm_up": confirm_up,
        "confirm_down": confirm_down
    }

def build_levels(snap: dict):
    ch = snap["change_pct"]
    base = snap["base"]
    hi = snap["high_recent"]
    lo = snap["low_recent"]

    if ch >= 0:
        entry = price_with_buffer(hi, RISK_BUFFER_PCT, up=True)
        stop = price_with_buffer(base, RISK_BUFFER_PCT, up=False)
        return "UP", entry, stop
    else:
        entry = price_with_buffer(lo, RISK_BUFFER_PCT, up=False)
        stop = price_with_buffer(base, RISK_BUFFER_PCT, up=True)
        return "DOWN", entry, stop

def anomaly_score_from_snapshot(snap: dict):
    ratio = snap.get("ratio")
    ch = abs(snap.get("change_pct", 0))
    if ratio is None:
        return None
    if ratio >= ANOMALY_VALUE_RATIO and ch >= ANOMALY_CHANGE_PCT:
        score = 7
        if ratio >= 6: score += 1
        if ch >= 1.2: score += 1
        if ch >= 2.0: score += 1
        return min(10, score)
    return None


# =========================
# Message builders (максимально простой формат)
# =========================

def build_anomaly_message(ticker: str, snap: dict, score: int):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if ratio is not None else "—"

    direction, entry, stop = build_levels(snap)

    price_now = snap["price_now"]
    base = snap["base"]
    ch = snap["change_pct"]

    # подтверждение
    if direction == "UP":
        confirmed = "ДА" if snap["confirm_up"] else "НЕТ"
    else:
        confirmed = "ДА" if snap["confirm_down"] else "НЕТ"

    confirm_text = f"Подтверждение: {confirmed} (2 свечи {CANDLE_INTERVAL}м закрылись {'выше' if direction=='UP' else 'ниже'} {fmt_price(base)} ₽)"

    if direction == "UP":
        action_not_in = f"Если НЕ в позиции: жди подтверждения и вход по {fmt_price(entry)} ₽"
        action_in = f"Если В позиции: держи, но выход если 2 свечи закрылись ниже {fmt_price(base)} ₽"
        short_line = f"Шорт: обычно не нужен. Только если цена вернулась ниже {fmt_price(base)} ₽."
    else:
        action_not_in = f"Если НЕ в позиции: НЕ покупай. Жди возврата выше {fmt_price(base)} ₽"
        action_in = f"Если В позиции: сократи/защити при уходе ниже {fmt_price(entry)} ₽"
        short_line = f"Шорт (если доступен): вход ниже {fmt_price(entry)} ₽, стоп выше {fmt_price(stop)} ₽"

    return (
        f"⚡ {header_for(ticker)} • {score}/10\n"
        f"Время (MSK): {now}\n\n"
        f"Что сейчас:\n"
        f"- Импульс {('вверх' if ch>0 else 'вниз')} {ch:+.1f}% за ~40 мин\n"
        f"- Активность: {ratio_txt}\n\n"
        f"Уровни:\n"
        f"- Сейчас: {fmt_price(price_now)} ₽\n"
        f"- Старт: {fmt_price(base)} ₽\n"
        f"- Триггер: {fmt_price(entry)} ₽\n"
        f"- Стоп: {fmt_price(stop)} ₽\n\n"
        f"{confirm_text}\n\n"
        f"Действия:\n"
        f"- {action_not_in}\n"
        f"- {action_in}\n"
        f"- {short_line}"
    )

def build_news_message(ticker: str, title: str, url: str, kind: str, why_line: str, score: int):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    snap = market_snapshot(ticker)

    if not snap:
        return (
            f"📰 {header_for(ticker)} • {score}/10\n"
            f"Время (MSK): {now}\n\n"
            f"Новость: {title}\n"
            f"Смысл: {why_line}\n\n"
            f"Действие: открой график и жди 2 свечи подтверждения движения.\n"
            f"Источник: {url}"
        )

    direction, entry, stop = build_levels(snap)
    base = snap["base"]
    ch = snap["change_pct"]
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if ratio is not None else "—"

    if direction == "UP":
        confirmed = "ДА" if snap["confirm_up"] else "НЕТ"
    else:
        confirmed = "ДА" if snap["confirm_down"] else "НЕТ"

    confirm_text = f"Подтверждение: {confirmed} (2 свечи {CANDLE_INTERVAL}м закрылись {'выше' if direction=='UP' else 'ниже'} {fmt_price(base)} ₽)"

    # максимально простое “что делать”
    if kind == "positive":
        headline = "Суть: позитив → покупка только после подтверждения"
    elif kind == "negative":
        headline = "Суть: негатив → защита/шорт только после подтверждения"
    else:
        headline = "Суть: эффект неясен → лучше ждать"

    if direction == "UP":
        actions = (
            f"- Если НЕ в позиции: вход по {fmt_price(entry)} ₽ после подтверждения\n"
            f"- Если В позиции: держи, но выход если 2 свечи закрылись ниже {fmt_price(base)} ₽"
        )
    else:
        actions = (
            f"- Если НЕ в позиции: не покупай. Жди возврата выше {fmt_price(base)} ₽\n"
            f"- Если В позиции: защити/сократи при уходе ниже {fmt_price(entry)} ₽\n"
            f"- Шорт (если доступен): вход ниже {fmt_price(entry)} ₽, стоп выше {fmt_price(stop)} ₽"
        )

    return (
        f"📰 {header_for(ticker)} • {score}/10\n"
        f"Время (MSK): {now}\n\n"
        f"{headline}\n"
        f"Новость: {title}\n"
        f"Рынок: {ch:+.1f}% за ~40 мин, активность {ratio_txt}\n"
        f"{confirm_text}\n\n"
        f"Уровни: старт {fmt_price(base)} ₽ | триггер {fmt_price(entry)} ₽ | стоп {fmt_price(stop)} ₽\n\n"
        f"Действия:\n{actions}\n\n"
        f"Источник: {url}"
    )


# =========================
# Runs
# =========================

def run_monitor():
    sent = 0
    sent_tickers = set()

    # 1) News (только явные тикеры)
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

            tg_send(build_news_message(t, title, link, kind, why_line, score))

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

    # 2) Anomaly scan
    wl = sorted(list(WATCHLIST))
    n = len(wl)
    start = int(state.get("scan_index", 0)) % max(1, n)
    chunk = [wl[(start + i) % n] for i in range(min(SCAN_PER_RUN, n))]
    state["scan_index"] = (start + SCAN_PER_RUN) % max(1, n)

    for t in chunk:
        bucket = floor_time_bucket(datetime.now(MSK), CANDLE_INTERVAL)
        key = f"anomaly:{t}:{bucket}"
        if seen(key):
            continue

        snap = market_snapshot(t)
        if not snap:
            continue

        score = anomaly_score_from_snapshot(snap)
        if not score or score < MIN_SCORE:
            continue

        mark_seen(key)
        tg_send(build_anomaly_message(t, snap, score))

        add_daily_signal({
            "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
            "ticker": t,
            "company": get_company_name(t),
            "type": "anomaly",
            "score": score,
            "title": f"Импульс {snap['change_pct']:+.1f}% (активность x{snap['ratio']:.1f})",
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
