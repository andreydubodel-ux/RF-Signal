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
# Config / state
# =========================

CFG = json.load(open("config.json", "r", encoding="utf-8"))
MSK = ZoneInfo(CFG.get("timezone", "Europe/Moscow"))

STATE_PATH = "state.json"
state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

state.setdefault("seen", {})
state.setdefault("scan_index", 0)
state.setdefault("daily_signals", [])
state.setdefault("name_cache", {})

# для “личного бота”
state.setdefault("tg_offset", 0)          # чтобы не читать одни и те же команды
state.setdefault("recent_signals", {})    # {id: payload}
state.setdefault("recent_order", [])      # [id1,id2,...]
state.setdefault("last_summary", {})      # последний итог run_monitor

WATCHLIST = set(CFG["watchlist"])

MIN_SCORE = int(CFG.get("min_score_to_alert", 7))
MAX_ALERTS = int(CFG.get("max_alerts_per_run", 8))

SCAN_PER_RUN = int(CFG.get("scan_per_run", 12))
CANDLE_INTERVAL = int(CFG.get("candle_interval_minutes", 10))

ANOMALY_VALUE_RATIO = float(CFG.get("anomaly_value_ratio", 3.0))
ANOMALY_CHANGE_PCT = float(CFG.get("anomaly_change_pct", 0.7))

CONFIRM_CANDLES = int(CFG.get("confirm_candles", 2))
RISK_BUFFER_PCT = float(CFG.get("risk_buffer_pct", 0.15))

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()  # может быть пустым на этапе настройки
EVENT_NAME = os.getenv("GITHUB_EVENT_NAME", "")

UA = {"User-Agent": "rf-signal-agent/8.0 (+github actions)"}


# =========================
# Telegram
# =========================

def tg_send(chat_id: int, text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    limit = 3800
    parts = [text[i:i + limit] for i in range(0, len(text), limit)]
    for part in parts:
        r = requests.post(url, json={"chat_id": chat_id, "text": part}, timeout=30)
        r.raise_for_status()

def tg_send_to_owner(text: str):
    if not TG_CHAT_ID:
        return
    tg_send(int(TG_CHAT_ID), text)


# =========================
# State helpers
# =========================

def save_state():
    json.dump(state, open(STATE_PATH, "w", encoding="utf-8"), ensure_ascii=False, indent=2)

def seen(key: str) -> bool:
    return key in state["seen"]

def mark_seen(key: str):
    state["seen"][key] = int(time.time())
    if len(state["seen"]) > 6000:
        items = sorted(state["seen"].items(), key=lambda x: x[1])
        for k, _ in items[:1500]:
            state["seen"].pop(k, None)

def add_recent_signal(sig_id: str, payload: dict):
    state["recent_signals"][sig_id] = payload
    state["recent_order"].append(sig_id)
    # держим последние 50
    while len(state["recent_order"]) > 50:
        old = state["recent_order"].pop(0)
        state["recent_signals"].pop(old, None)

def add_daily_signal(entry: dict):
    state["daily_signals"].append(entry)
    cutoff = datetime.now(MSK).date() - timedelta(days=7)
    state["daily_signals"] = [
        e for e in state["daily_signals"]
        if datetime.fromisoformat(e["ts_msk"]).date() >= cutoff
    ]


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
# RSS
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
# Explicit ticker extraction only
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

    if "T" in found:
        if "(T)" not in t.replace(" ", ""):
            found.discard("T")

    out = [x for x in found if x in WATCHLIST]
    out.sort()
    return out


# =========================
# Classification
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
# Market data
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

    recent = candles[-6:]
    highs = [float(c.get("high") or 0) for c in recent if float(c.get("high") or 0) > 0]
    lows = [float(c.get("low") or 0) for c in recent if float(c.get("low") or 0) > 0]
    high_recent = max(highs) if highs else price_now
    low_recent = min(lows) if lows else price_now

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
# Signal IDs / Explain
# =========================

def make_id(prefix: str, ticker: str, title: str) -> str:
    raw = f"{prefix}|{ticker}|{title}|{datetime.now(MSK).isoformat(timespec='minutes')}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6].upper()

def explain_signal(sig: dict) -> str:
    # максимально простое “почему” и “что делать”
    kind = sig.get("kind")
    ticker = sig.get("ticker")
    company = sig.get("company")
    ts = sig.get("ts_msk")
    snap = sig.get("snap") or {}
    base = snap.get("base")
    price_now = snap.get("price_now")
    ch = snap.get("change_pct")
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if isinstance(ratio, (int, float)) else "—"

    lines = []
    lines.append(f"📘 Объяснение по {company} ({ticker})")
    lines.append(f"Время (MSK): {ts}")
    lines.append("")
    if kind == "anomaly":
        lines.append("Что это было:")
        lines.append(f"- Цена сдвинулась {ch:+.1f}% за ~40 минут")
        lines.append(f"- Активность выросла до {ratio_txt} от обычной")
        lines.append("")
        lines.append("Что это обычно значит (простыми словами):")
        lines.append("- На рынке появился крупный покупатель/продавец или новость уже “разгоняет” движение.")
        lines.append("- Это НЕ команда купить/продать. Это повод открыть график и действовать по правилам.")
    else:
        lines.append("Что это было:")
        lines.append(f"- Новость: {sig.get('title')}")
        lines.append("- Смысл: агент оценил как событие, которое может сдвинуть цену (но подтверждаем ценой).")

    lines.append("")
    lines.append("Главное правило (чтобы не входить на шуме):")
    lines.append(f"- Подтверждение = {CONFIRM_CANDLES} свечи по {CANDLE_INTERVAL} минут закрылись по одну сторону от уровня старта.")

    if base and price_now:
        direction, entry, stop = build_levels(snap)
        lines.append("")
        lines.append("Уровни:")
        lines.append(f"- Сейчас: {fmt_price(price_now)} ₽")
        lines.append(f"- Старт импульса: {fmt_price(base)} ₽")
        lines.append(f"- Триггер: {fmt_price(entry)} ₽")
        lines.append(f"- Стоп: {fmt_price(stop)} ₽")

        lines.append("")
        lines.append("Как действовать:")
        if direction == "UP":
            lines.append(f"- Если НЕ в позиции: жди подтверждения и вход по триггеру {fmt_price(entry)} ₽.")
            lines.append(f"- Если В позиции: держи, но выход если {CONFIRM_CANDLES} свечи закрылись ниже {fmt_price(base)} ₽.")
            lines.append(f"- Шорт: только если цена вернулась ниже {fmt_price(base)} ₽ и там закрепилась.")
        else:
            lines.append(f"- Если НЕ в позиции: не покупай “в падение”. Жди возврата выше {fmt_price(base)} ₽.")
            lines.append(f"- Если В позиции: защити/сократи при уходе ниже {fmt_price(entry)} ₽.")
            lines.append(f"- Если шорт доступен: вход ниже {fmt_price(entry)} ₽, стоп выше {fmt_price(stop)} ₽.")

    lines.append("")
    lines.append("Когда это НЕ работает:")
    lines.append("- Цена быстро вернулась к уровню “до” и там удержалась.")
    lines.append("- Объём всплеснул на 1 свече и сразу пропал (ложный вынос).")

    return "\n".join(lines)


# =========================
# Message builders (простые и исполняемые)
# =========================

def build_anomaly_message(sig_id: str, ticker: str, snap: dict, score: int):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if ratio is not None else "—"

    direction, entry, stop = build_levels(snap)

    price_now = snap["price_now"]
    base = snap["base"]
    ch = snap["change_pct"]

    if direction == "UP":
        confirmed = "ДА" if snap["confirm_up"] else "НЕТ"
        action_not_in = f"НЕ в позиции: жди подтверждения → вход по {fmt_price(entry)} ₽"
        action_in = f"В позиции: держи; выход если {CONFIRM_CANDLES} свечи ниже {fmt_price(base)} ₽"
        extra = f"Шорт: только если вернулись ниже {fmt_price(base)} ₽."
    else:
        confirmed = "ДА" if snap["confirm_down"] else "НЕТ"
        action_not_in = f"НЕ в позиции: не покупай; жди возврата выше {fmt_price(base)} ₽"
        action_in = f"В позиции: сократи/защити при уходе ниже {fmt_price(entry)} ₽"
        extra = f"Шорт (если доступен): вход < {fmt_price(entry)} ₽, стоп > {fmt_price(stop)} ₽"

    return (
        f"⚡ {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
        f"Время (MSK): {now}\n\n"
        f"Что сейчас:\n"
        f"- Импульс {('вверх' if ch>0 else 'вниз')} {ch:+.1f}%\n"
        f"- Активность: {ratio_txt}\n\n"
        f"Уровни:\n"
        f"- Сейчас {fmt_price(price_now)} ₽ | Старт {fmt_price(base)} ₽\n"
        f"- Триггер {fmt_price(entry)} ₽ | Стоп {fmt_price(stop)} ₽\n\n"
        f"Подтверждение: {confirmed} ({CONFIRM_CANDLES} свечи {CANDLE_INTERVAL}м)\n\n"
        f"Действия:\n"
        f"- {action_not_in}\n"
        f"- {action_in}\n"
        f"- {extra}\n\n"
        f"Если непонятно: /explain {sig_id}"
    )

def build_news_message(sig_id: str, ticker: str, title: str, url: str, kind: str, why_line: str, score: int):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    snap = market_snapshot(ticker)

    if not snap:
        return (
            f"📰 {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
            f"Время (MSK): {now}\n\n"
            f"Новость: {title}\n"
            f"Смысл: {why_line}\n\n"
            f"Действие: открой график и жди подтверждения (2 свечи 10м).\n"
            f"Источник: {url}\n\n"
            f"/explain {sig_id}"
        )

    direction, entry, stop = build_levels(snap)
    base = snap["base"]
    ch = snap["change_pct"]
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if ratio is not None else "—"

    if direction == "UP":
        confirmed = "ДА" if snap["confirm_up"] else "НЕТ"
        actions = (
            f"- НЕ в позиции: подтверждение → вход по {fmt_price(entry)} ₽\n"
            f"- В позиции: держи; выход если {CONFIRM_CANDLES} свечи ниже {fmt_price(base)} ₽"
        )
    else:
        confirmed = "ДА" if snap["confirm_down"] else "НЕТ"
        actions = (
            f"- НЕ в позиции: не покупай; жди возврата выше {fmt_price(base)} ₽\n"
            f"- В позиции: сократи/защити при уходе ниже {fmt_price(entry)} ₽\n"
            f"- Шорт (если доступен): вход < {fmt_price(entry)} ₽, стоп > {fmt_price(stop)} ₽"
        )

    headline = "Суть: позитив" if kind == "positive" else ("Суть: негатив" if kind == "negative" else "Суть: эффект неясен")

    return (
        f"📰 {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
        f"Время (MSK): {now}\n\n"
        f"{headline}\n"
        f"Новость: {title}\n"
        f"Рынок: {ch:+.1f}% и активность {ratio_txt}\n"
        f"Подтверждение: {confirmed} ({CONFIRM_CANDLES} свечи {CANDLE_INTERVAL}м)\n\n"
        f"Уровни: старт {fmt_price(base)} ₽ | триггер {fmt_price(entry)} ₽ | стоп {fmt_price(stop)} ₽\n\n"
        f"Действия:\n{actions}\n\n"
        f"Источник: {url}\n\n"
        f"/explain {sig_id}"
    )


# =========================
# Monitor
# =========================

def run_monitor(manual: bool = False):
    dbg = {
        "rss": 0,
        "news_sent": 0,
        "anomaly_scanned": 0,
        "anomaly_sent": 0
    }
    sent = 0
    sent_tickers = set()

    # 1) News
    rss_entries = fetch_moex_rss_all_news()
    dbg["rss"] = len(rss_entries)

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

            sig_id = make_id("NEWS", t, title)
            msg = build_news_message(sig_id, t, title, link, kind, why_line, score)
            tg_send_to_owner(msg)

            snap = market_snapshot(t)
            add_recent_signal(sig_id, {
                "id": sig_id,
                "kind": "news",
                "ticker": t,
                "company": get_company_name(t),
                "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
                "title": title,
                "url": link,
                "snap": snap,
                "score": score
            })

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
            dbg["news_sent"] += 1
            if sent >= MAX_ALERTS:
                state["last_summary"] = {"ts_msk": datetime.now(MSK).isoformat(timespec="seconds"), **dbg, "sent": sent}
                save_state()
                return sent, dbg

    # 2) Anomalies
    wl = sorted(list(WATCHLIST))
    n = len(wl)
    start = int(state.get("scan_index", 0)) % max(1, n)
    chunk = [wl[(start + i) % n] for i in range(min(SCAN_PER_RUN, n))]
    state["scan_index"] = (start + SCAN_PER_RUN) % max(1, n)

    for t in chunk:
        dbg["anomaly_scanned"] += 1

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
        sig_id = make_id("ANOM", t, f"{snap['change_pct']:+.1f}")
        msg = build_anomaly_message(sig_id, t, snap, score)
        tg_send_to_owner(msg)

        add_recent_signal(sig_id, {
            "id": sig_id,
            "kind": "anomaly",
            "ticker": t,
            "company": get_company_name(t),
            "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
            "title": f"Импульс {snap['change_pct']:+.1f}% (x{snap['ratio']:.1f})",
            "url": "",
            "snap": snap,
            "score": score
        })

        add_daily_signal({
            "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
            "ticker": t,
            "company": get_company_name(t),
            "type": "anomaly",
            "score": score,
            "title": f"Импульс {snap['change_pct']:+.1f}% (x{snap['ratio']:.1f})",
            "url": ""
        })

        sent += 1
        dbg["anomaly_sent"] += 1
        if sent >= MAX_ALERTS:
            break

    state["last_summary"] = {"ts_msk": datetime.now(MSK).isoformat(timespec="seconds"), **dbg, "sent": sent}
    save_state()
    return sent, dbg


# =========================
# Bot listener (commands)
# =========================

def tg_get_updates(offset: int):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    params = {
        "offset": offset,
        "timeout": 20,
        "allowed_updates": ["message"]
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def help_text():
    return (
        "Команды:\n"
        "/run — запустить мониторинг вручную\n"
        "/status — статус последнего запуска\n"
        "/last — последние сигналы (ID)\n"
        "/explain — объяснить последний сигнал\n"
        "/explain ABC123 — объяснить сигнал по ID\n"
        "/whoami — показать твой chat_id\n\n"
        "Важно: это не автоторговля. Это подсказки по рынку, решение — на тебе."
    )

def status_text():
    s = state.get("last_summary") or {}
    if not s:
        return "Статус: ещё не было запусков."
    return (
        "Статус последнего запуска:\n"
        f"- Время (MSK): {s.get('ts_msk','—')}\n"
        f"- Отправлено: {s.get('sent',0)}\n"
        f"- RSS новостей: {s.get('rss',0)} | news_sent: {s.get('news_sent',0)}\n"
        f"- Аномалии: проверено {s.get('anomaly_scanned',0)} | отправлено {s.get('anomaly_sent',0)}"
    )

def last_text():
    order = state.get("recent_order", [])
    if not order:
        return "Пока нет сохранённых сигналов."
    last_ids = order[-8:][::-1]
    lines = ["Последние сигналы:"]
    for sid in last_ids:
        sig = state["recent_signals"].get(sid, {})
        lines.append(f"- {sid} | {sig.get('company','')} ({sig.get('ticker','')}) | {sig.get('kind','')}")
    lines.append("\n/explain <ID> — чтобы разобрать любой из них.")
    return "\n".join(lines)

def run_listen_once():
    # один цикл: прочитать апдейты -> обработать команды -> сохранить offset
    data = tg_get_updates(int(state.get("tg_offset", 0)))
    if not data.get("ok"):
        return

    updates = data.get("result", []) or []
    if not updates:
        return

    max_update_id = None
    do_run = False

    for upd in updates:
        max_update_id = max(max_update_id or upd["update_id"], upd["update_id"])
        msg = upd.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip()

        if not chat_id or not text:
            continue

        # /whoami можно из любого чата (чтобы настроить TG_CHAT_ID)
        if text.lower().startswith("/whoami"):
            tg_send(chat_id, f"Твой chat_id: {chat_id}\nЕсли хочешь личную работу — поставь это число в секрет TG_CHAT_ID.")
            continue

        # остальное — только владельцу
        if TG_CHAT_ID and int(chat_id) != int(TG_CHAT_ID):
            continue

        cmd = text.split()[0].lower()

        if cmd in ("/start", "/help"):
            tg_send(chat_id, help_text())

        elif cmd == "/status":
            tg_send(chat_id, status_text())

        elif cmd == "/last":
            tg_send(chat_id, last_text())

        elif cmd == "/run":
            do_run = True
            tg_send(chat_id, "Ок. Запускаю мониторинг. Пришлю результат.")

        elif cmd == "/explain":
            parts = text.split()
            if len(parts) >= 2:
                sid = parts[1].strip().upper()
            else:
                order = state.get("recent_order", [])
                sid = order[-1] if order else ""
            sig = state.get("recent_signals", {}).get(sid)
            if not sig:
                tg_send(chat_id, "Не нашёл сигнал. Используй /last чтобы посмотреть ID.")
            else:
                tg_send(chat_id, explain_signal(sig))

        else:
            tg_send(chat_id, "Не понял команду. Напиши /help")

    # обновляем offset
    if max_update_id is not None:
        state["tg_offset"] = int(max_update_id) + 1
        save_state()

    if do_run:
        sent, dbg = run_monitor(manual=True)
        if sent == 0:
            tg_send_to_owner(
                "ℹ️ Мониторинг выполнен — сигналов не найдено.\n"
                f"Новости RSS: {dbg.get('rss',0)}, отправлено новостей: {dbg.get('news_sent',0)}\n"
                f"Аномалии: проверено {dbg.get('anomaly_scanned',0)}, отправлено: {dbg.get('anomaly_sent',0)}"
            )


# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", action="store_true")
    args = ap.parse_args()

    if args.listen:
        run_listen_once()
    else:
        run_monitor(manual=False)

if __name__ == "__main__":
    main()
