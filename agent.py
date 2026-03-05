import os
import json
import time
import hashlib
import argparse
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any, Tuple, List

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

# Telegram long polling
state.setdefault("tg_offset", 0)

# Explain storage
state.setdefault("recent_signals", {})   # {id: payload}
state.setdefault("recent_order", [])     # [id1,id2,...]
state.setdefault("last_summary", {})     # summary of last run

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
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "").strip()  # owner chat id (личка)
UA = {"User-Agent": "rf-signal-agent/9.0 (+github actions)"}


# =========================
# Telegram API helpers
# =========================

def tg_send(chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None):
    """Send message to chat_id. Can attach reply keyboard."""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    limit = 3800
    parts = [text[i:i + limit] for i in range(0, len(text), limit)]
    for part in parts:
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": part}
        if reply_markup:
            payload["reply_markup"] = reply_markup
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()

def owner_id() -> Optional[int]:
    if not TG_CHAT_ID:
        return None
    try:
        return int(TG_CHAT_ID)
    except Exception:
        return None

def tg_send_owner(text: str, reply_markup: Optional[Dict[str, Any]] = None):
    oid = owner_id()
    if oid is None:
        return
    tg_send(oid, text, reply_markup=reply_markup)

def tg_get_updates(offset: int):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates"
    params = {"offset": offset, "timeout": 20, "allowed_updates": ["message"]}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def tg_set_my_commands():
    """Commands shown in Telegram '/' menu."""
    url = f"https://api.telegram.org/bot{TG_TOKEN}/setMyCommands"
    commands = [
        {"command": "run", "description": "Ручной мониторинг"},
        {"command": "status", "description": "Статус последнего запуска"},
        {"command": "last", "description": "Последние сигналы (ID)"},
        {"command": "explain", "description": "Объяснить последний сигнал / по ID"},
        {"command": "test", "description": "Тест: бот жив"},
        {"command": "menu", "description": "Показать кнопки"},
        {"command": "hide", "description": "Спрятать кнопки"},
        {"command": "whoami", "description": "Показать chat_id"},
        {"command": "help", "description": "Справка"},
    ]
    r = requests.post(url, json={"commands": commands}, timeout=30)
    r.raise_for_status()


# =========================
# Keyboards (buttons)
# =========================

def main_menu_keyboard():
    # ReplyKeyboardMarkup
    return {
        "keyboard": [
            [{"text": "🚀 Запуск /run"}, {"text": "🧪 Тест /test"}],
            [{"text": "📊 Статус /status"}, {"text": "📌 Последние /last"}],
            [{"text": "📘 Объяснить /explain"}, {"text": "❓ Помощь /help"}],
            [{"text": "🆔 Кто я /whoami"}, {"text": "🙈 Спрятать /hide"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True
    }

def remove_keyboard():
    return {"remove_keyboard": True}


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

def add_daily_signal(entry: dict):
    state["daily_signals"].append(entry)
    cutoff = datetime.now(MSK).date() - timedelta(days=7)
    state["daily_signals"] = [
        e for e in state["daily_signals"]
        if datetime.fromisoformat(e["ts_msk"]).date() >= cutoff
    ]

def add_recent_signal(sig_id: str, payload: dict):
    state["recent_signals"][sig_id] = payload
    state["recent_order"].append(sig_id)
    # keep last 50
    while len(state["recent_order"]) > 50:
        old = state["recent_order"].pop(0)
        state["recent_signals"].pop(old, None)


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
# Explicit ticker extraction ONLY (решает VK/включит и T/T2)
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

    # тикер T разрешаем только как (T)
    if "T" in found and "(T)" not in t.replace(" ", ""):
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

def fmt_price(p: Optional[float]) -> str:
    if p is None:
        return "—"
    s = f"{p:.2f}".rstrip("0").rstrip(".")
    return s

def price_with_buffer(price: float, pct: float, up: bool) -> float:
    return price * (1 + pct / 100.0) if up else price * (1 - pct / 100.0)

def floor_time_bucket(dt: datetime, minutes: int) -> str:
    m = (dt.minute // minutes) * minutes
    return dt.replace(minute=m, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")

def market_snapshot(ticker: str) -> Optional[Dict[str, Any]]:
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

def build_levels(snap: Dict[str, Any]) -> Tuple[str, float, float]:
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

def anomaly_score_from_snapshot(snap: Dict[str, Any]) -> Optional[int]:
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
# IDs / explain
# =========================

def make_id(prefix: str, ticker: str, title: str) -> str:
    raw = f"{prefix}|{ticker}|{title}|{datetime.now(MSK).isoformat(timespec='minutes')}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:6].upper()

def explain_signal(sig: Dict[str, Any]) -> str:
    ticker = sig.get("ticker", "")
    company = sig.get("company", "")
    ts = sig.get("ts_msk", "")
    kind = sig.get("kind", "")
    snap = sig.get("snap") or {}

    lines = []
    lines.append(f"📘 Объяснение: {company} ({ticker})")
    lines.append(f"Время (MSK): {ts}")
    lines.append("")

    if kind == "anomaly":
        lines.append("Что это значит:")
        lines.append("- Цена/объём ведут себя необычно → кто-то крупный двигает рынок или идёт реакция на событие.")
        lines.append("- Это НЕ автокоманда. Это повод открыть график и действовать по правилам ниже.")
    else:
        lines.append("Что это значит:")
        lines.append("- Появилась новость по бумаге. Действуем только если рынок подтвердил движением.")

    lines.append("")
    lines.append(f"Подтверждение = {CONFIRM_CANDLES} свечи по {CANDLE_INTERVAL} минут закрылись по одну сторону от уровня старта.")

    if snap:
        direction, entry, stop = build_levels(snap)
        base = snap.get("base")
        price_now = snap.get("price_now")
        ratio = snap.get("ratio")
        ratio_txt = f"x{ratio:.1f}" if isinstance(ratio, (int, float)) else "—"
        ch = snap.get("change_pct", 0.0)

        lines.append("")
        lines.append("Ситуация сейчас:")
        lines.append(f"- Цена: {fmt_price(price_now)} ₽")
        lines.append(f"- Изменение: {ch:+.1f}% за ~40 мин")
        lines.append(f"- Активность: {ratio_txt}")
        lines.append("")
        lines.append("Уровни:")
        lines.append(f"- Старт: {fmt_price(base)} ₽")
        lines.append(f"- Триггер: {fmt_price(entry)} ₽")
        lines.append(f"- Стоп: {fmt_price(stop)} ₽")
        lines.append("")
        lines.append("Как действовать:")
        if direction == "UP":
            lines.append(f"- НЕ в позиции: жди подтверждения и вход по {fmt_price(entry)} ₽.")
            lines.append(f"- В позиции: держи; выход если {CONFIRM_CANDLES} свечи закрылись ниже {fmt_price(base)} ₽.")
            lines.append(f"- Шорт: только если цена вернулась ниже {fmt_price(base)} ₽ и закрепилась.")
        else:
            lines.append(f"- НЕ в позиции: не покупай на падении; жди возврата выше {fmt_price(base)} ₽.")
            lines.append(f"- В позиции: сократи/защити при уходе ниже {fmt_price(entry)} ₽.")
            lines.append(f"- Шорт (если доступен): вход < {fmt_price(entry)} ₽, стоп > {fmt_price(stop)} ₽.")

    lines.append("")
    lines.append("Когда сигнал часто ложный:")
    lines.append("- Цена быстро вернулась к уровню старта и удержалась там.")
    lines.append("- Всплеск был на одной свече и сразу пропал.")
    return "\n".join(lines)


# =========================
# Messages (простые и “исполняемые”)
# =========================

def build_anomaly_message(sig_id: str, ticker: str, snap: Dict[str, Any], score: int) -> str:
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    ratio = snap.get("ratio")
    ratio_txt = f"x{ratio:.1f}" if ratio is not None else "—"

    direction, entry, stop = build_levels(snap)

    price_now = snap["price_now"]
    base = snap["base"]
    ch = snap["change_pct"]

    if direction == "UP":
        confirmed = "ДА" if snap["confirm_up"] else "НЕТ"
        a1 = f"НЕ в позиции: подтверждение → вход по {fmt_price(entry)} ₽"
        a2 = f"В позиции: держи; выход если {CONFIRM_CANDLES} свечи ниже {fmt_price(base)} ₽"
        a3 = f"Шорт: только если вернулись ниже {fmt_price(base)} ₽."
    else:
        confirmed = "ДА" if snap["confirm_down"] else "НЕТ"
        a1 = f"НЕ в позиции: не покупай; жди возврата выше {fmt_price(base)} ₽"
        a2 = f"В позиции: сократи/защити при уходе ниже {fmt_price(entry)} ₽"
        a3 = f"Шорт: вход < {fmt_price(entry)} ₽, стоп > {fmt_price(stop)} ₽"

    return (
        f"⚡ {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
        f"Время (MSK): {now}\n\n"
        f"Что сейчас: {('вверх' if ch>0 else 'вниз')} {ch:+.1f}% • активность {ratio_txt}\n"
        f"Уровни: сейчас {fmt_price(price_now)} ₽ | старт {fmt_price(base)} ₽ | триггер {fmt_price(entry)} ₽ | стоп {fmt_price(stop)} ₽\n"
        f"Подтверждение: {confirmed} ({CONFIRM_CANDLES} свечи {CANDLE_INTERVAL}м)\n\n"
        f"Действия:\n- {a1}\n- {a2}\n- {a3}\n\n"
        f"Если непонятно: /explain {sig_id}"
    )

def build_news_message(sig_id: str, ticker: str, title: str, url: str, kind: str, why_line: str, score: int) -> str:
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    snap = market_snapshot(ticker)

    headline = "Суть: позитив" if kind == "positive" else ("Суть: негатив" if kind == "negative" else "Суть: эффект неясен")

    if not snap:
        return (
            f"📰 {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
            f"Время (MSK): {now}\n\n"
            f"{headline}\n"
            f"Новость: {title}\n"
            f"Смысл: {why_line}\n\n"
            f"Действие: открой график и жди подтверждения (2 свечи 10м).\n"
            f"Источник: {url}\n\n"
            f"/explain {sig_id}"
        )

    direction, entry, stop = build_levels(snap)
    base = snap["base"]
    price_now = snap["price_now"]
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
            f"- Шорт: вход < {fmt_price(entry)} ₽, стоп > {fmt_price(stop)} ₽"
        )

    return (
        f"📰 {header_for(ticker)} • {score}/10 • ID {sig_id}\n"
        f"Время (MSK): {now}\n\n"
        f"{headline}\n"
        f"Новость: {title}\n"
        f"Рынок: сейчас {fmt_price(price_now)} ₽ | {ch:+.1f}% • активность {ratio_txt}\n"
        f"Подтверждение: {confirmed} ({CONFIRM_CANDLES} свечи {CANDLE_INTERVAL}м)\n"
        f"Уровни: старт {fmt_price(base)} ₽ | триггер {fmt_price(entry)} ₽ | стоп {fmt_price(stop)} ₽\n\n"
        f"Действия:\n{actions}\n\n"
        f"Источник: {url}\n\n"
        f"/explain {sig_id}"
    )


# =========================
# Monitor
# =========================

def run_monitor(manual: bool = False) -> Tuple[int, Dict[str, Any]]:
    """
    manual=True: по запросу /run -> в конце шлём итог даже если сигналов нет
    manual=False: авто-мониторинг по расписанию -> молчим если 0 сигналов
    """
    dbg: Dict[str, Any] = {
        "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
        "rss": 0,
        "news_sent": 0,
        "anomaly_scanned": 0,
        "anomaly_sent": 0,
        "sent": 0
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
            tg_send_owner(msg)

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
                break

        if sent >= MAX_ALERTS:
            break

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
        tg_send_owner(msg)

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

    dbg["sent"] = sent
    state["last_summary"] = dbg
    save_state()

    if manual:
        tg_send_owner(
            "✅ Ручной запуск завершён.\n"
            f"- Отправлено сигналов: {sent}\n"
            f"- RSS новостей: {dbg['rss']} | новостей отправлено: {dbg['news_sent']}\n"
            f"- Аномалии: проверено {dbg['anomaly_scanned']} | отправлено: {dbg['anomaly_sent']}"
        )

    return sent, dbg


# =========================
# Test / heartbeat
# =========================

def run_test():
    """
    Всегда отправляет сообщение (чтобы ты понимал: бот жив, токен работает, GitHub Actions крутится).
    Плюс делает 1 лёгкую проверку данных: сколько RSS-новостей и есть ли цена по первой бумаге.
    """
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M:%S")
    rss = fetch_moex_rss_all_news()
    rss_n = len(rss)

    sample = sorted(list(WATCHLIST))[0] if WATCHLIST else "SBER"
    snap = market_snapshot(sample)
    if snap:
        sample_line = f"{header_for(sample)}: сейчас {fmt_price(snap.get('price_now'))} ₽, {snap.get('change_pct',0):+.1f}%"
    else:
        sample_line = f"{header_for(sample)}: цену получить не удалось (может быть ночь/нет данных/сеть)."

    last = state.get("last_summary") or {}
    last_line = f"Последний запуск: {last.get('ts_msk','—')}, sent={last.get('sent','—')}"

    tg_send_owner(
        "🧪 ТЕСТ: бот жив.\n"
        f"Время (MSK): {now}\n"
        f"RSS новостей сейчас: {rss_n}\n"
        f"Проверка данных: {sample_line}\n"
        f"{last_line}\n\n"
        "Если всё это пришло — бот точно не умер."
    )


# =========================
# Bot commands
# =========================

def help_text():
    return (
        "Команды (можно кнопками):\n"
        "/run — ручной мониторинг\n"
        "/status — статус последнего запуска\n"
        "/last — последние сигналы (ID)\n"
        "/explain — объяснить последний сигнал\n"
        "/explain ABC123 — объяснить сигнал по ID\n"
        "/test — тест: бот жив\n"
        "/menu — показать кнопки\n"
        "/hide — спрятать кнопки\n"
        "/whoami — показать chat_id\n"
    )

def status_text():
    s = state.get("last_summary") or {}
    if not s:
        return "Статус: запусков ещё не было. Нажми /run или /test."
    return (
        "Статус последнего запуска:\n"
        f"- Время (MSK): {s.get('ts_msk','—')}\n"
        f"- Отправлено: {s.get('sent',0)}\n"
        f"- RSS: {s.get('rss',0)} | news_sent: {s.get('news_sent',0)}\n"
        f"- Аномалии: проверено {s.get('anomaly_scanned',0)} | отправлено: {s.get('anomaly_sent',0)}"
    )

def last_text():
    order = state.get("recent_order", [])
    if not order:
        return "Пока нет сохранённых сигналов. Нажми /run и дождись сигналов."
    last_ids = order[-8:][::-1]
    lines = ["Последние сигналы:"]
    for sid in last_ids:
        sig = state["recent_signals"].get(sid, {})
        lines.append(f"- {sid} | {sig.get('company','')} ({sig.get('ticker','')}) | {sig.get('kind','')}")
    lines.append("\n/explain <ID> — чтобы разобрать любой.")
    return "\n".join(lines)

def extract_cmd(text: str) -> str:
    # вытащим /команду из "🚀 Запуск /run"
    m = re.search(r"(/[a-z_]+)", text.lower())
    if m:
        return m.group(1)
    return (text.split()[0].lower() if text.split() else "")

def run_listen_once(auto_monitor: bool):
    data = tg_get_updates(int(state.get("tg_offset", 0)))
    if not data.get("ok"):
        return

    updates = data.get("result", []) or []
    do_run = False
    do_test = False
    do_help = False
    explain_id: Optional[str] = None

    max_update_id = None

    for upd in updates:
        max_update_id = max(max_update_id or upd["update_id"], upd["update_id"])
        msg = upd.get("message") or {}
        chat = msg.get("chat") or {}
        chat_id = chat.get("id")
        text = (msg.get("text") or "").strip()

        if not chat_id or not text:
            continue

        # /whoami — можно из любого чата, чтобы узнать личный chat_id
        if extract_cmd(text) == "/whoami":
            tg_send(chat_id, f"Твой chat_id: {chat_id}\nПоставь его в секрет TG_CHAT_ID, чтобы бот работал в личке.")
            continue

        oid = owner_id()
        if oid is not None and int(chat_id) != int(oid):
            # игнорируем чужие чаты
            continue

        cmd = extract_cmd(text)

        if cmd in ("/start", "/help"):
            do_help = True

        elif cmd == "/menu":
            tg_send(chat_id, "Меню включено.", reply_markup=main_menu_keyboard())

        elif cmd == "/hide":
            tg_send(chat_id, "Ок, спрятал кнопки.", reply_markup=remove_keyboard())

        elif cmd == "/status":
            tg_send(chat_id, status_text(), reply_markup=main_menu_keyboard())

        elif cmd == "/last":
            tg_send(chat_id, last_text(), reply_markup=main_menu_keyboard())

        elif cmd == "/run":
            tg_send(chat_id, "Ок. Запускаю мониторинг. Пришлю итог.", reply_markup=main_menu_keyboard())
            do_run = True

        elif cmd == "/test":
            tg_send(chat_id, "Ок. Делаю тест и пришлю результат.", reply_markup=main_menu_keyboard())
            do_test = True

        elif cmd == "/explain":
            parts = text.split()
            if len(parts) >= 2:
                explain_id = parts[1].strip().upper()
            else:
                order = state.get("recent_order", [])
                explain_id = order[-1].upper() if order else ""
            if not explain_id:
                tg_send(chat_id, "Нет сигналов для объяснения. Сначала /run.", reply_markup=main_menu_keyboard())

        else:
            tg_send(chat_id, "Не понял. Нажми кнопку или напиши /help", reply_markup=main_menu_keyboard())

    if max_update_id is not None:
        state["tg_offset"] = int(max_update_id) + 1
        save_state()

    if do_help:
        try:
            tg_set_my_commands()
        except Exception:
            pass
        tg_send_owner(help_text(), reply_markup=main_menu_keyboard())

    if explain_id:
        sig = state.get("recent_signals", {}).get(explain_id)
        if not sig:
            tg_send_owner("Не нашёл ID. Нажми /last чтобы увидеть список.", reply_markup=main_menu_keyboard())
        else:
            tg_send_owner(explain_signal(sig), reply_markup=main_menu_keyboard())

    if do_test:
        run_test()

    if do_run:
        run_monitor(manual=True)

    # авто-мониторинг по расписанию: молчим, если 0 сигналов
    if auto_monitor and not do_run:
        run_monitor(manual=False)


# =========================
# Main
# =========================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", action="store_true")
    ap.add_argument("--auto", action="store_true", help="Run monitoring every listen cycle (silent if no signals)")
    ap.add_argument("--test", action="store_true")
    ap.add_argument("--run", action="store_true")
    args = ap.parse_args()

    if args.test:
        run_test()
        return

    if args.run:
        run_monitor(manual=True)
        return

    if args.listen:
        run_listen_once(auto_monitor=args.auto)
        return

    # default: single auto monitor
    run_monitor(manual=False)


if __name__ == "__main__":
    main()
