import os
import json
import time
import hashlib
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import feedparser

CFG = json.load(open("config.json", "r", encoding="utf-8"))
ALIASES = json.load(open("aliases.json", "r", encoding="utf-8"))

MSK = ZoneInfo(CFG.get("timezone", "Europe/Moscow"))

STATE_PATH = "state.json"
state = json.load(open(STATE_PATH, "r", encoding="utf-8"))

WATCHLIST = CFG["watchlist"]
MIN_SCORE = int(CFG.get("min_score_to_alert", 7))
MAX_ALERTS = int(CFG.get("max_alerts_per_run", 8))

SCAN_PER_RUN = int(CFG.get("scan_per_run", 10))
CANDLE_INTERVAL = int(CFG.get("candle_interval_minutes", 10))
ANOMALY_VALUE_RATIO = float(CFG.get("anomaly_value_ratio", 3.0))
ANOMALY_CHANGE_PCT = float(CFG.get("anomaly_change_pct", 0.7))

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

UA = {"User-Agent": "rf-signal-agent/1.0 (+github actions)"}

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    limit = 3900
    parts = [text[i:i+limit] for i in range(0, len(text), limit)]
    for part in parts:
        r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": part}, timeout=30)
        r.raise_for_status()

def seen(key: str) -> bool:
    return key in state["seen"]

def mark_seen(key: str):
    state["seen"][key] = int(time.time())
    if len(state["seen"]) > 3000:
        items = sorted(state["seen"].items(), key=lambda x: x[1])
        for k, _ in items[:500]:
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

def fetch_moex_rss_all_news():
    urls = [
        "https://moex.com/export/news.aspx?cat=100",
        "https://www.moex.com/export/news.aspx?cat=100"
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

POS_KW = ["дивиденд", "рекомендац", "выкуп", "байбек", "отчет", "результат", "прибыл", "рост"]
NEG_KW = ["санкц", "огранич", "убыт", "дефолт", "банкрот", "приостанов", "арест", "суд", "иск", "допэмисс", "spo"]
TECH_KW = [
    "ценового коридора репо", "репо", "ставки переноса", "диапазона оценки",
    "процентных рисков", "параметров риск", "клиринг", "торговый календарь",
    "сектор роста", "включении ценных бумаг в сектор роста", "регламент",
    "изменены значения", "коридора", "ставки", "риск-параметров"
]
def match_tickers(text: str):
    t = text.upper()
    hits = []
    for sec in WATCHLIST:
        aliases = ALIASES.get(sec, [sec])
        for a in aliases:
            if a.upper() in t:
                hits.append(sec)
                break
    return list(dict.fromkeys(hits))

def classify(title: str, summary: str):
    s = (title + " " + summary).lower()
    pos = any(k in s for k in POS_KW)
    neg = any(k in s for k in NEG_KW)
    if pos and not neg:
        return "positive", "Новость выглядит позитивной."
    if neg and not pos:
        return "negative", "Новость выглядит негативной."
    if pos and neg:
        return "mixed", "Новость смешанная: есть и плюсы, и риски."
    return "unknown", "Эффект новости неочевиден."

def score_news(kind: str):
    if kind in ("positive", "negative"):
        return 7
    if kind == "mixed":
        return 6
    return 5

def fetch_candles(ticker: str, interval_minutes: int, lookback_days: int = 3):
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

def market_anomaly(ticker: str):
    try:
        candles = fetch_candles(ticker, CANDLE_INTERVAL, lookback_days=3)
    except Exception:
        return None
    if len(candles) < 30:
        return None

    last = candles[-1]
    prev = candles[-2]

    last_close = float(last.get("close") or 0)
    prev_close = float(prev.get("close") or 0)
    last_value = float(last.get("value") or 0)

    if prev_close <= 0 or last_close <= 0:
        return None

    change_pct = (last_close - prev_close) / prev_close * 100.0

    values = []
    for c in candles[-25:-2]:
        v = float(c.get("value") or 0)
        if v > 0:
            values.append(v)
    if len(values) < 10:
        return None

    values_sorted = sorted(values)
    median = values_sorted[len(values_sorted)//2]
    if median <= 0:
        return None

    ratio = last_value / median

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

def build_news_message(ticker: str, title: str, url: str, kind: str, why_line: str, score: int):
    if kind == "positive":
        action = "Присмотреться к покупке"
    elif kind == "negative":
        action = "Сократить / не входить"
    else:
        action = "Подождать"

    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    return (
        f"📰 {ticker} — {action} ({score}/10)\n"
        f"Время (MSK): {now}\n\n"
        f"Что случилось: {title}\n\n"
        f"Почему это важно:\n"
        f"- {why_line}\n"
        f"- Такие новости часто двигают цену, но лучше действовать, когда видно реакцию рынка.\n\n"
        f"Что делать:\n"
        f"- Если уже держишь: не суетись, оцени, усилились ли покупки/продажи.\n"
        f"- Если не держишь: не входи “наугад” — дождись, что цена не откатила обратно.\n\n"
        f"Когда идея может быть ошибкой:\n"
        f"- Появилось уточнение/опровержение.\n"
        f"- Цена быстро вернулась к уровню до новости и там удерживается.\n\n"
        f"Источник: {url}"
    )

def build_anomaly_message(ticker: str, sig: dict):
    now = datetime.now(MSK).strftime("%Y-%m-%d %H:%M")
    return (
        f"⚡ {ticker} — похоже на сильное движение ({sig['score']}/10)\n"
        f"Время (MSK): {now}\n\n"
        f"Что видно:\n"
        f"- Цена резко пошла {sig['direction']} ({sig['change_pct']:.1f}%).\n"
        f"- Активность сделок выросла примерно в {sig['ratio']:.1f} раза относительно обычного.\n\n"
        f"Почему это важно:\n"
        f"- Часто так начинается движение на новости или из-за сильного перекоса спроса/предложения.\n\n"
        f"Что делать:\n"
        f"- Если уже в позиции: подумай о частичной фиксации, если движение в твою сторону.\n"
        f"- Если не в позиции: не входи вслепую — дождись, что движение не “сдулось” обратно."
    )

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
        score = score_news(kind)
        if score < MIN_SCORE:
            continue

        for t in tickers[:3]:
            tg_send(build_news_message(t, title, link, kind, why_line, score))
            add_daily_signal({
                "ts_msk": datetime.now(MSK).isoformat(timespec="seconds"),
                "ticker": t,
                "type": "news",
                "action": "buy" if kind == "positive" else ("sell" if kind == "negative" else "wait"),
                "score": score,
                "title": title,
                "url": link
            })
            sent += 1
            if sent >= MAX_ALERTS:
                save_state()
                return

    # 2) Anomaly scan (rotate tickers)
    n = len(WATCHLIST)
    start = int(state.get("scan_index", 0)) % max(1, n)
    chunk = []
    for i in range(SCAN_PER_RUN):
        chunk.append(WATCHLIST[(start + i) % n])
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
            "type": "anomaly",
            "action": "watch",
            "score": sig["score"],
            "title": f"Движение {sig['direction']} {sig['change_pct']:.1f}% (x{sig['ratio']:.1f} активность)",
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
    top = items_sorted[:10]

    lines = [f"📌 Дайджест (MSK) {now}", f"Сигналов за день: {len(items)}", ""]
    for e in top:
        ts = datetime.fromisoformat(e["ts_msk"]).strftime("%H:%M")
        act = {"buy": "покупка", "sell": "продажа/избегать", "wait": "подождать", "watch": "движение"}[e["action"]]
        url = f" | {e['url']}" if e.get("url") else ""
        lines.append(f"- {ts} {e['ticker']} — {act} ({e['score']}/10): {e['title']}{url}")

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
