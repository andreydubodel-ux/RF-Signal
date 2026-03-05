import os
import requests

TG_TOKEN = os.environ["TG_TOKEN"]
TG_CHAT_ID = os.environ["TG_CHAT_ID"]

def tg_send(text: str):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    r = requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text}, timeout=30)
    r.raise_for_status()

if __name__ == "__main__":
    tg_send("✅ rf-signal-agent: тестовый запуск из GitHub Actions прошёл")
