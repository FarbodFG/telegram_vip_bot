import os
import time
import sqlite3
import requests
from flask import Flask, request, jsonify
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

PLAN_CHANNELS = {
    "basic": ["@CHANNEL_USERNAME_1"],
    "pro": ["@CHANNEL_USERNAME_1", "@CHANNEL_USERNAME_2"]
}

DB_PATH = "db.sqlite3"
app = Flask(__name__)

# ---------- Database ----------
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        tg_id INTEGER PRIMARY KEY,
        username TEXT
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_tg_id INTEGER,
        plan TEXT,
        end_ts INTEGER
    )""")
    con.commit()
    con.close()

def add_user(tg_id, username):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT OR IGNORE INTO users (tg_id, username) VALUES (?,?)", (tg_id, username))
    con.commit()
    con.close()

def activate_subscription(tg_id, plan, days=30):
    end = int(time.time()) + days * 86400
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO subscriptions (user_tg_id, plan, end_ts) VALUES (?,?,?)", (tg_id, plan, end))
    con.commit()
    con.close()

# ---------- Telegram ----------
def send_message(chat_id, text):
    requests.post(f"{API_URL}/sendMessage", json={
        "chat_id": chat_id,
        "text": text
    })

def create_invite_link(channel):
    expire_date = int(time.time()) + 86400
    r = requests.post(f"{API_URL}/createChatInviteLink", json={
        "chat_id": channel,
        "member_limit": 1,
        "expire_date": expire_date
    })
    return r.json()["result"]["invite_link"]

# ---------- Telegram Webhook ----------
@app.route("/webhook", methods=["POST"])
def telegram_webhook():
    data = request.json
    if "message" in data:
        msg = data["message"]
        chat_id = msg["chat"]["id"]
        username = msg["from"].get("username", "")
        text = msg.get("text", "")

        add_user(chat_id, username)

        if text == "/start":
            send_message(chat_id, "سلام 👋 برای خرید اشتراک دستور /buy رو بزن")

        elif text == "/buy":
            # شبیه سازی پرداخت موفق
            activate_subscription(chat_id, "basic")
            links = []
            for ch in PLAN_CHANNELS["basic"]:
                links.append(create_invite_link(ch))

            send_message(chat_id, "پرداخت با موفقیت انجام شد ✅ لینک ورود:")
            for l in links:
                send_message(chat_id, l)

    return "ok"

# ---------- Run ----------
if __name__ == "__main__":
    init_db()
    app.run(port=5000)
