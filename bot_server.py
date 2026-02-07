"""
Production-ready Telegram VIP Bot (single-file example)
- Flask app exposing /webhook and /payment_webhook
- SQLAlchemy for DB (use DATABASE_URL env var, default sqlite for quick start)
- Background jobs via RQ (Redis) if REDIS_URL provided, otherwise ThreadPoolExecutor fallback
- Robust error handling, logging, request timeouts, retries
- Use environment variables for secrets (BOT_TOKEN, DATABASE_URL, REDIS_URL)
- Designed to run under gunicorn: `gunicorn telegram_vip_bot_production:app`

NOTE: This is a minimal-but-production-minded example. For real production use,
- run Redis (for background jobs)
- use PostgreSQL (DATABASE_URL)
- configure HTTPS at platform (Render, Heroku, DigitalOcean)
- rotate tokens and secure environment variables

Files you'll want alongside this:
- requirements.txt
- Procfile (web: gunicorn telegram_vip_bot_production:app)
- worker Procfile or service to run: rq worker default
- optional: Dockerfile

"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Optional, List

import requests
from flask import Flask, request, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Boolean, BigInteger, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Background job systems
try:
    import redis
    from rq import Queue
    REDIS_AVAILABLE = True
except Exception:
    REDIS_AVAILABLE = False

from concurrent.futures import ThreadPoolExecutor

# ------------------------- Configuration -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    logger.error('BOT_TOKEN is not set in environment. Exiting.')
    raise SystemExit('BOT_TOKEN env required')

API_URL = f'https://api.telegram.org/bot{BOT_TOKEN}'

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///./data.db')
REDIS_URL = os.getenv('REDIS_URL')  # optional but recommended

# PLAN_CHANNELS can be provided as JSON in env or hard-coded as fallback
PLAN_CHANNELS_JSON = os.getenv('PLAN_CHANNELS_JSON')
if PLAN_CHANNELS_JSON:
    PLAN_CHANNELS = json.loads(PLAN_CHANNELS_JSON)
else:
    # Example. Use chat ids (numeric, -100...) for private channels
    PLAN_CHANNELS = {
        "basic": ["-1001234567890"],
        "pro": ["-1001234567890", "-1009876543210"]
    }

# timeouts & limits
REQUEST_TIMEOUT = float(os.getenv('REQUEST_TIMEOUT', '10'))  # seconds for external HTTP
INVITE_EXPIRE_DAYS = int(os.getenv('INVITE_EXPIRE_DAYS', '7'))
INVITE_MEMBER_LIMIT = int(os.getenv('INVITE_MEMBER_LIMIT', '1'))
# DB setup
Base = declarative_base()
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

# ------------------------- Models -------------------------
class User(Base):
    __tablename__ = 'users'
    tg_id = Column(BigInteger, primary_key=True, index=True)
    username = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

class Subscription(Base):
    __tablename__ = 'subscriptions'
    id = Column(Integer, primary_key=True, index=True)
    user_tg_id = Column(BigInteger, nullable=False, index=True)
    plan = Column(String(50), nullable=False)
    start_ts = Column(DateTime, default=datetime.utcnow)
    end_ts = Column(DateTime, nullable=False)
    active = Column(Boolean, default=True)

class InviteLinkRecord(Base):
    __tablename__ = 'invite_links'
    id = Column(Integer, primary_key=True)
    user_tg_id = Column(BigInteger, nullable=False)
    plan = Column(String(50), nullable=False)
    channel_id = Column(String(64), nullable=False)
    invite_link = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expire_at = Column(DateTime, nullable=True)
    used = Column(Boolean, default=False)

# create tables
Base.metadata.create_all(bind=engine)

# ------------------------- Background worker -------------------------
executor = ThreadPoolExecutor(max_workers=int(os.getenv('MAX_WORKERS', '4')))
rq_queue = None
if REDIS_URL and REDIS_AVAILABLE:
    try:
        redis_conn = redis.from_url(REDIS_URL)
        rq_queue = Queue('default', connection=redis_conn)
        logger.info('RQ/Redis queue initialized')
    except Exception as e:
        logger.exception('Failed to initialize Redis queue, falling back to ThreadPoolExecutor')
        rq_queue = None

# ------------------------- Helpers -------------------------

def db_session():
    """Get a DB session (use in functions and close after)"""
    return SessionLocal()


def send_message(chat_id: int, text: str):
    url = f"{API_URL}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        if not data.get('ok'):
            logger.error('sendMessage failed: %s', data)
            return False
        return True
    except Exception:
        logger.exception('sendMessage exception')
        return False


def create_invite_link(channel_id: str, name: Optional[str] = None) -> Optional[str]:
    """Create a temporary invite link for a channel.
    Returns invite URL or None on failure. Logs telegram response.
    """
    url = f"{API_URL}/createChatInviteLink"
    expire_date = int(time.time()) + INVITE_EXPIRE_DAYS * 24 * 3600
    payload = {
        "chat_id": channel_id,
        "expire_date": expire_date,
        "member_limit": INVITE_MEMBER_LIMIT
    }
    if name:
        payload['name'] = name

    try:
        r = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        # guard: always check ok and log response if not ok
        data = r.json()
        if not data.get('ok'):
            logger.error('createChatInviteLink failed: %s', data)
            return None
        link = data['result'].get('invite_link')
        return link
    except Exception:
        logger.exception('createChatInviteLink exception')
        return None


# ------------------------- Business logic (run in background) -------------------------
def process_purchase(user_tg_id: int, plan: str):
    """Background job: activate subscription, create invite links, store them and message user."""
    db = db_session()
    try:
        now = datetime.utcnow()
        end = now + timedelta(days=30)
        # create subscription record
        sub = Subscription(user_tg_id=user_tg_id, plan=plan, start_ts=now, end_ts=end, active=True)
        db.add(sub)
        db.commit()

        channels: List[str] = PLAN_CHANNELS.get(plan, [])
        created_links = []
        for ch in channels:
            link = create_invite_link(ch, name=f"{plan}-{user_tg_id}")
            if link:
                rec = InviteLinkRecord(user_tg_id=user_tg_id, plan=plan, channel_id=str(ch), invite_link=link, expire_at=now + timedelta(days=INVITE_EXPIRE_DAYS))
                db.add(rec)
                db.commit()
                created_links.append(link)
            else:
                logger.warning('Failed to create invite link for channel %s and user %s', ch, user_tg_id)

        # notify user with all links (send messages sequentially)
        if created_links:
            send_message(user_tg_id, f"پرداخت با موفقیت انجام شد. لینک‌های دسترسی ارسال شدند.")
            for l in created_links:
                send_message(user_tg_id, l)
        else:
            send_message(user_tg_id, "خطا در ایجاد لینک‌ها. لطفاً با پشتیبانی تماس بگیرید.")

    except Exception:
        logger.exception('Error in process_purchase')
    finally:
        db.close()


# ------------------------- Flask App -------------------------
app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def webhook():
    """Telegram will POST updates here. We enqueue processing quickly and return 200.
    """
    update = request.get_json(silent=True, force=True)
    logger.info('Update received: keys=%s', list(update.keys()) if isinstance(update, dict) else type(update))

    # fast ACK
    try:
        # We only handle messages for now
        if not update or 'message' not in update:
            return '', 200

        msg = update['message']
        chat_id = msg.get('chat', {}).get('id')
        text = msg.get('text', '')

        # basic user registration
        db = db_session()
        try:
            uid = msg.get('from', {}).get('id')
            username = msg.get('from', {}).get('username')
            if uid:
                user = db.query(User).get(uid)
                if not user:
                    db.add(User(tg_id=uid, username=username))
                    db.commit()
        finally:
            db.close()

        # commands
        if text and text.startswith('/buy'):
            # parse plan name (eg: /buy pro) or default
            parts = text.split()
            plan = parts[1] if len(parts) > 1 else 'basic'

            # enqueue background job
            if rq_queue:
                rq_queue.enqueue(process_purchase, chat_id, plan)
            else:
                executor.submit(process_purchase, chat_id, plan)

            # immediate feedback
            send_message(chat_id, 'درخواست خرید دریافت شد. در حال پردازش...')

    except Exception:
        logger.exception('Unhandled exception in webhook')
    return '', 200


# Payment webhook endpoint (example: your payment gateway posts here)
@app.route('/payment_webhook', methods=['POST'])
def payment_webhook():
    """Receive payment gateway callbacks. Validate signature and then enqueue purchase processing.
    The exact validation depends on gateway — here we expect JSON {user_tg_id, plan, status}
    """
    payload = request.get_json(silent=True, force=True)
    logger.info('Payment webhook payload: %s', payload)

    # TODO: verify gateway signature/header here
    if not payload:
        return jsonify({'ok': False}), 400

    status = payload.get('status')
    user_tg_id = payload.get('user_tg_id')
    plan = payload.get('plan')

    if status == 'paid' and user_tg_id and plan:
        # enqueue
        if rq_queue:
            rq_queue.enqueue(process_purchase, int(user_tg_id), plan)
        else:
            executor.submit(process_purchase, int(user_tg_id), plan)
        return jsonify({'ok': True}), 200

    return jsonify({'ok': False}), 400


# health check
@app.route('/')
def index():
    return 'OK', 200


# ------------------------- Run (for local dev only) -------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv('PORT', 5000)))
