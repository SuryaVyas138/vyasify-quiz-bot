#!/usr/bin/env python3
"""
Vyasify Quiz Bot — FINAL STABLE BUILD
✔ Instant first question (no lag)
✔ Queue retained for scale
✔ Safe concurrency
✔ UPSC-style quiz flow
"""

import os
import csv
import time
import asyncio
import requests
import re
import uuid
import sys
import random
import json
from io import StringIO
from datetime import datetime, timezone, timedelta, time as dtime
import logging
import signal
import unittest

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.helpers import escape_markdown
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN and "test" not in sys.argv:
    raise RuntimeError("BOT_TOKEN environment variable is required")

ADMIN_IDS = {2053638316}

QUIZ_CSV_URL = os.environ.get(
    "QUIZ_CSV_URL",
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

DEFAULT_QUESTION_TIME = 20
TRANSITION_DELAY = 1
DEFAULT_MARKS_PER_QUESTION = 2.0
DEFAULT_NEGATIVE_RATIO = 1 / 3
GRACE_SECONDS = 0.5

MAX_CONCURRENT_SENDS = 8
SEND_JITTER_MAX = 0.3
SEND_RETRY_MAX = 4
SEND_BASE_BACKOFF = 0.5

SESSION_TTL = 30 * 60
DAILY_SCORES_FILE = "daily_scores.json"

# ================= STATE =================
sessions = {}
daily_scores = {}
current_quiz_date_key = None

_cached_quizzes = {}
_cached_quiz_lock = asyncio.Lock()

_send_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
_send_queue = asyncio.Queue()
_shutdown_event = asyncio.Event()

# ================= HELPERS =================

def parse_correct_option(opt):
    if not opt:
        return None
    opt = opt.strip().upper()
    return ord(opt) - 65 if opt in ("A", "B", "C", "D") else None

def safe_escape(text):
    return escape_markdown(text or "", version=2)

def fetch_csv():
    r = requests.get(QUIZ_CSV_URL, timeout=15)
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.text)))

def normalize_rows(rows):
    out = []
    for r in rows:
        try:
            r["_date"] = datetime.strptime(r["date"], "%d-%m-%Y").date()
            r["_time"] = int(float(r.get("time", DEFAULT_QUESTION_TIME)))
            r["_marks"] = float(r.get("marks", DEFAULT_MARKS_PER_QUESTION))
            r["_neg"] = float(r.get("negative_ratio", DEFAULT_NEGATIVE_RATIO))
            if parse_correct_option(r.get("correct_option")) is None:
                r["_invalid"] = True
            out.append(r)
        except Exception:
            continue
    return out

async def preload_quiz():
    global current_quiz_date_key
    rows = normalize_rows(fetch_csv())
    today = now_ist().date()
    dates = sorted({r["_date"] for r in rows if r["_date"] <= today})
    if not dates:
        return None
    key = dates[-1].isoformat()
    async with _cached_quiz_lock:
        _cached_quizzes[key] = [r for r in rows if r["_date"].isoformat() == key]
        if key != current_quiz_date_key:
            daily_scores.clear()
            current_quiz_date_key = key
    return key

# ================= SEND HELPERS =================

async def send_with_retries(bot, factory):
    attempt = 0
    backoff = SEND_BASE_BACKOFF
    while attempt <= SEND_RETRY_MAX:
        try:
            async with _send_semaphore:
                await asyncio.sleep(random.uniform(0, SEND_JITTER_MAX))
                return await factory()
        except (RetryAfter, TimedOut, NetworkError):
            attempt += 1
            await asyncio.sleep(backoff * attempt)
        except TelegramError:
            raise
    raise RuntimeError("Send failed")

async def send_worker(app):
    bot = app.bot
    while not _shutdown_event.is_set():
        try:
            job = await _send_queue.get()
            await send_with_retries(bot, job["factory"])
            _send_queue.task_done()
        except Exception:
            logger.exception("Send worker error")

# ================= UI =================

def skip_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_q")]])

async def send_greeting(ctx, uid):
    await ctx.bot.send_message(
        chat_id=uid,
        text=safe_escape(
            "📘 Welcome to Vyasify Daily Quiz\n\n"
            "• UPSC Prelims focused\n"
            "• Timed questions\n"
            "• Negative marking\n\n"
            "Tap below to start"
        ),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("▶️ Start Quiz", callback_data="start_quiz")]]
        ),
        parse_mode="MarkdownV2",
    )

# ================= QUIZ FLOW =================

async def start_quiz(ctx, uid):
    async with _cached_quiz_lock:
        if not current_quiz_date_key:
            await preload_quiz()
    questions = _cached_quizzes.get(current_quiz_date_key, [])
    if not questions:
        await ctx.bot.send_message(chat_id=uid, text="Quiz not available.")
        return

    sessions[uid] = {
        "questions": questions,
        "index": 0,
        "marks": 0.0,
        "attempted": 0,
        "wrong": 0,
        "skipped": 0,
        "transitioned": False,
        "lock": asyncio.Lock(),
        "timeout": None,
        "deadline": None,
        "start": time.time(),
    }

    await send_question(ctx, uid, immediate=True)

async def send_question(ctx, uid, immediate=False):
    s = sessions.get(uid)
    if not s or s["index"] >= len(s["questions"]):
        await finish_quiz(ctx, uid)
        return

    q = s["questions"][s["index"]]
    correct = parse_correct_option(q["correct_option"])
    options = [q["option_a"], q["option_b"], q["option_c"], q["option_d"]]

    async def factory():
        return await ctx.bot.send_poll(
            chat_id=uid,
            question=f"Q{s['index']+1}. {q['question']}",
            options=options,
            type="quiz",
            correct_option_id=correct,
            is_anonymous=False,
            open_period=q["_time"],
            reply_markup=skip_keyboard(),
        )

    if immediate:
        await factory()
    else:
        await _send_queue.put({"factory": factory})

    loop = asyncio.get_running_loop()
    s["deadline"] = loop.time() + q["_time"]
    s["timeout"] = loop.call_at(
        s["deadline"],
        lambda: asyncio.create_task(timeout_question(ctx, uid))
    )

async def timeout_question(ctx, uid):
    s = sessions.get(uid)
    if not s:
        return
    async with s["lock"]:
        if s["transitioned"]:
            return
        s["transitioned"] = True
        s["skipped"] += 1
        s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(ctx, uid)

async def handle_answer(update: Update, ctx):
    uid = update.poll_answer.user.id
    s = sessions.get(uid)
    if not s:
        return

    async with s["lock"]:
        if s["transitioned"]:
            return
        s["transitioned"] = True
        s["attempted"] += 1
        q = s["questions"][s["index"]]
        correct = parse_correct_option(q["correct_option"])
        selected = update.poll_answer.option_ids[0]
        if selected == correct:
            s["marks"] += q["_marks"]
        else:
            s["wrong"] += 1
            s["marks"] -= q["_marks"] * q["_neg"]
        s["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(ctx, uid)

async def handle_skip(update: Update, ctx):
    uid = update.callback_query.from_user.id
    s = sessions.get(uid)
    if not s:
        return
    async with s["lock"]:
        if s["transitioned"]:
            return
        s["transitioned"] = True
        s["skipped"] += 1
        s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(ctx, uid)

# ================= FINISH =================

async def finish_quiz(ctx, uid):
    s = sessions.pop(uid, None)
    if not s:
        return
    time_taken = int(time.time() - s["start"])
    await ctx.bot.send_message(
        chat_id=uid,
        text=safe_escape(
            f"🏁 Quiz Finished\n\n"
            f"Marks: {round(s['marks'],2)}\n"
            f"Attempted: {s['attempted']}\n"
            f"Wrong: {s['wrong']}\n"
            f"Skipped: {s['skipped']}\n"
            f"Time: {time_taken}s"
        ),
        parse_mode="MarkdownV2",
    )

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", lambda u, c: send_greeting(c, u.effective_user.id)))
    app.add_handler(CallbackQueryHandler(lambda u, c: start_quiz(c, u.callback_query.from_user.id), pattern="start_quiz"))
    app.add_handler(CallbackQueryHandler(handle_skip, pattern="skip_q"))
    app.add_handler(PollAnswerHandler(handle_answer))

    async def post_init(app):
        await preload_quiz()
        asyncio.create_task(send_worker(app))

    app.post_init = post_init
    app.run_polling()

if __name__ == "__main__":
    main()
