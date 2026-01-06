#!/usr/bin/env python3
"""
Vyasify Quiz Bot — FINAL VERIFIED BUILD
✔ Greeting works
✔ Instant first question
✔ No lag
✔ Safe async handlers
"""

import os
import csv
import time
import asyncio
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone
import logging

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    PollAnswerHandler,
    ContextTypes,
)
from telegram.helpers import escape_markdown

# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not set")

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

IST = timezone(timedelta(hours=5, minutes=30))
DEFAULT_TIME = 20
TRANSITION_DELAY = 1

# ================= STATE =================
sessions = {}
quiz_cache = []

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ================= HELPERS =================

def safe(text):
    return escape_markdown(text or "", version=2)

def parse_correct(opt):
    return ord(opt) - 65 if opt in ("A", "B", "C", "D") else None

def load_quiz():
    r = requests.get(QUIZ_CSV_URL, timeout=15)
    r.raise_for_status()
    rows = list(csv.DictReader(StringIO(r.text)))
    return rows

# ================= UI =================

def skip_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip", callback_data="skip")]
    ])

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_user.id,
        text=safe(
            "📘 Welcome to *Vyasify Daily Quiz*\n\n"
            "• UPSC Prelims focused\n"
            "• Timed questions\n"
            "• Negative marking\n\n"
            "Tap below to start 👇"
        ),
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("▶️ Start Quiz", callback_data="start_quiz")]
        ]),
        parse_mode="MarkdownV2",
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await update.callback_query.answer()

    sessions[uid] = {
        "index": 0,
        "marks": 0.0,
        "attempted": 0,
        "wrong": 0,
        "skipped": 0,
        "start": time.time(),
        "lock": asyncio.Lock(),
        "transitioned": False,
    }

    await send_question(context, uid)

async def send_question(context, uid):
    s = sessions.get(uid)
    if not s:
        return

    if s["index"] >= len(quiz_cache):
        await finish_quiz(context, uid)
        return

    q = quiz_cache[s["index"]]
    correct = parse_correct(q["correct_option"])

    await context.bot.send_poll(
        chat_id=uid,
        question=f"Q{s['index']+1}. {q['question']}",
        options=[
            q["option_a"],
            q["option_b"],
            q["option_c"],
            q["option_d"],
        ],
        type="quiz",
        correct_option_id=correct,
        is_anonymous=False,
        open_period=int(q.get("time", DEFAULT_TIME)),
        reply_markup=skip_keyboard(),
    )

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.poll_answer.user.id
    s = sessions.get(uid)
    if not s:
        return

    async with s["lock"]:
        if s["transitioned"]:
            return
        s["transitioned"] = True
        q = quiz_cache[s["index"]]
        selected = update.poll_answer.option_ids[0]
        if selected == parse_correct(q["correct_option"]):
            s["marks"] += 2
        else:
            s["wrong"] += 1
            s["marks"] -= 2 / 3
        s["attempted"] += 1
        s["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    s["transitioned"] = False
    await send_question(context, uid)

async def handle_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.callback_query.from_user.id
    await update.callback_query.answer()
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
    s["transitioned"] = False
    await send_question(context, uid)

async def finish_quiz(context, uid):
    s = sessions.pop(uid, None)
    if not s:
        return
    time_taken = int(time.time() - s["start"])
    await context.bot.send_message(
        chat_id=uid,
        text=safe(
            f"🏁 *Quiz Finished*\n\n"
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
    global quiz_cache
    quiz_cache = load_quiz()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(start_quiz, pattern="start_quiz"))
    app.add_handler(CallbackQueryHandler(handle_skip, pattern="skip"))
    app.add_handler(PollAnswerHandler(handle_answer))

    app.run_polling()

if __name__ == "__main__":
    main()
