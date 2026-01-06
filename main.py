#!/usr/bin/env python3
import os
import csv
import time
import asyncio
import requests
from io import StringIO

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

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable not set")

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

QUESTION_TIME = 20
TRANSITION_DELAY = 1

# ================= STATE =================

quiz_questions = []
sessions = {}

# ================= HELPERS =================

def load_quiz():
    r = requests.get(QUIZ_CSV_URL, timeout=15)
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.text)))

def correct_index(opt):
    if opt in ("A", "B", "C", "D"):
        return ord(opt) - 65
    return None

def skip_keyboard():
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏭ Skip", callback_data="skip")]]
    )

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_user.id,
        text=(
            "📘 Welcome to *Vyasify Daily Quiz*\n\n"
            "• UPSC Prelims focused\n"
            "• Timed questions\n"
            "• Negative marking\n\n"
            "Tap below to start 👇"
        ),
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("▶️ Start Quiz", callback_data="start_quiz")]]
        ),
        parse_mode="Markdown",
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id

    sessions[user_id] = {
        "index": 0,
        "attempted": 0,
        "wrong": 0,
        "skipped": 0,
        "marks": 0.0,
        "start_time": time.time(),
    }

    await send_question(context, user_id)

async def send_question(context, user_id):
    session = sessions.get(user_id)
    if not session:
        return

    idx = session["index"]

    if idx >= len(quiz_questions):
        await finish_quiz(context, user_id)
        return

    q = quiz_questions[idx]

    await context.bot.send_poll(
        chat_id=user_id,
        question=f"Q{idx + 1}. {q['question']}",
        options=[
            q["option_a"],
            q["option_b"],
            q["option_c"],
            q["option_d"],
        ],
        type="quiz",
        correct_option_id=correct_index(q["correct_option"]),
        is_anonymous=False,
        open_period=QUESTION_TIME,
        reply_markup=skip_keyboard(),
    )

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    session = sessions.get(user_id)
    if not session:
        return

    q = quiz_questions[session["index"]]
    selected = update.poll_answer.option_ids[0]
    correct = correct_index(q["correct_option"])

    session["attempted"] += 1
    if selected == correct:
        session["marks"] += 2
    else:
        session["wrong"] += 1
        session["marks"] -= 2 / 3

    session["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

async def handle_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    session = sessions.get(user_id)
    if not session:
        return

    session["skipped"] += 1
    session["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

async def finish_quiz(context, user_id):
    session = sessions.pop(user_id, None)
    if not session:
        return

    time_taken = int(time.time() - session["start_time"])

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"Attempted: {session['attempted']}\n"
            f"Wrong: {session['wrong']}\n"
            f"Skipped: {session['skipped']}\n"
            f"Marks: {round(session['marks'], 2)}\n"
            f"Time Taken: {time_taken} sec"
        ),
        parse_mode="Markdown",
    )

# ================= MAIN =================

def main():
    global quiz_questions
    quiz_questions = load_quiz()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(start_quiz, pattern="start_quiz"))
    app.add_handler(CallbackQueryHandler(handle_skip, pattern="skip"))
    app.add_handler(PollAnswerHandler(handle_answer))

    # 🚨 REQUIRED FOR QUIZ BOTS
    app.run_polling(
        allowed_updates=["message", "callback_query", "poll_answer"]
    )

if __name__ == "__main__":
    main()
