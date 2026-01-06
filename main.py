#!/usr/bin/env python3
import os
import csv
import requests
from io import StringIO
import asyncio

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
    raise RuntimeError("BOT_TOKEN environment variable is NOT set")

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

QUESTION_TIME = 20

# ================= STATE =================

quiz = []
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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Skip", callback_data="skip")]
    ])

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "BOT IS ALIVE ✅\n\nPress Start Quiz to continue.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start Quiz", callback_data="start_quiz")]
        ])
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    user_id = update.callback_query.from_user.id
    sessions[user_id] = {"i": 0}
    await send_question(context, user_id)

async def send_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    if s["i"] >= len(quiz):
        await context.bot.send_message(chat_id=user_id, text="Quiz finished.")
        sessions.pop(user_id, None)
        return

    q = quiz[s["i"]]

    await context.bot.send_poll(
        chat_id=user_id,
        question=q["question"],
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
    if user_id not in sessions:
        return
    sessions[user_id]["i"] += 1
    await send_question(context, user_id)

async def handle_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    user_id = update.callback_query.from_user.id
    if user_id not in sessions:
        return
    sessions[user_id]["i"] += 1
    await send_question(context, user_id)

# ================= MAIN =================

def main():
    global quiz
    quiz = load_quiz()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # 🔥 AUTO-DELETE WEBHOOK (SELF-FIX)
    async def startup(app):
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            print("Webhook deleted successfully")
        except Exception as e:
            print("Webhook delete failed:", e)

    app.post_init = startup

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(start_quiz, pattern="start_quiz"))
    app.add_handler(CallbackQueryHandler(handle_skip, pattern="skip"))
    app.add_handler(PollAnswerHandler(handle_answer))

    app.run_polling(
        allowed_updates=["message", "callback_query", "poll_answer"]
    )

if __name__ == "__main__":
    main()
