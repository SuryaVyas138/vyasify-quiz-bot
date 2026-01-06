#!/usr/bin/env python3
import os
import csv
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

# ---------------- CONFIG ----------------

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

QUIZ_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

# ---------------- DATA ----------------

quiz = []
sessions = {}

# ---------------- HELPERS ----------------

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

# ---------------- HANDLERS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_user.id,
        text="Welcome to Vyasify Quiz.\n\nPress Start to begin.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Start Quiz", callback_data="start")]
        ])
    )

async def start_quiz(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.callback_query.from_user.id
    sessions[uid] = {"i": 0}
    await send_question(context, uid)

async def send_question(context, uid):
    s = sessions.get(uid)
    if not s:
        return

    if s["i"] >= len(quiz):
        await context.bot.send_message(chat_id=uid, text="Quiz finished.")
        sessions.pop(uid, None)
        return

    q = quiz[s["i"]]

    await context.bot.send_poll(
        chat_id=uid,
        question=q["question"],
        options=[q["option_a"], q["option_b"], q["option_c"], q["option_d"]],
        type="quiz",
        correct_option_id=correct_index(q["correct_option"]),
        is_anonymous=False,
        reply_markup=skip_keyboard(),
    )

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.poll_answer.user.id
    if uid not in sessions:
        return
    sessions[uid]["i"] += 1
    await send_question(context, uid)

async def handle_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    uid = update.callback_query.from_user.id
    if uid not in sessions:
        return
    sessions[uid]["i"] += 1
    await send_question(context, uid)

# ---------------- MAIN ----------------

def main():
    global quiz
    quiz = load_quiz()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(start_quiz, pattern="start"))
    app.add_handler(CallbackQueryHandler(handle_skip, pattern="skip"))
    app.add_handler(PollAnswerHandler(handle_answer))

    app.run_polling(
        allowed_updates=["message", "callback_query", "poll_answer"]
    )

if __name__ == "__main__":
    main()
0.
