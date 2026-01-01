import os
import csv
import requests
import asyncio
from io import StringIO
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    PollAnswerHandler,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN")

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

QUESTION_TIME = 20  # seconds per question

# In-memory session storage
user_sessions = {}

# ---------------- /start ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 Welcome to Vyasify Quiz Bot\n\n"
        "⏱ Each question has 20 seconds.\n"
        "📅 Use /daily to start today’s quiz."
    )

# ---------------- /daily ----------------

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now().strftime("%d-%m-%Y")
    questions = []

    try:
        response = requests.get(CSV_URL, timeout=15)
        response.raise_for_status()
    except Exception:
        await update.message.reply_text(
            "⚠️ Unable to load quiz data right now."
        )
        return

    reader = csv.DictReader(StringIO(response.text))
    for row in reader:
        if row.get("date", "").strip() == today:
            questions.append(row)

    if not questions:
        await update.message.reply_text(
            "❌ Today’s quiz is not yet uploaded."
        )
        return

    user_id = update.effective_user.id

    user_sessions[user_id] = {
        "score": 0,
        "current": 0,
        "total": len(questions),
        "questions": questions,
        "explanations": [],
        "active": None,
    }

    await update.message.reply_text(
        f"📝 Daily Quiz Started\n"
        f"Questions: {len(questions)}\n"
        f"⏱ 20 seconds per question"
    )

    await send_next_question(context, user_id)

# ---------------- SEND QUESTIONS SEQUENTIALLY ----------------

async def send_next_question(context, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return

    if session["current"] >= session["total"]:
        await send_final_result(context, user_id)
        return

    q = session["questions"][session["current"]]

    poll = await context.bot.send_poll(
        chat_id=user_id,
        question=q["question"],
        options=[
            q["option_a"],
            q["option_b"],
            q["option_c"],
            q["option_d"],
        ],
        type="quiz",
        correct_option_id=ord(q["correct_option"].strip()) - ord("A"),
        is_anonymous=False,
        open_period=QUESTION_TIME,
    )

    session["active"] = {
        "poll_id": poll.poll.id,
        "correct": ord(q["correct_option"].strip()) - ord("A"),
        "explanation": q["explanation"],
        "source": q["source"],
        "answered": False,
    }

    # Wait for timer
    await asyncio.sleep(QUESTION_TIME + 1)

    # If NOT answered → time over
    if session["active"] and not session["active"]["answered"]:
        session["explanations"].append(
            f"• {q['explanation']} (Source: {q['source']})"
        )
        session["current"] += 1
        session["active"] = None
        await send_next_question(context, user_id)

# ---------------- POLL ANSWER HANDLER ----------------

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    session = user_sessions.get(user_id)
    if not session or not session["active"]:
        return

    chosen = update.poll_answer.option_ids[0]
    data = session["active"]
    data["answered"] = True

    if chosen == data["correct"]:
        session["score"] += 1

    session["explanations"].append(
        f"• {data['explanation']} (Source: {data['source']})"
    )

    session["current"] += 1
    session["active"] = None

    await send_next_question(context, user_id)

# ---------------- FINAL RESULT ----------------

async def send_final_result(context, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return

    result_text = (
        f"✅ *Quiz Completed!*\n\n"
        f"🎯 *Score:* {session['score']} / {session['total']}\n\n"
        f"*📖 Explanations:*\n" +
        "\n".join(session["explanations"])
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=result_text,
        parse_mode="Markdown",
    )

    del user_sessions[user_id]

# ---------------- MAIN ----------------

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    app.run_polling()

if __name__ == "__main__":
    main()
