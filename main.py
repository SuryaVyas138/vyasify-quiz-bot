import os
import csv
import requests
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

# Session store (in-memory)
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
            "⚠️ Unable to load quiz data right now.\nPlease try again later."
        )
        return

    f = StringIO(response.text)
    reader = csv.DictReader(f)

    for row in reader:
        if row.get("date", "").strip() == today:
            questions.append(row)

    if not questions:
        await update.message.reply_text(
            "❌ Today’s quiz is not yet uploaded.\nPlease check back later."
        )
        return

    user_id = update.effective_user.id

    user_sessions[user_id] = {
        "score": 0,
        "total": len(questions),
        "answered": 0,
        "explanations": [],
        "poll_map": {},
    }

    await update.message.reply_text(
        f"📝 Daily Quiz Started\n"
        f"Questions: {len(questions)}\n"
        f"⏱ Time per question: {QUESTION_TIME} seconds"
    )

    for q in questions:
        poll = await context.bot.send_poll(
            chat_id=update.effective_chat.id,
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
            open_period=QUESTION_TIME,  # ⏱ TIMER ENFORCED HERE
        )

        user_sessions[user_id]["poll_map"][poll.poll.id] = {
            "correct": ord(q["correct_option"].strip()) - ord("A"),
            "explanation": q["explanation"],
            "source": q["source"],
        }

# ---------------- POLL ANSWER HANDLER ----------------

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    poll_id = update.poll_answer.poll_id
    chosen = update.poll_answer.option_ids[0]

    session = user_sessions.get(user_id)
    if not session or poll_id not in session["poll_map"]:
        return

    data = session["poll_map"][poll_id]
    session["answered"] += 1

    if chosen == data["correct"]:
        session["score"] += 1

    session["explanations"].append(
        f"• {data['explanation']} (Source: {data['source']})"
    )

    # If all questions answered
    if session["answered"] == session["total"]:
        await send_final_result(context, user_id)

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
