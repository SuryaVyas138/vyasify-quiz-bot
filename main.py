import os
import csv
import requests
import asyncio
import time
from io import StringIO
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    PollAnswerHandler,
)

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

BASE_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

QUESTION_TIME = 20          # seconds per question
TRANSITION_DELAY = 1        # smooth gap between questions (seconds)

# In-memory sessions
user_sessions = {}

# ================= /start =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 *Welcome to Vyasify Quiz Bot*\n\n"
        "⏱ 20 seconds per question\n"
        "📅 Use /daily to start today’s quiz",
        parse_mode="Markdown",
    )

# ================= /daily =================

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    await update.message.reply_text(
        "📝 *Daily Quiz Initialising…*\n"
        "⏳ Fetching latest questions…",
        parse_mode="Markdown",
    )

    today = datetime.now().strftime("%d-%m-%Y")
    questions = []

    # ---- FORCE FRESH FETCH (cache busting) ----
    csv_url = f"{BASE_CSV_URL}&_ts={int(time.time())}"

    try:
        response = requests.get(
            csv_url,
            timeout=15,
            headers={"Cache-Control": "no-cache"},
        )
        response.raise_for_status()
    except Exception:
        await update.message.reply_text("⚠️ Unable to load quiz data.")
        return

    reader = csv.DictReader(StringIO(response.text))
    for row in reader:
        if row.get("date", "").strip() == today:
            questions.append(row)

    if not questions:
        await update.message.reply_text(
            "❌ Today’s quiz is not yet uploaded.\nPlease check back later."
        )
        return

    user_sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "score": 0,
        "total": len(questions),
        "explanations": [],
        "active": None,
        "timer_task": None,
    }

    await update.message.reply_text(
        f"✅ *Quiz Ready!*\n"
        f"📊 Questions: {len(questions)}\n"
        f"⏱ Time per question: {QUESTION_TIME} seconds",
        parse_mode="Markdown",
    )

    await send_question(context, user_id)

# ================= SEND QUESTION =================

async def send_question(context, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return

    if session["index"] >= session["total"]:
        await send_result(context, user_id)
        return

    q = session["questions"][session["index"]]
    q_no = session["index"] + 1
    total = session["total"]

    await context.bot.send_message(
        chat_id=user_id,
        text=f"🧠 *Question {q_no} of {total}*\n⏱ {QUESTION_TIME} seconds",
        parse_mode="Markdown",
    )

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
    }

    session["timer_task"] = asyncio.create_task(
        handle_timeout(context, user_id)
    )

# ================= TIMEOUT HANDLER =================

async def handle_timeout(context, user_id):
    await asyncio.sleep(QUESTION_TIME)

    session = user_sessions.get(user_id)
    if not session or not session["active"]:
        return

    data = session["active"]

    session["explanations"].append(
        f"• {data['explanation']} (Source: {data['source']})"
    )

    session["index"] += 1
    session["active"] = None

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= ANSWER HANDLER =================

async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    session = user_sessions.get(user_id)
    if not session or not session["active"]:
        return

    data = session["active"]
    chosen = update.poll_answer.option_ids[0]

    if session["timer_task"]:
        session["timer_task"].cancel()
        session["timer_task"] = None

    if chosen == data["correct"]:
        session["score"] += 1

    session["explanations"].append(
        f"• {data['explanation']} (Source: {data['source']})"
    )

    session["index"] += 1
    session["active"] = None

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= FINAL RESULT =================

async def send_result(context, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return

    text = (
        f"✅ *Quiz Completed!*\n\n"
        f"🎯 *Your Score:* {session['score']} / {session['total']}\n\n"
        f"*📖 Explanations:*\n" +
        "\n".join(session["explanations"])
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        parse_mode="Markdown",
    )

    del user_sessions[user_id]

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))
    app.add_handler(PollAnswerHandler(handle_poll_answer))

    app.run_polling()

if __name__ == "__main__":
    main()
