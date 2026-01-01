import os
import csv
import time
import asyncio
import requests
import re
from io import StringIO
from datetime import datetime, timezone, timedelta

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ================= TIMEZONE (IST) =================

IST = timezone(timedelta(hours=5, minutes=30))

def today():
    return datetime.now(IST).strftime("%d-%m-%Y")

def now_time():
    return datetime.now(IST).strftime("%H:%M:%S")

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

ADMIN_IDS = {123456789}  # 🔴 REPLACE with your Telegram ID

OFFENSIVE_WORDS = {
    "fuck", "shit", "bitch", "asshole", "idiot", "stupid"
}

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

QUESTION_TIME = 20
TRANSITION_DELAY = 1

# ================= STORAGE =================

sessions = {}
daily_scores = {}
blocked_logs = []

# ================= HELPERS =================

def fetch_csv(url):
    url = f"{url}&_ts={int(time.time())}"
    r = requests.get(url, timeout=15, headers={"Cache-Control": "no-cache"})
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))

def contains_offensive(text: str) -> bool:
    words = re.findall(r"\b\w+\b", text.lower())
    return any(w in OFFENSIVE_WORDS for w in words)

# ================= GREETING =================

async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])

    text = (
        f"👋 *Hello {name}!*\n\n"
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "This is a daily exam-oriented quiz designed for *UPSC, SSC, and Regulatory Body* aspirants.\n\n"
        "📝 20 seconds per question\n"
        "📊 Score & percentile\n"
        "📖 Detailed explanations at the end\n\n"
        "👇 Tap a button below to continue"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

# ================= COMMAND =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await send_greeting(context, user.id, user.first_name)

# ================= BUTTON HANDLER =================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user = query.from_user

    if query.data == "start_quiz":
        await start_quiz(context, user.id, user.first_name)

    elif query.data == "how_it_works":
        await context.bot.send_message(
            chat_id=user.id,
            text=(
                "ℹ️ *How Vyasify Daily Quiz Works*\n\n"
                "1️⃣ Tap *Start Today’s Quiz*\n"
                "2️⃣ Answer each question within 20 seconds\n"
                "3️⃣ Get score & percentile\n"
                "4️⃣ Review explanations at the end\n\n"
                "🎯 Learning-focused daily practice."
            ),
            parse_mode="Markdown",
        )

# ================= QUIZ START =================

async def start_quiz(context, user_id, name):
    rows = fetch_csv(QUIZ_CSV_URL)
    questions = [r for r in rows if r["date"].strip() == today()]

    if not questions:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Today’s quiz is not yet uploaded."
        )
        return

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "score": 0,
        "start": time.time(),
        "name": name,
        "active": False,
        "timer": None,
        "finished": False,
        "explanations": [],
    }

    await context.bot.send_message(chat_id=user_id, text="📘 Daily Quiz Initialising…")
    await asyncio.sleep(1)
    await send_question(context, user_id)

# ================= QUIZ FLOW =================

async def send_question(context, user_id):
    s = sessions.get(user_id)
    if not s or s["finished"]:
        return

    if s["index"] >= len(s["questions"]):
        s["finished"] = True
        await finish_quiz(context, user_id)
        return

    q = s["questions"][s["index"]]

    await context.bot.send_poll(
        chat_id=user_id,
        question=q["question"],
        options=[q["option_a"], q["option_b"], q["option_c"], q["option_d"]],
        type="quiz",
        correct_option_id=ord(q["correct_option"].strip()) - 65,
        explanation=f"{q['explanation']}\n\nSource: {q['source']}",
        explanation_parse_mode="Markdown",
        is_anonymous=False,
        open_period=QUESTION_TIME,
    )

    s["active"] = True
    s["timer"] = asyncio.create_task(question_timeout(context, user_id))

async def question_timeout(context, user_id):
    await asyncio.sleep(QUESTION_TIME + 0.5)
    s = sessions.get(user_id)
    if not s or not s["active"] or s["finished"]:
        return

    store_explanation(s)
    s["active"] = False
    s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    s = sessions.get(user_id)
    if not s or not s["active"] or s["finished"]:
        return

    if s["timer"]:
        s["timer"].cancel()

    q = s["questions"][s["index"]]
    if update.poll_answer.option_ids[0] == ord(q["correct_option"].strip()) - 65:
        s["score"] += 1

    store_explanation(s)
    s["active"] = False
    s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= EXPLANATION =================

def store_explanation(session):
    q = session["questions"][session["index"]]
    session["explanations"].append(
        f"Q{session['index']+1}. {q['question']}\n\n"
        f"Explanation:\n{q['explanation']}\n\n"
        f"Source: {q['source']}"
    )

# ================= FINAL RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    correct = s["score"]
    percentile = int((correct / total) * 100)

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"✅ Correct: {correct}\n"
            f"❌ Wrong: {total - correct}\n"
            f"🎯 Accuracy: {percentile}%"
        ),
        parse_mode="Markdown",
    )

    await context.bot.send_message(
        chat_id=user_id,
        text="📖 *Explanations*\n\n" + "\n\n".join(s["explanations"]),
        parse_mode="Markdown",
    )

    del sessions[user_id]

# ================= MESSAGE HANDLER =================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = update.message.text

    # Admin → allow everything
    if user.id in ADMIN_IDS:
        return

    # Offensive → block + log
    if contains_offensive(text):
        blocked_logs.append({
            "date": today(),
            "time": now_time(),
            "user_id": user.id,
            "name": user.first_name,
            "username": user.username,
            "message": text,
        })
        await update.message.reply_text(
            "❌ Please maintain respectful language.\n"
            "Use /start or buttons to continue."
        )
        return

    # Normal text → greeting
    await send_greeting(context, user.id, user.first_name)

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(PollAnswerHandler(handle_answer))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.run_polling()

if __name__ == "__main__":
    main()
