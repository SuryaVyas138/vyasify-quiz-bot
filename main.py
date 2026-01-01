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

QUIZ_RELEASE_HOUR = 17  # 5 PM IST

def now_ist():
    return datetime.now(IST)

def today():
    return now_ist().strftime("%d-%m-%Y")

def now_time():
    return now_ist().strftime("%H:%M:%S")

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

ADMIN_IDS = {2053638316}

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

def get_active_quiz_date(rows):
    """
    Decide which quiz date to serve based on time.
    """
    available_dates = sorted(
        {r["date"].strip() for r in rows},
        key=lambda d: datetime.strptime(d, "%d-%m-%Y")
    )

    if not available_dates:
        return None

    current_time = now_ist()

    # Before 5 PM → serve latest available quiz BEFORE today
    if current_time.hour < QUIZ_RELEASE_HOUR:
        for d in reversed(available_dates):
            if d < today():
                return d
        return available_dates[0]

    # After 5 PM → serve today's quiz if available
    return today() if today() in available_dates else available_dates[-1]

# ================= GREETING =================

async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])

    text = (
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "This is a *daily exam-oriented quiz* designed specifically for:\n"
        "🎯 *UPSC | SSC | Regulatory Body Examinations*\n\n"
        "📝 *20 seconds per question*\n"
        "📊 *Score, Rank & Percentile*\n"
        "📖 *Detailed explanations at the end*\n\n"
        "Practice daily. Compete smartly. Improve consistently.\n\n"
        "👇 *Tap below to begin*"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

# ================= COMMAND =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)

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
                "ℹ️ *How It Works*\n\n"
                "• Daily exam-style MCQs\n"
                "• 20 seconds per question\n"
                "• Score, rank & leaderboard\n"
                "• Detailed explanations\n"
            ),
            parse_mode="Markdown",
        )

# ================= QUIZ START =================

async def start_quiz(context, user_id, name):
    rows = fetch_csv(QUIZ_CSV_URL)
    quiz_date = get_active_quiz_date(rows)

    if not quiz_date:
        await context.bot.send_message(chat_id=user_id, text="❌ No quiz available.")
        return

    questions = [r for r in rows if r["date"].strip() == quiz_date]

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

    await context.bot.send_message(
        chat_id=user_id,
        text=f"📘 Quiz for *{quiz_date}* starting now…",
        parse_mode="Markdown"
    )

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
    if not s or not s["active"]:
        return

    store_explanation(s)
    s["active"] = False
    s["index"] += 1
    await send_question(context, user_id)

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    s = sessions.get(user_id)
    if not s or not s["active"]:
        return

    if s["timer"]:
        s["timer"].cancel()

    q = s["questions"][s["index"]]
    if update.poll_answer.option_ids[0] == ord(q["correct_option"].strip()) - 65:
        s["score"] += 1

    store_explanation(s)
    s["active"] = False
    s["index"] += 1
    await send_question(context, user_id)

# ================= EXPLANATION =================

def store_explanation(session):
    q = session["questions"][session["index"]]
    session["explanations"].append(
        f"Q{session['index']+1}. {q['question']}\n\n"
        f"{q['explanation']}\n\nSource: {q['source']}"
    )

# ================= FINAL RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    correct = s["score"]

    time_taken = int(time.time() - s["start"])
    minutes, seconds = divmod(time_taken, 60)

    accuracy = int((correct / total) * 100)

    daily_scores[user_id] = {
        "name": s["name"],
        "score": correct,
        "time": time_taken
    }

    ranked = sorted(
        daily_scores.values(),
        key=lambda x: (-x["score"], x["time"])
    )[:10]

    leaderboard = "\n".join(
        f"{i+1}. {e['name']} — {e['score']} | {e['time']//60}m {e['time']%60}s"
        for i, e in enumerate(ranked)
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"✅ Correct: {correct}\n"
            f"❌ Wrong: {total - correct}\n"
            f"🎯 Accuracy: {accuracy}%\n"
            f"⏱ Time Taken: {minutes}m {seconds}s\n\n"
            "🏆 *Daily Leaderboard*\n"
            f"{leaderboard}"
        ),
        parse_mode="Markdown"
    )

    del sessions[user_id]

# ================= MESSAGE HANDLER =================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text

    if contains_offensive(text):
        await update.message.reply_text(
            "❌ Please maintain respectful language."
        )

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
