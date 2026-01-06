import os
import csv
import time
import asyncio
import requests
import re
from io import StringIO
from datetime import datetime, timezone, timedelta

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

# ================= TIMEZONE =================

IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    return datetime.now(IST)

def today_date():
    return now_ist().date()

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

OFFENSIVE_WORDS = {"fuck", "shit", "bitch", "asshole", "idiot", "stupid"}

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

DEFAULT_QUESTION_TIME = 20
TRANSITION_DELAY = 1
DEFAULT_MARKS_PER_QUESTION = 2
DEFAULT_NEGATIVE_RATIO = 1 / 3

# ================= STATE =================

sessions = {}
daily_scores = {}
current_quiz_date_key = None

# ================= HELPERS =================

def fetch_csv(url):
    r = requests.get(f"{url}&_ts={int(time.time())}", timeout=15)
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))

def contains_offensive(text):
    return any(w in OFFENSIVE_WORDS for w in re.findall(r"\b\w+\b", text.lower()))

def normalize_sheet_rows(rows):
    normalized = []
    for r in rows:
        raw = r.get("date")
        if not raw:
            continue

        try:
            parsed = datetime.strptime(raw.strip(), "%d-%m-%Y")
        except ValueError:
            continue

        r["_date_obj"] = parsed.date()
        r["_time_limit"] = int(r.get("time", DEFAULT_QUESTION_TIME))
        normalized.append(r)

    return normalized

def get_active_quiz_date(rows):
    today = today_date()
    available = sorted({r["_date_obj"] for r in rows})
    valid = [d for d in available if d <= today]
    return valid[-1] if valid else None

def skip_keyboard(q_index):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip Question", callback_data=f"skip_{q_index}")]
    ])

# ================= EXPLANATION RECORDER =================

def record_explanation(session, q, q_no):
    if len(session["explanations"]) >= q_no:
        return

    question_text = q["question"].replace("\\n", "\n")
    explanation_text = q["explanation"].replace("\\n", "\n")

    session["explanations"].append(
        f"Q{q_no}. {question_text}\n"
        f"*📘Explanation:* {explanation_text}"
    )

# ================= GREETING =================

async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])

    text = (
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "🔹 Daily 10 UPSC Prelims-oriented questions\n"
        "⏱ Timed | ⏭ Skip enabled | 📊 Leaderboard\n\n"
        "👇 Tap below to start"
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

    if query.data == "start_quiz":
        await start_quiz(context, query.from_user.id, query.from_user.first_name)

    elif query.data.startswith("skip_"):
        user_id = query.from_user.id
        s = sessions.get(user_id)

        if not s or s["transitioned"]:
            return

        skip_q_index = int(query.data.split("_")[1])
        if skip_q_index != s["index"]:
            return

        if s["timer"]:
            s["timer"].cancel()

        record_explanation(
            s,
            s["questions"][s["index"]],
            s["index"] + 1
        )

        await advance_question(context, user_id)

# ================= QUIZ START =================

async def start_quiz(context, user_id, name):
    rows = normalize_sheet_rows(fetch_csv(QUIZ_CSV_URL))
    quiz_date = get_active_quiz_date(rows)

    if not quiz_date:
        await context.bot.send_message(chat_id=user_id, text="❌ Quiz not available.")
        return

    questions = [r for r in rows if r["_date_obj"] == quiz_date]

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "attempted": 0,
        "wrong": 0,
        "score": 0,
        "marks": 0.0,
        "start": time.time(),
        "transitioned": False,
        "timer": None,
        "active_poll_id": None,
        "name": name,
        "explanations": [],
    }

    await send_question(context, user_id)

# ================= QUIZ FLOW =================

async def send_question(context, user_id):
    s = sessions[user_id]

    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    s["transitioned"] = False
    q = s["questions"][s["index"]]

    await context.bot.send_message(
        chat_id=user_id,
        text=f"*Q{s['index'] + 1}.* {q['question']}",
        parse_mode="Markdown",
        reply_markup=skip_keyboard(s["index"])
    )

    poll = await context.bot.send_poll(
        chat_id=user_id,
        question="Choose the correct answer:",
        options=[q["option_a"], q["option_b"], q["option_c"], q["option_d"]],
        type="quiz",
        correct_option_id=ord(q["correct_option"].upper()) - 65,
        open_period=q["_time_limit"],
        is_anonymous=False,
    )

    s["active_poll_id"] = poll.message_id
    s["timer"] = asyncio.create_task(
        question_timeout(context, user_id, s["index"], q["_time_limit"])
    )

async def question_timeout(context, user_id, q_index, t):
    await asyncio.sleep(t)
    s = sessions.get(user_id)

    if not s or s["transitioned"] or q_index != s["index"]:
        return

    record_explanation(s, s["questions"][q_index], q_index + 1)
    await advance_question(context, user_id)

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = sessions.get(update.poll_answer.user.id)
    if not s or s["transitioned"]:
        return

    # ❗ Ignore answers from old polls
    if update.poll_answer.poll_id != context.bot_data.get(
        f"poll_{update.poll_answer.user.id}_{s['index']}", update.poll_answer.poll_id
    ):
        return

    if s["timer"]:
        s["timer"].cancel()

    q = s["questions"][s["index"]]
    s["attempted"] += 1

    if update.poll_answer.option_ids[0] == ord(q["correct_option"].upper()) - 65:
        s["score"] += 1
        s["marks"] += DEFAULT_MARKS_PER_QUESTION
    else:
        s["wrong"] += 1
        s["marks"] -= DEFAULT_MARKS_PER_QUESTION * DEFAULT_NEGATIVE_RATIO

    record_explanation(s, q, s["index"] + 1)
    await advance_question(context, update.poll_answer.user.id)

async def advance_question(context, user_id):
    s = sessions[user_id]
    if s["transitioned"]:
        return

    s["transitioned"] = True
    s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    skipped = len(s["questions"]) - s["attempted"]

    await context.bot.send_message(
        chat_id=user_id,
        text=f"🏁 Quiz Finished\n\nAttempted: {s['attempted']}\nSkipped: {skipped}\nMarks: {round(s['marks'],2)}"
    )

    del sessions[user_id]

# ================= TEXT HANDLER =================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)

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
