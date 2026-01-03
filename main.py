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

# ================= TIMEZONE =================

IST = timezone(timedelta(hours=5, minutes=30))
QUIZ_RELEASE_HOUR = 17

def now_ist():
    return datetime.now(IST)

def today_date():
    return now_ist().date()

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

OFFENSIVE_WORDS = {"fuck", "shit", "bitch", "asshole", "idiot", "stupid"}

QUIZ_CSV_URL = "YOUR_CSV_URL"

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
    out = []
    for r in rows:
        try:
            r["_date_obj"] = datetime.strptime(r["date"].strip(), "%d-%m-%Y").date()
        except:
            continue
        r["_time_limit"] = int(r.get("time", DEFAULT_QUESTION_TIME))
        out.append(r)
    return out

def get_active_quiz_date(rows):
    today = today_date()
    dates = sorted({r["_date_obj"] for r in rows})
    if now_ist().hour < QUIZ_RELEASE_HOUR:
        return max([d for d in dates if d <= today], default=None)
    return today if today in dates else None

# ================= GREETING =================

async def send_greeting(context, user_id):
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])
    await context.bot.send_message(chat_id=user_id, text="📘 *Welcome to Vyasify Daily Quiz*", reply_markup=kb, parse_mode="Markdown")

# ================= HANDLERS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_greeting(context, update.effective_user.id)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    if update.callback_query.data == "start_quiz":
        await start_quiz(context, update.effective_user.id)

# ================= QUIZ =================

async def start_quiz(context, user_id):
    global current_quiz_date_key, daily_scores

    rows = normalize_sheet_rows(fetch_csv(QUIZ_CSV_URL))
    quiz_date = get_active_quiz_date(rows)
    if not quiz_date:
        await context.bot.send_message(chat_id=user_id, text="❌ Quiz not available.")
        return

    if quiz_date.isoformat() != current_quiz_date_key:
        daily_scores.clear()
        current_quiz_date_key = quiz_date.isoformat()

    questions = [r for r in rows if r["_date_obj"] == quiz_date]

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "score": 0,
        "attempted": 0,
        "wrong": 0,
        "marks": 0.0,
        "start": time.time(),
        "poll_message_id": None,
        "transitioned": False,
        "explanations": []
    }

    await send_question(context, user_id)

async def send_question(context, user_id):
    s = sessions[user_id]
    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    q = s["questions"][s["index"]]
    poll = await context.bot.send_poll(
        chat_id=user_id,
        question=q["question"],
        options=[q["option_a"], q["option_b"], q["option_c"], q["option_d"]],
        type="quiz",
        correct_option_id=ord(q["correct_option"].strip().upper()) - 65,
        open_period=q["_time_limit"],
        is_anonymous=False
    )

    s["poll_message_id"] = poll.message_id
    s["transitioned"] = False
    asyncio.create_task(question_timeout(context, user_id, q["_time_limit"]))

async def question_timeout(context, user_id, t):
    await asyncio.sleep(t)
    s = sessions.get(user_id)
    if not s or s["transitioned"]:
        return
    await advance_question(context, user_id)

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    s = sessions.get(update.poll_answer.user.id)
    if not s or s["transitioned"]:
        return

    q = s["questions"][s["index"]]
    s["attempted"] += 1

    if update.poll_answer.option_ids[0] == ord(q["correct_option"].strip().upper()) - 65:
        s["score"] += 1
        s["marks"] += DEFAULT_MARKS_PER_QUESTION
    else:
        s["wrong"] += 1
        s["marks"] -= DEFAULT_MARKS_PER_QUESTION * DEFAULT_NEGATIVE_RATIO

    await advance_question(context, update.poll_answer.user.id)

async def advance_question(context, user_id):
    s = sessions[user_id]
    s["transitioned"] = True
    s["index"] += 1
    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    skipped = total - s["attempted"]

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"📝 Total Attempted: {s['attempted']}\n"
            f"✅ Correct: {s['score']}\n"
            f"❌ Wrong: {s['wrong']}\n"
            f"⏭ Skipped: {skipped}\n"
            f"🎯 Total Marks: {round(s['marks'], 2)}"
        ),
        parse_mode="Markdown"
    )

    del sessions[user_id]

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(PollAnswerHandler(handle_answer))
    app.run_polling()

if __name__ == "__main__":
    main()
