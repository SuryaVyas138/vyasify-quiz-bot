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
QUIZ_RELEASE_HOUR = 17  # 5 PM IST

def now_ist():
    return datetime.now(IST)

def today_date():
    return now_ist().date()

def today_str():
    return today_date().strftime("%d-%m-%Y")

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

DEFAULT_QUESTION_TIME = 20
TRANSITION_DELAY = 1

# ================= STATE =================

sessions = {}
daily_scores = {}
blocked_logs = []
current_quiz_date_key = None

# ================= HELPERS =================

def fetch_csv(url):
    url = f"{url}&_ts={int(time.time())}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))

def contains_offensive(text: str) -> bool:
    words = re.findall(r"\b\w+\b", text.lower())
    return any(w in OFFENSIVE_WORDS for w in words)

# --------- DATE & TIME NORMALISATION ---------

def normalize_sheet_rows(rows):
    normalized = []

    for r in rows:
        raw = r.get("date")
        if not raw:
            continue

        raw = raw.strip()
        try:
            parsed = datetime.strptime(raw, "%d-%m-%Y")
        except ValueError:
            try:
                parsed = datetime.strptime(raw, "%m-%d-%Y")
            except ValueError:
                continue

        r["_date_obj"] = parsed.date()

        try:
            t = int(r.get("time", DEFAULT_QUESTION_TIME))
            r["_time_limit"] = t if t > 0 else DEFAULT_QUESTION_TIME
        except Exception:
            r["_time_limit"] = DEFAULT_QUESTION_TIME

        normalized.append(r)

    return normalized

def get_active_quiz_date(rows):
    today = today_date()
    available = sorted({r["_date_obj"] for r in rows})

    if not available:
        return None

    if now_ist().hour < QUIZ_RELEASE_HOUR:
        valid = [d for d in available if d <= today]
        return valid[-1] if valid else None

    return today if today in available else None

# ================= GREETING =================

async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])

    text = (
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "This is a focused daily practice platform for aspirants of:\n"
        "🎯 *UPSC | SSC | Regulatory Body Examinations*\n\n"
        "🔹 *Daily 10 questions* strictly aligned to *UPSC Prelims-oriented topics*\n\n"
        "📝 Timed questions to build exam temperament\n"
        "📊 Score, Rank & Percentile for self-benchmarking\n"
        "📖 Simple explanations for concept clarity\n\n"
        "Practice daily and improve accuracy.\n\n"
        "👇 *Tap below to start today’s quiz*"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=text,
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

# ================= COMMAND =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_greeting(
        context,
        update.effective_user.id,
        update.effective_user.first_name
    )

# ================= BUTTON HANDLER =================

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "start_quiz":
        await start_quiz(context, query.from_user.id, query.from_user.first_name)

    elif query.data == "how_it_works":
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=(
                "ℹ️ *How the Daily Quiz Works*\n\n"
                "Each quiz is time-bound and exam-oriented.\n"
                "Answer questions within the given time to build speed and accuracy.\n\n"
                "• Performance-based ranking\n"
                "• Clear explanations after completion\n"
                "• Designed for UPSC Prelims preparation"
            ),
            parse_mode="Markdown",
        )

# ================= QUIZ START =================

async def start_quiz(context, user_id, name):
    global current_quiz_date_key, daily_scores

    rows = normalize_sheet_rows(fetch_csv(QUIZ_CSV_URL))
    quiz_date = get_active_quiz_date(rows)

    if not quiz_date:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Today’s quiz is not yet available."
        )
        return

    quiz_date_key = quiz_date.isoformat()
    if quiz_date_key != current_quiz_date_key:
        daily_scores.clear()
        current_quiz_date_key = quiz_date_key

    questions = [r for r in rows if r["_date_obj"] == quiz_date]
    if not questions:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Quiz data error. Please try again later."
        )
        return

    quiz_topic = (questions[0].get("topic") or "").strip()

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "score": 0,
        "start": time.time(),
        "name": name,
        "active": False,
        "transitioned": False,   # 🔑 SINGLE-ADVANCE GUARD
        "timer": None,
        "finished": False,
        "explanations": [],
    }

    header = f"📘 *Quiz for {quiz_date.strftime('%d-%m-%Y')}*"
    if quiz_topic:
        header += f"\n🧠 *Topic:* {quiz_topic}"

    msg = await context.bot.send_message(
        chat_id=user_id,
        text=f"{header}\n\n⏳ Starting in *3️⃣…*",
        parse_mode="Markdown"
    )

    for n in ["2️⃣…", "1️⃣…"]:
        await asyncio.sleep(1)
        await msg.edit_text(
            f"{header}\n\n⏳ Starting in *{n}*",
            parse_mode="Markdown"
        )

    await asyncio.sleep(1)
    await msg.delete()
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
    t = q["_time_limit"]

    s["active"] = True
    s["transitioned"] = False

    await context.bot.send_poll(
        chat_id=user_id,
        question=q["question"],
        options=[q["option_a"], q["option_b"], q["option_c"], q["option_d"]],
        type="quiz",
        correct_option_id=ord(q["correct_option"].strip()) - 65,
        explanation=q["explanation"],
        is_anonymous=False,
        open_period=t,
    )

    s["timer"] = asyncio.create_task(question_timeout(context, user_id, t))

async def advance_question(context, user_id):
    s = sessions.get(user_id)
    if not s or s["transitioned"]:
        return

    s["transitioned"] = True
    s["active"] = False
    s["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

async def question_timeout(context, user_id, t):
    await asyncio.sleep(t)
    s = sessions.get(user_id)
    if not s or s["transitioned"]:
        return

    store_explanation(s)
    await advance_question(context, user_id)

async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    s = sessions.get(user_id)
    if not s or s["transitioned"]:
        return

    q = s["questions"][s["index"]]
    if update.poll_answer.option_ids[0] == ord(q["correct_option"].strip()) - 65:
        s["score"] += 1

    store_explanation(s)

    if s["timer"]:
        s["timer"].cancel()

    await advance_question(context, user_id)

# ================= EXPLANATIONS =================

def store_explanation(session):
    q = session["questions"][session["index"]]
    session["explanations"].append(
        f"Q{session['index'] + 1}. {q['question']}\n"
        f"🔹 *Explanation:*:\n{q['explanation']}"
        
)


# ================= FINAL RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    correct = s["score"]
    time_taken = int(time.time() - s["start"])

    daily_scores[user_id] = {
        "name": s["name"],
        "score": correct,
        "time": time_taken
    }

    ranked = sorted(
        daily_scores.values(),
        key=lambda x: (-x["score"], x["time"])
    )[:10]

    leaderboard = ""
    for i, r in enumerate(ranked, 1):
        m, sec = divmod(r["time"], 60)
        leaderboard += f"{i}. {r['name']} — {r['score']} | {m}m {sec}s\n"

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"✅ Correct: {correct}\n"
            f"❌ Wrong: {total - correct}\n"
            f"⏱ Time: {time_taken//60}m {time_taken%60}s\n\n"
            "🏆 *Daily Leaderboard (Top 10)*\n"
            f"{leaderboard}"
        ),
        parse_mode="Markdown"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text="📖 *Simple Explanations*\n\n" + "\n\n".join(s["explanations"]),
        parse_mode="Markdown"
    )

    del sessions[user_id]

# ================= MESSAGE HANDLER =================

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if contains_offensive(update.message.text):
        await update.message.reply_text(
            "❌ Please maintain respectful language."
        )
        return
    await send_greeting(
        context,
        update.effective_user.id,
        update.effective_user.first_name
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
