import os
import csv
import time
import asyncio
import requests
from io import StringIO
from datetime import datetime

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
    ContextTypes,
)

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

QUIZ_CSV_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
    "/pub?output=csv"
)

QUESTION_TIME = 20
TRANSITION_DELAY = 1

# ================= STORAGE =================

sessions = {}          # user_id -> session
daily_scores = {}      # date -> [(user_id, score, time)]

# ================= HELPERS =================

def today():
    return datetime.now().strftime("%d-%m-%Y")

def fetch_csv(url):
    url = f"{url}&_ts={int(time.time())}"
    r = requests.get(url, timeout=15, headers={"Cache-Control": "no-cache"})
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))

def compute_rank(date, user_id):
    records = daily_scores.get(date, [])
    records.sort(key=lambda x: (-x[1], x[2]))  # score desc, time asc

    total = len(records)
    for i, r in enumerate(records, start=1):
        if r[0] == user_id:
            percentile = int(((total - i) / total) * 100)
            return i, total, percentile

    return total, total, 0

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

    # Reset old session
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
        text="📘 Daily Quiz Initialising…"
    )
    await asyncio.sleep(0.8)

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            f"✅ Quiz Ready!\n\n"
            f"🏁 Questions: {len(questions)}\n"
            f"⏱ Time per question: {QUESTION_TIME} seconds"
        )
    )

    await asyncio.sleep(1)
    await send_question(context, user_id)

# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 *Vyasify Daily Quiz*\n\n"
        "📝 Use /daily to start today’s quiz",
        parse_mode="Markdown",
    )

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    await start_quiz(context, user.id, user.first_name)

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

    # 💡 Explanation INSIDE poll (Telegram will auto-show it)
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
    correct = ord(q["correct_option"].strip()) - 65

    if update.poll_answer.option_ids[0] == correct:
        s["score"] += 1

    store_explanation(s)
    s["active"] = False
    s["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= EXPLANATION STORAGE =================

def store_explanation(session):
    q = session["questions"][session["index"]]
    session["explanations"].append(
        f"Q{session['index'] + 1}. {q['question']}\n"
        f"✔ Correct: Option {q['correct_option']}\n"
        f"Explanation: {q['explanation']}\n"
        f"Source: {q['source']}"
    )

# ================= FINAL RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    time_taken = int(time.time() - s["start"])
    date = today()

    daily_scores.setdefault(date, []).append(
        (user_id, s["score"], time_taken)
    )

    rank, total_users, percentile = compute_rank(date, user_id)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Try Again", callback_data="retry")]
    ])

    msg = (
        "🏁 *Quiz Finished!*\n\n"
        f"✅ Correct: {s['score']}\n"
        f"❌ Wrong: {total - s['score']}\n"
        f"⏱ Time: {time_taken//60} min {time_taken%60} sec\n\n"
        f"🏆 Rank: {rank} / {total_users}\n"
        f"📈 You scored higher than {percentile}% of participants\n\n"
        "📖 *Explanations*\n\n" +
        "\n\n".join(s["explanations"])
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=msg,
        reply_markup=keyboard,
        parse_mode="Markdown",
    )

    del sessions[user_id]

# ================= RETRY =================

async def retry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user = query.from_user
    await start_quiz(context, user.id, user.first_name)

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))
    app.add_handler(PollAnswerHandler(handle_answer))
    app.add_handler(CallbackQueryHandler(retry, pattern="retry"))

    app.run_polling()

if __name__ == "__main__":
    main()
