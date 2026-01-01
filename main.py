import os
import csv
import time
import asyncio
import requests
from io import StringIO
from datetime import datetime

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    ContextTypes,
)

# ================= CONFIG =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")

QUIZ_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

QUESTION_TIME = 20
TRANSITION_DELAY = 1

sessions = {}

# ================= HELPERS =================

def today():
    return datetime.now().strftime("%d-%m-%Y")

def fetch_csv(url):
    url = f"{url}&_ts={int(time.time())}"
    res = requests.get(url, timeout=15, headers={"Cache-Control": "no-cache"})
    res.raise_for_status()
    return list(csv.DictReader(StringIO(res.content.decode("utf-8-sig"))))

# ================= COMMANDS =================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 *Vyasify Daily Quiz*\n\n"
        "📝 Use /daily to start today’s quiz",
        parse_mode="Markdown"
    )

async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    name = update.effective_user.first_name

    rows = fetch_csv(QUIZ_CSV_URL)
    questions = [r for r in rows if r["date"].strip() == today()]

    if not questions:
        await update.message.reply_text("❌ Today’s quiz is not yet uploaded.")
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
    }

    # Intro messages (forced order)
    await update.message.reply_text("📘 Daily Quiz Initialising…")
    await asyncio.sleep(0.8)

    await update.message.reply_text(
        f"✅ Quiz Ready!\n\n"
        f"🏁 Questions: {len(questions)}\n"
        f"⏱ Time per question: {QUESTION_TIME} seconds"
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

    s["active"] = False
    s["index"] += 1

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)

# ================= FINAL RESULT =================

async def finish_quiz(context, user_id):
    s = sessions[user_id]

    total = len(s["questions"])
    time_taken = int(time.time() - s["start"])

    msg = (
        "🏁 *Quiz Finished!*\n\n"
        f"✅ Correct: {s['score']}\n"
        f"❌ Wrong: {total - s['score']}\n"
        f"⏱ Time: {time_taken//60} min {time_taken%60} sec\n\n"
        "📊 *Ranking will be announced shortly*"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=msg,
        parse_mode="Markdown",
    )

    del sessions[user_id]

# ================= MAIN =================

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))
    app.add_handler(PollAnswerHandler(handle_answer))
    app.run_polling()

if __name__ == "__main__":
    main()
