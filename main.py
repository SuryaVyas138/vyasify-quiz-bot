import os
import csv
import time
import asyncio
import requests
from datetime import datetime
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Poll
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    PollAnswerHandler,
    ContextTypes
)

BOT_TOKEN = os.getenv("BOT_TOKEN")

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

TIME_PER_QUESTION = 20

sessions = {}
daily_scores = {}

def today():
    return datetime.now().strftime("%d-%m-%Y")

def load_questions():
    res = requests.get(CSV_URL, timeout=10)
    rows = list(csv.DictReader(res.text.splitlines()))
    return [r for r in rows if r["date"] == today()]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "This is a daily exam-oriented quiz designed for:\n"
        "🎯 UPSC | SSC | NABARD | Regulatory Exams\n\n"
        "📝 20 seconds per question\n"
        "📊 Score, Rank & Percentile\n"
        "📖 Detailed explanations at the end\n\n"
        "👇 Tap below to begin"
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🧠 Start Daily Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("🔁 Try Again", callback_data="start_quiz")]
    ])

    await update.message.reply_text(text, reply_markup=keyboard, parse_mode="Markdown")

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "start_quiz":
        questions = load_questions()
        if not questions:
            await query.message.reply_text("❌ Today’s quiz is not yet uploaded.")
            return

        user = query.from_user
        sessions[user.id] = {
            "name": user.first_name,
            "questions": questions,
            "index": 0,
            "score": 0,
            "start": time.time(),
            "answers": [],
            "explanations": []
        }

        await query.message.reply_text(
            f"✅ *Quiz Ready!*\n\n"
            f"📚 Questions: {len(questions)}\n"
            f"⏱ Time per question: {TIME_PER_QUESTION} seconds",
            parse_mode="Markdown"
        )

        await asyncio.sleep(1)
        await send_question(context, user.id)

async def send_question(context, user_id):
    s = sessions[user_id]
    q = s["questions"][s["index"]]

    explanation_hint = f"💡 {q['explanation']}"

    poll = await context.bot.send_poll(
        chat_id=user_id,
        question=f"Q{s['index']+1}. {q['question']}",
        options=[q["option a"], q["option b"], q["option c"], q["option d"]],
        type=Poll.QUIZ,
        correct_option_id=ord(q["correct option"]) - 65,
        explanation=explanation_hint,
        explanation_parse_mode="Markdown",
        open_period=TIME_PER_QUESTION,
        is_anonymous=False
    )

    s["poll_id"] = poll.poll.id

async def poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.poll_answer.user.id
    s = sessions.get(user_id)
    if not s:
        return

    q = s["questions"][s["index"]]
    correct = ord(q["correct option"]) - 65

    if update.poll_answer.option_ids:
        chosen = update.poll_answer.option_ids[0]
        if chosen == correct:
            s["score"] += 1

    s["explanations"].append(
        f"Q{s['index']+1}. {q['question']}\n\n"
        f"Explanation:\n{q['explanation']}\n"
        f"Source: {q['source']}"
    )

    s["index"] += 1

    if s["index"] < len(s["questions"]):
        await asyncio.sleep(1)
        await send_question(context, user_id)
    else:
        await finish_quiz(context, user_id)

async def finish_quiz(context, user_id):
    s = sessions[user_id]
    total = len(s["questions"])
    time_taken = int(time.time() - s["start"])
    date = today()

    daily_scores.setdefault(date, []).append(
        (user_id, s["name"], s["score"], time_taken)
    )

    records = sorted(daily_scores[date], key=lambda x: (-x[2], x[3]))
    rank = next(i+1 for i, r in enumerate(records) if r[0] == user_id)
    percentile = int(((len(records) - rank) / len(records)) * 100)

    leaderboard = ["🏆 *Today’s Leaderboard*"]
    for i, r in enumerate(records[:min(10, len(records))], 1):
        leaderboard.append(
            f"{i}. {r[1]} — {r[2]}/{total} — {r[3]}s"
        )

    summary = (
        "🏁 *Quiz Finished!*\n\n"
        f"✅ Score: {s['score']} / {total}\n"
        f"⏱ Time: {time_taken}s\n"
        f"🏆 Rank: {rank}\n"
        f"📈 Percentile: {percentile}%\n\n" +
        "\n".join(leaderboard)
    )

    await context.bot.send_message(
        chat_id=user_id,
        text=summary,
        parse_mode="Markdown"
    )

    await context.bot.send_message(
        chat_id=user_id,
        text="📖 *Explanations*\n\n" + "\n\n".join(s["explanations"]),
        parse_mode="Markdown"
    )

    del sessions[user_id]

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(PollAnswerHandler(poll_answer))
    app.run_polling()

if __name__ == "__main__":
    main()
