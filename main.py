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
)

# ---------------- CONFIG ----------------

BOT_TOKEN = os.environ.get("BOT_TOKEN")

CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"

# ---------------- COMMANDS ----------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 Welcome to Vyasify Quiz Bot\n\n"
        "📅 Use /daily to attempt today’s quiz."
    )

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

    # If NO questions found
    if len(questions) == 0:
        await update.message.reply_text(
            "❌ Today’s quiz is not yet uploaded.\n"
            "Please check back later."
        )
        return

    # Send ALL available questions
    for q in questions:
        await context.bot.send_poll(
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
            explanation=f"{q['explanation']} (Source: {q['source']})",
            is_anonymous=False,
        )

# ---------------- MAIN ----------------

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))

    app.run_polling()

if __name__ == "__main__":
    main()
