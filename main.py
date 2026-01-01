import os
import csv
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN")

# ---------- COMMAND: /start ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📘 Welcome to Vyasify Quiz Bot\n\n"
        "Use /daily to attempt today’s 10-question quiz."
    )

# ---------- COMMAND: /daily ----------
async def daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    today = datetime.now().strftime("%d-%m-%Y")
    questions = []

    with open("daily_quiz.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["date"] == today:
                questions.append(row)

    if len(questions) == 0:
        await update.message.reply_text(
            "❌ Today’s quiz is not yet uploaded.\nPlease check back later."
        )
        return

    # Limit strictly to 10 questions
    questions = questions[:10]

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
            correct_option_id=ord(q["correct_option"]) - ord("A"),
            explanation=f"{q['explanation']} (Source: {q['source']})",
            is_anonymous=False,
        )

# ---------- MAIN ----------
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("daily", daily))

    app.run_polling()

if __name__ == "__main__":
    main()


