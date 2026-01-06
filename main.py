### Complete updated `main.py`

```python
import os
import csv
import time
import asyncio
import requests
import re
from io import StringIO
from datetime import datetime, timezone, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.helpers import escape_markdown
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
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

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
    try:
        r = requests.get(f"{url}&_ts={int(time.time())}", timeout=15)
        r.raise_for_status()
        return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))
    except Exception:
        # propagate to caller to handle user-friendly message
        raise


def contains_offensive(text):
    if not text:
        return False
    return any(w in OFFENSIVE_WORDS for w in re.findall(r"\b\w+\b", text.lower()))


def normalize_sheet_rows(rows):
    normalized = []
    for r in rows:
        raw = r.get("date")
        if not raw:
            continue

        try:
            parsed = datetime.strptime(raw.strip(), "%d-%m-%Y")
        except Exception:
            continue

        # safe time parsing
        time_val = r.get("time", "")
        try:
            time_limit = int(time_val) if str(time_val).strip() else DEFAULT_QUESTION_TIME
        except (ValueError, TypeError):
            time_limit = DEFAULT_QUESTION_TIME

        # ensure required fields exist with safe defaults
        r["_date_obj"] = parsed.date()
        r["_time_limit"] = time_limit
        r["question"] = r.get("question", "No question provided")
        r["option_a"] = r.get("option_a", "Option A")
        r["option_b"] = r.get("option_b", "Option B")
        r["option_c"] = r.get("option_c", "Option C")
        r["option_d"] = r.get("option_d", "Option D")
        r["correct_option"] = r.get("correct_option", "A")
        r["explanation"] = r.get("explanation", "No explanation provided")
        normalized.append(r)

    return normalized


def get_active_quiz_date(rows):
    today = today_date()
    available = sorted({r["_date_obj"] for r in rows})
    valid = [d for d in available if d <= today]
    return valid[-1] if valid else None


# ================= EXPLANATION RECORDER =================


def record_explanation(session, q, q_no):
    # ensure we only record once per question
    if len(session["explanations"]) >= q_no:
        return

    question_text = q.get("question", "").replace("\\n", "\n")
    explanation_text = q.get("explanation", "").replace("\\n", "\n")

    session["explanations"].append(
        f"Q{q_no}. {question_text}\n"
        f"*📘Explanation:* {explanation_text}"
    )


# ================= GREETING =================


async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
            [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")],
        ]
    )

    text = (
        "📘 *Welcome to Vyasify Daily Quiz*\n\n"
        "This is a daily practice platform for aspirants of 🎯 *UPSC | SSC | Regulatory Body Examinations*\n\n"
        "🔹 *Daily 10 questions* strictly aligned to *UPSC Prelims-oriented topics*\n\n"
        "✅ Correct Answer: 2 Marks\n"
        "❌ Negative Marking: -1/3 Marks\n"
        "🚫 Skipped: 0 Marks\n\n"
        "📝 Timed questions to build exam temperament\n"
        "📊 Score, Rank & Percentile for self-benchmarking\n"
        "📖 Simple explanations for concept clarity\n\n"
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
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)


# ================= BUTTON HANDLER =================


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data or ""
    if data == "start_quiz":
        await start_quiz(context, query.from_user.id, query.from_user.first_name)
        return

    if data == "how_it_works":
        await context.bot.send_message(
            chat_id=query.from_user.id,
            text=(
                "ℹ️ *How the Daily Quiz Works*\n\n"
                "• 10 exam-oriented questions daily\n"
                "• Timed per question\n"
                "• UPSC-style marking\n"
                "• Leaderboard based on first attempt\n"
                "• Explanations after completion"
            ),
            parse_mode="Markdown",
        )
        return

    # Handle skip callbacks of the form "skip:<index>"
    if data.startswith("skip:"):
        try:
            q_index = int(data.split(":", 1)[1])
        except Exception:
            return

        user_id = query.from_user.id
        s = sessions.get(user_id)
        if not s:
            await context.bot.send_message(chat_id=user_id, text="❌ You don't have an active quiz right now.")
            return

        # Validate that the skip is for the current question and not already transitioned
        if s.get("transitioned") or s.get("index") != q_index:
            # ignore stale/invalid skip presses
            return

        # Cancel timer safely
        timer = s.get("timer")
        if timer and not timer.done():
            timer.cancel()
            try:
                await timer
            except asyncio.CancelledError:
                pass

        # Try to stop the poll message so it doesn't remain open
        try:
            if s.get("poll_message_id"):
                await context.bot.stop_poll(chat_id=user_id, message_id=s["poll_message_id"])
        except Exception:
            # ignore stop_poll errors; poll may already be closed
            pass

        # Mark skipped and record explanation
        s["skipped"] = s.get("skipped", 0) + 1
        record_explanation(s, s["questions"][q_index], q_index + 1)

        # Advance to next question
        await advance_question(context, user_id)
        return


# ================= QUIZ START =================


async def start_quiz(context, user_id, name):
    global current_quiz_date_key, daily_scores

    try:
        rows = normalize_sheet_rows(fetch_csv(QUIZ_CSV_URL))
    except Exception:
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ Unable to fetch today's quiz. Please try again later."
        )
        return

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

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "current_q_index": 0,
        "score": 0,
        "attempted": 0,
        "wrong": 0,
        "skipped": 0,  # NEW: explicit skipped counter
        "marks": 0.0,
        "start": time.time(),
        "transitioned": False,
        "poll_message_id": None,
        "timer": None,
        "name": name,
        "explanations": [],
    }

    await send_question(context, user_id)


# ================= QUIZ FLOW =================


async def send_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    q = s["questions"][s["index"]]
    s["current_q_index"] = s["index"]
    s["transitioned"] = False

    # Escape question and options for MarkdownV2
    question_text = escape_markdown(q.get("question", "").replace("\\n", "\n"), version=2)
    options = [
        escape_markdown(q.get("option_a", ""), version=2),
        escape_markdown(q.get("option_b", ""), version=2),
        escape_markdown(q.get("option_c", ""), version=2),
        escape_markdown(q.get("option_d", ""), version=2),
    ]

    # Send question text
    await context.bot.send_message(
        chat_id=user_id,
        text=f"*Q{s['index'] + 1}.* {question_text}",
        parse_mode="MarkdownV2"
    )

    # Inline keyboard with Skip button. callback_data includes the question index for validation.
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton("⏭ Skip", callback_data=f"skip:{s['index']}")]]
    )

    try:
        poll = await context.bot.send_poll(
            chat_id=user_id,
            question="Choose the correct answer:",
            options=options,
            type="quiz",
            correct_option_id=max(0, ord(q.get("correct_option", "A").strip().upper()) - 65),
            open_period=q.get("_time_limit", DEFAULT_QUESTION_TIME),
            is_anonymous=False,
            reply_markup=keyboard,
        )
    except Exception:
        # If poll creation fails, mark as skipped and move on
        s["skipped"] = s.get("skipped", 0) + 1
        record_explanation(s, q, s["index"] + 1)
        await advance_question(context, user_id)
        return

    s["poll_message_id"] = poll.message_id
    # Use asyncio task for timeout; cancel/await safely elsewhere
    s["timer"] = asyncio.create_task(question_timeout(context, user_id, s["index"], q.get("_time_limit", DEFAULT_QUESTION_TIME)))


async def question_timeout(context, user_id, q_index, t):
    await asyncio.sleep(t)
    s = sessions.get(user_id)
    if not s or s.get("transitioned"):
        return

    # Count as skipped on timeout
    s["skipped"] = s.get("skipped", 0) + 1

    record_explanation(s, s["questions"][q_index], q_index + 1)
    await advance_question(context, user_id)


async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    poll_answer = update.poll_answer
    user = getattr(poll_answer, "user", None)
    if not user:
        return

    s = sessions.get(user.id)
    if not s or s.get("transitioned"):
        return

    # cancel timer safely
    timer = s.get("timer")
    if timer and not timer.done():
        timer.cancel()
        try:
            await timer
        except asyncio.CancelledError:
            pass

    q = s["questions"][s["index"]]
    s["attempted"] += 1

    option_ids = poll_answer.option_ids
    if not option_ids:
        # no option selected; treat as skip
        s["skipped"] = s.get("skipped", 0) + 1
        record_explanation(s, q, s["index"] + 1)
        await advance_question(context, user.id)
        return

    selected = option_ids[0]
    try:
        correct = ord(q.get("correct_option", "A").strip().upper()) - 65
    except Exception:
        correct = 0

    if selected == correct:
        s["score"] += 1
        s["marks"] += DEFAULT_MARKS_PER_QUESTION
    else:
        s["wrong"] += 1
        s["marks"] -= DEFAULT_MARKS_PER_QUESTION * DEFAULT_NEGATIVE_RATIO

    record_explanation(s, q, s["index"] + 1)
    await advance_question(context, user.id)


async def advance_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    if s.get("transitioned"):
        return

    s["transitioned"] = True
    s["index"] += 1

    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)


# ================= RESULT =================


async def finish_quiz(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    total = len(s["questions"])
    # skipped is tracked explicitly (timeouts and skip button)
    skipped = s.get("skipped", 0)
    time_taken = int(time.time() - s["start"])

    if user_id not in daily_scores:
        daily_scores[user_id] = {
            "name": s["name"],
            "score": round(s["marks"], 2),
            "time": time_taken
        }

    ranked = sorted(daily_scores.values(), key=lambda x: (-x["score"], x["time"]))[:10]

    leaderboard = ""
    for i, r in enumerate(ranked, 1):
        m, sec = divmod(r["time"], 60)
        leaderboard += f"{i}. {r['name']} — {r['score']} | {m}m {sec}s\n"

    await context.bot.send_message(
        chat_id=user_id,
        text=(
            "🏁 *Quiz Finished!*\n\n"
            f"📝 Attempted: {s['attempted']}/{total}\n"
            f"✅ Correct: {s['score']}\n"
            f"❌ Wrong: {s['wrong']}\n"
            f"⏭ Skipped: {skipped}\n"
            f"🎯 Marks: {round(s['marks'],2)}\n"
            f"⏱ Time: {time_taken//60}m {time_taken%60}s\n\n"
            "🏆 *Daily Leaderboard (Top 10)*\n"
            f"{leaderboard}"
        ),
        parse_mode="Markdown"
    )

    if s["explanations"]:
        header = "📖 *Simple Explanations*\n\n"
        chunk = header

        for exp in s["explanations"]:
            if len(chunk) + len(exp) > 3800:
                await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="Markdown")
                chunk = header
            chunk += exp + "\n\n"

        if chunk.strip() != header.strip():
            await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="Markdown")

    # cleanup
    try:
        del sessions[user_id]
    except KeyError:
        pass


# ================= TEXT HANDLER =================


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text if update.message else ""
    if contains_offensive(text):
        await update.message.reply_text("❌ Please maintain respectful language. Send Hi to start the QUIZ.")
        return
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
```
