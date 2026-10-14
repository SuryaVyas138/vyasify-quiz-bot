import os
import csv
import time
import json
import html
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Poll,
    ParseMode
)
from telegram.ext import (
    Updater,
    CommandHandler,
    CallbackQueryHandler,
    PollAnswerHandler,
    MessageHandler,
    Filters,
    CallbackContext,
)

# ================= CONFIGURATION =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable not set. Please set it in systemd or environment.")
ADMIN_ID = 2053638316

# Google Sheet CSV URLs (Direct instant export + CDN fallback + Apps Script Web App)
QUIZ_CSV_URL = "https://docs.google.com/spreadsheets/d/15JMXTiM1AbI0LAh85R26pBo459vDG-G1i59Vqz6a3wg/export?format=csv&gid=0"
FALLBACK_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"
APPS_SCRIPT_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbysyuvXUbTu65lRIpBHpihOKsELv0-92fQN71SbLqC1laDTiN3PDkJqIOVIjMWxx3z8iQ/exec"

# ⚙️ SCORING RULES
CORRECT_MARK = 2.0
WRONG_MARK = 0.33
DEFAULT_TIME = 20
CYCLE_HOUR = 9  # Updates at 09:00 AM IST

# File to save leaderboard
LEADERBOARD_FILE = "leaderboard.json"

# ================= GLOBAL STATE =================

sessions = {}
leaderboard = {}

# ================= HELPER FUNCTIONS =================

def now_ist():
    """
    Returns current datetime in IST (UTC + 5:30).
    Manually calculated to ensure server independence.
    """
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_quiz_date_key():
    """
    Determines the active quiz date based on the 09:00 AM cycle.
    - Time < 09:00 AM: Fetch Yesterday's Quiz
    - Time >= 09:00 AM: Fetch Today's Quiz
    """
    now = now_ist()
    
    if now.hour < CYCLE_HOUR:
        # Before 9 AM: Fetch Yesterday's Quiz
        target_date = now.date() - timedelta(days=1)
    else:
        # After 9 AM: Fetch Today's Quiz
        target_date = now.date()
        
    return target_date.isoformat()

def clean_text(text):
    """
    Robust text cleaner for HTML parsing.
    1. Replaces literal '\\n' with actual newline.
    2. Escapes HTML special characters (<, >, &) to prevent injection/crashes.
    """
    if not text: return ""
    # Fix Newlines first
    text = text.replace('\\n', '\n').replace('\r\n', '\n')
    # Escape HTML characters
    return html.escape(text)

def format_time(seconds):
    """Formats seconds into '1m 41s' style."""
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"

def truncate(text, limit=100):
    text = text.replace("\\n", " ").strip()
    return text if len(text) <= limit else text[:limit - 3] + "..."

# ================= DATA MANAGEMENT =================

def load_leaderboard():
    global leaderboard
    if os.path.exists(LEADERBOARD_FILE):
        try:
            with open(LEADERBOARD_FILE, "r") as f:
                leaderboard = json.load(f)
        except:
            leaderboard = {}

def save_leaderboard():
    with open(LEADERBOARD_FILE, "w") as f:
        json.dump(leaderboard, f)

def fetch_questions(date_key=None):
    """
    Fetches questions for a specific date (YYYY-MM-DD).
    If no date provided, calculates based on 9 AM logic.
    Supports multi-tier retrieval: Direct CSV -> Cached CSV -> Apps Script WebApp -> Vyasify API
    """
    # Determine target date
    if date_key:
        target_date = datetime.strptime(date_key, "%Y-%m-%d").date()
    else:
        target_date = datetime.strptime(get_quiz_date_key(), "%Y-%m-%d").date()

    # Tier 1 & 2: Primary Direct Export (instant) & Fallback Published CSV
    for url in [QUIZ_CSV_URL, FALLBACK_CSV_URL]:
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            content = r.content.decode("utf-8-sig")
            rows = list(csv.DictReader(StringIO(content)))

            valid_questions = []
            for row in rows:
                try:
                    row = {k.strip(): v for k, v in row.items()}
                    clean_date = str(row.get("date", "")).strip().lstrip("'")
                    row_date = datetime.strptime(clean_date, "%d-%m-%Y").date()

                    if row_date == target_date:
                        time_val = str(row.get("time", "")).strip()
                        if time_val and time_val.isdigit():
                            row["_time"] = int(time_val)
                        else:
                            row["_time"] = DEFAULT_TIME
                        valid_questions.append(row)
                except Exception:
                    continue

            if valid_questions:
                return valid_questions
        except Exception as e:
            print(f"CSV Fetch Error ({url}): {e}")

    # Tier 3: Google Apps Script Web App Endpoint
    try:
        sheet_date = target_date.strftime("%d-%m-%Y")
        res = requests.get(f"{APPS_SCRIPT_WEBAPP_URL}?date={sheet_date}", timeout=15)
        if res.status_code == 200:
            data = res.json()
            qs = data.get("questions", [])
            valid_questions = []
            for q in qs:
                clean_q = {k.strip(): v for k, v in q.items()}
                time_val = str(clean_q.get("time", "")).strip()
                clean_q["_time"] = int(time_val) if time_val.isdigit() else DEFAULT_TIME
                valid_questions.append(clean_q)
            if valid_questions:
                return valid_questions
    except Exception as e:
        print(f"Apps Script WebApp Fetch Error: {e}")

    # Tier 4: Direct Vyasify Web API Fallback
    try:
        iso_str = target_date.isoformat()
        api_res = requests.get(f"https://vyasify.in/api/current-affairs/daily-quiz?date={iso_str}", timeout=15)
        if api_res.status_code == 200:
            api_data = api_res.json()
            qs = api_data.get("questions", [])
            valid_questions = []
            for q in qs:
                opts = q.get("options", [])
                clean_correct = str(q.get("answer", "A")).strip().upper()
                if len(clean_correct) > 1 and clean_correct[0] in "ABCD":
                    clean_correct = clean_correct[0]
                row_fmt = {
                    "date": target_date.strftime("%d-%m-%Y"),
                    "topic": q.get("sourceArticleTitle", "Current Affairs"),
                    "question": q.get("question", ""),
                    "option_a": opts[0] if len(opts) > 0 else "",
                    "option_b": opts[1] if len(opts) > 1 else "",
                    "option_c": opts[2] if len(opts) > 2 else "",
                    "option_d": opts[3] if len(opts) > 3 else "",
                    "correct_option": clean_correct,
                    "explanation": q.get("explanation", ""),
                    "source": "Vyasify Current Affairs",
                    "time": 90,
                    "_time": 90,
                    "marks": 2,
                    "negative_ratio": 0.66
                }
                valid_questions.append(row_fmt)
            if valid_questions:
                return valid_questions
    except Exception as e:
        print(f"Direct API Fetch Error: {e}")

    return []

# ================= KEYBOARDS =================

def start_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶ Start Today's Quiz", callback_data="action_start")],
        [InlineKeyboardButton("ℹ How it works", callback_data="action_help")]
    ])

def skip_keyboard(idx):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip Question", callback_data=f"skip:{idx}")]
    ])

def back_home_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 Back to Menu", callback_data="action_home")]
    ])

# ================= HANDLERS =================

def welcome_user(update: Update, context: CallbackContext):
    """Sends the styled Welcome Message."""
    user = update.effective_user
    name = user.first_name.upper() if user.first_name else "ASPIRANT"
    
    # Fetch questions for the current cycle to get Topic and Time
    questions = fetch_questions()
    
    if questions:
        if "topic" in questions[0]:
            topic_name = questions[0]["topic"].strip()
        else:
            topic_name = "General Studies"
        # Get duration from the first question
        q_duration = questions[0].get("_time", DEFAULT_TIME)
    else:
        topic_name = "General Studies"
        q_duration = DEFAULT_TIME

    # Styled Text with HTML Blockquotes
    text = (
        f"📘 Welcome <b>{clean_text(name)}</b> to Vyasify Daily Quiz\n\n"
        "This is a daily practice platform for aspirants of 🎯 UPSC | SSC | Regulatory Body Examinations\n\n"
        f"<blockquote>⭐ Today's Topic: <b>{clean_text(topic_name)}</b> ❞</blockquote>\n"
        f"<blockquote>⏳ Time per Question: <b>{q_duration} Seconds</b></blockquote>\n\n"
        "🔷 Daily 10 questions strictly aligned to UPSC Prelims-oriented topics\n\n"
        f"✅ Correct Answer: +{int(CORRECT_MARK)} Marks\n"
        f"❌ Negative Marking: -{WRONG_MARK} Marks\n"
        "⏭ Skip: 0 Marks\n\n"
        "📝 Timed questions to build exam temperament\n"
        "📊 Score, Rank & Percentile for self-benchmarking\n"
        "📖 Simple explanations for concept clarity\n\n"
        "👇 Tap below to start today’s quiz"
    )
    
    try:
        if update.callback_query:
            update.callback_query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )
        else:
            context.bot.send_message(
                chat_id=user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )
    except:
        context.bot.send_message(
            chat_id=user.id,
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=start_keyboard()
        )

def show_how_it_works(update: Update, context: CallbackContext):
    """Displays the How It Works information."""
    
    # Fetch active questions to display correct duration
    questions = fetch_questions()
    if questions:
        q_duration = questions[0].get("_time", DEFAULT_TIME)
    else:
        q_duration = DEFAULT_TIME

    text = (
        "ℹ️ <b>How Vyasify Quiz Works</b>\n\n"
        "📅 <b>Daily Cycle:</b> \n"
        "New questions are updated exactly at <b>09:00 AM IST</b>. \n"
        "• Before 9 AM: You get Yesterday's quiz.\n"
        "• After 9 AM: You get Today's quiz.\n\n"
        "⏱ <b>Timer Rules:</b> \n"
        f"Today, each question has a limit of <b>{q_duration} seconds</b>. \n"
        "If the time runs out, the question is marked as <b>Skipped</b>.\n\n"
        "🏆 <b>Leaderboard:</b> \n"
        "Ranks are calculated based on: \n"
        "1. Highest Score\n"
        "2. Lowest Time Taken\n"
        "The leaderboard resets daily at 09:00 AM.\n\n"
        "Good luck with your preparation! 📝"
    )
    
    update.callback_query.edit_message_text(
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=back_home_keyboard()
    )

def start_quiz_session(update: Update, context: CallbackContext):
    """Initializes the quiz session with Build-Up Wave animation."""
    uid = update.effective_user.id
    name = update.effective_user.first_name if update.effective_user.first_name else "Aspirant"
    
    # 1. Cleanup
    if uid in sessions:
        try:
            if sessions[uid].get("poll_msg_id"):
                context.bot.stop_poll(uid, sessions[uid]["poll_msg_id"])
        except: pass
        del sessions[uid]
    
    for job in context.job_queue.get_jobs_by_name(str(uid)):
        job.schedule_removal()

    # 2. Fetch Data & Lock Date Key
    # We lock the key now so scores save to the correct day even if time crosses 9 AM during quiz
    current_date_key = get_quiz_date_key()
    questions = fetch_questions(current_date_key)
    
    if not questions:
        context.bot.send_message(uid, "❌ <b>No quiz available for the current cycle.</b> \n(Next update at 09:00 AM IST).", parse_mode=ParseMode.HTML)
        return

    # 3. Build-Up Wave Animation (With Emojis & Text)
    try:
        msg = context.bot.send_message(uid, "⏳ Initializing...", parse_mode=ParseMode.HTML)
        
        # 3 Series
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 3️⃣", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 3️⃣ .", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 3️⃣ . .", parse_mode=ParseMode.HTML)
        
        # 2 Series
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 2️⃣", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 2️⃣ .", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 2️⃣ . .", parse_mode=ParseMode.HTML)

        # 1 Series
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 1️⃣", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 1️⃣ .", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
        msg.edit_text("Quiz Starts in 1️⃣ . .", parse_mode=ParseMode.HTML)
        
        # GO
        time.sleep(0.5)
        msg.edit_text(f"🚀 Goodluck <b>{clean_text(name)}</b>! 🤝", parse_mode=ParseMode.HTML)
        time.sleep(0.5)
    except Exception as e:
        # Fallback if animation fails (e.g. rate limits)
        pass

    # 4. Initialize Session
    sessions[uid] = {
        "questions": questions,
        "date_key": current_date_key, 
        "total_q": len(questions),
        "index": 0,
        "marks": 0.0,
        "correct": 0,
        "wrong": 0,
        "skipped": 0,
        "locked": False,
        "poll_id": None,
        "poll_msg_id": None,
        "start_time": time.time(),
        "name": update.effective_user.full_name,
        "explanations": [],
    }

    send_question(context, uid)

def send_question(context: CallbackContext, uid):
    s = sessions.get(uid)
    if not s: return

    if s["index"] >= len(s["questions"]):
        finish_quiz(context, uid)
        return

    s["locked"] = False
    q = s["questions"][s["index"]]
    
    context.bot.send_message(
        uid,
        f"<b>Q{s['index'] + 1}.</b> {clean_text(q['question'])}",
        parse_mode=ParseMode.HTML,
        reply_markup=skip_keyboard(s["index"])
    )

    options = [
        truncate(q.get("option_a", "")),
        truncate(q.get("option_b", "")),
        truncate(q.get("option_c", "")),
        truncate(q.get("option_d", ""))
    ]
    
    try:
        poll = context.bot.send_poll(
            uid,
            "Select the correct option:",
            options,
            type=Poll.QUIZ,
            correct_option_id=ord(q["correct_option"].strip().upper()) - 65,
            open_period=q["_time"],
            is_anonymous=False,
        )

        s["poll_id"] = poll.poll.id
        s["poll_msg_id"] = poll.message_id

        context.job_queue.run_once(
            timeout_handler,
            q["_time"] + 1,
            context={"uid": uid, "idx": s["index"]},
            name=str(uid)
        )
    except Exception as e:
        context.bot.send_message(uid, f"⚠️ Error with Q{s['index']+1}. Moving to next...")
        s["index"] += 1
        send_question(context, uid)

def timeout_handler(context: CallbackContext):
    data = context.job.context
    uid = data["uid"]
    idx = data["idx"]

    s = sessions.get(uid)
    if not s or s["locked"] or s["index"] != idx:
        return

    s["skipped"] += 1 
    handle_next_step(context, uid, "Time Up")

def handle_next_step(context, uid, status_label):
    s = sessions.get(uid)
    s["locked"] = True
    
    try:
        context.bot.stop_poll(uid, s["poll_msg_id"])
    except: pass
    
    status_emoji = ""
    if status_label == "Skipped":
        status_emoji = "⏭ <b>Skipped</b>"
    elif status_label == "Time Up":
        status_emoji = "⏰ <b>Time Up</b>"

    q = s["questions"][s["index"]]
    
    s["explanations"].append(
        f"<b>Q{s['index']+1}.</b> {clean_text(q['question'])}\n\n{status_emoji}\n\n📘 <b>Explanation:</b>\n{clean_text(q.get('explanation', 'N/A'))}\n\n------------------------------------------------"
    )

    s["index"] += 1
    send_question(context, uid)

# ================= INTERACTION HANDLERS =================

def poll_answer_handler(update: Update, context: CallbackContext):
    answer = update.poll_answer
    uid = answer.user.id
    s = sessions.get(uid)

    if not s or s["locked"] or answer.poll_id != s["poll_id"]:
        return

    s["locked"] = True
    
    for job in context.job_queue.get_jobs_by_name(str(uid)):
        job.schedule_removal()

    q = s["questions"][s["index"]]
    correct_idx = ord(q["correct_option"].strip().upper()) - 65
    selected = answer.option_ids[0]

    status_emoji = ""

    if selected == correct_idx:
        s["marks"] += CORRECT_MARK
        s["correct"] += 1
        status_emoji = "✅ <b>Correct</b>"
    else:
        s["marks"] -= WRONG_MARK
        s["wrong"] += 1
        status_emoji = "❌ <b>Wrong</b>"

    s["explanations"].append(
        f"<b>Q{s['index']+1}.</b> {clean_text(q['question'])}\n\n{status_emoji}\n\n📘 <b>Explanation:</b>\n{clean_text(q.get('explanation', 'N/A'))}\n\n------------------------------------------------"
    )

    s["index"] += 1
    time.sleep(1) 
    send_question(context, uid)

def button_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    uid = query.from_user.id
    
    # Optimization: Acknowledge immediately to stop loading spinner
    query.answer()

    if query.data == "action_start":
        start_quiz_session(update, context)
    
    elif query.data == "action_help":
        show_how_it_works(update, context)

    elif query.data == "action_home":
        welcome_user(update, context)

    elif query.data.startswith("skip:"):
        s = sessions.get(uid)
        if s and not s["locked"]:
            idx = int(query.data.split(":")[1])
            if idx == s["index"]:
                for job in context.job_queue.get_jobs_by_name(str(uid)):
                    job.schedule_removal()
                
                s["skipped"] += 1
                handle_next_step(context, uid, "Skipped")

def finish_quiz(context: CallbackContext, uid):
    s = sessions.pop(uid)
    day_key = s.get("date_key", get_quiz_date_key())
    
    duration = time.time() - s["start_time"]
    final_score = round(s["marks"], 2)
    time_str = format_time(duration)
    
    leaderboard.setdefault(day_key, {})[str(uid)] = {
        "name": s["name"].upper(),
        "score": final_score,
        "time": int(duration)
    }
    save_leaderboard()

    context.bot.send_message(
        uid,
        f"🙏 Thank you for attempting today's quiz, <b>{clean_text(s['name'])}</b>!\nGenerating your scorecard...",
        parse_mode=ParseMode.HTML
    )
    time.sleep(1)

    attempted_count = s["correct"] + s["wrong"]
    
    res_text = (
        "🏁 <b>Quiz Finished!</b>\n\n"
        f"📝 Attempted: {attempted_count}/{s['total_q']}\n"
        f"✅ Correct: {s['correct']}\n"
        f"❌ Wrong: {s['wrong']}\n"
        f"⏭ Skipped: {s['skipped']}\n"
        f"🎯 Marks: {final_score}\n"
        f"⏱ Time: {time_str}\n\n"
        "🏆 <b>Daily Leaderboard (Top 10)</b>\n\n"
    )

    ranked = sorted(
        leaderboard[day_key].values(),
        key=lambda x: (-x["score"], x["time"])
    )[:10]

    for i, r in enumerate(ranked, 1):
        r_time = format_time(r['time'])
        res_text += f"{i}. <b>{clean_text(r['name'])}</b> — {r['score']} | {r_time}\n"

    context.bot.send_message(uid, res_text, parse_mode=ParseMode.HTML)

    buffer = "📖 <b>Explanations</b>\n\n"
    for exp in s["explanations"]:
        if len(buffer) + len(exp) > 3800:
            context.bot.send_message(uid, buffer, parse_mode=ParseMode.HTML)
            buffer = ""
        buffer += exp + "\n\n"
    
    if buffer:
        context.bot.send_message(uid, buffer, parse_mode=ParseMode.HTML)

# ================= COMMAND HANDLERS =================

def show_leaderboard(update: Update, context: CallbackContext):
    day_key = get_quiz_date_key()
    load_leaderboard()
    
    current_data = leaderboard.get(day_key, {})
    
    if not current_data:
        update.message.reply_text("📉 No records yet for today's quiz cycle (since 9 AM). Be the first!", parse_mode=ParseMode.HTML)
        return

    ranked = sorted(
        current_data.values(),
        key=lambda x: (-x["score"], x["time"])
    )[:10]

    text = f"🏆 <b>Leaderboard ({day_key})</b>\n\n"
    for i, r in enumerate(ranked, 1):
        text += f"{i}. <b>{clean_text(r['name'])}</b> — {r['score']} | {format_time(r['time'])}\n"
    
    update.message.reply_text(text, parse_mode=ParseMode.HTML)

def text_handler(update: Update, context: CallbackContext):
    text = update.message.text.lower().strip()
    
    BANNED_WORDS = ["fuck", "shit", "sex", "bastard", "bitch", "asshole", "dick", "pussy", "idiot", "stupid"]
    if any(word in text for word in BANNED_WORDS):
        update.message.reply_text("⚠️ <b>Please maintain professional language.</b>\nThis is an educational platform.", parse_mode=ParseMode.HTML)
        return

    if text in ["hi", "hello", "start", "/start", "restart", "hey"]:
        welcome_user(update, context)
        return

    update.message.reply_text(
        "👋 <b>Hello!</b>\n\nTo start today's quiz, please type <b>Hi</b> or click /start.",
        parse_mode=ParseMode.HTML
    )

# ================= MAIN =================

def main():
    load_leaderboard()
    
    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", welcome_user))
    dp.add_handler(CommandHandler("leaderboard", show_leaderboard))
    dp.add_handler(CallbackQueryHandler(button_handler))
    dp.add_handler(PollAnswerHandler(poll_answer_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, text_handler))

    print("✅ Vyasify Bot is Online...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
