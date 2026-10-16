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

# ================= CONFIGURATION & CONSTANTS =================

BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable not set. Please set it in systemd or environment.")
ADMIN_ID = 2053638316

# API Endpoints for Dynamic Settings & Leaderboard Sync
CONFIG_API_URL = os.environ.get("CONFIG_API_URL", "https://vyasify.in/api/telegram-bot/config")
LEADERBOARD_API_URL = os.environ.get("LEADERBOARD_API_URL", "https://vyasify.in/api/telegram-bot/leaderboard")

# Google Sheet CSV URLs (Direct instant export + CDN fallback + Apps Script Web App)
QUIZ_CSV_URL = "https://docs.google.com/spreadsheets/d/15JMXTiM1AbI0LAh85R26pBo459vDG-G1i59Vqz6a3wg/export?format=csv&gid=0"
FALLBACK_CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C/pub?output=csv"
APPS_SCRIPT_WEBAPP_URL = "https://script.google.com/macros/s/AKfycbysyuvXUbTu65lRIpBHpihOKsELv0-92fQN71SbLqC1laDTiN3PDkJqIOVIjMWxx3z8iQ/exec"

# Default Scoring & Lifecycle Fallbacks
DEFAULT_CORRECT_MARK = 2.0
DEFAULT_WRONG_MARK = 0.33
DEFAULT_TIME = 90
DEFAULT_CYCLE_HOUR = 9  # Updates at 09:00 AM IST

# File to save local fallback leaderboard
LEADERBOARD_FILE = "leaderboard.json"

# Default configuration schema (matches Jarvis Admin Defaults)
DEFAULT_CONFIG = {
    "botEnabled": True,
    "adminId": 2053638316,
    "cycleHour": 9,
    "defaultTimeSeconds": 90,
    "maxQuestions": 10,
    "correctMark": 2.0,
    "wrongMark": 0.33,
    "negativeRatio": 0.66,
    "skipMark": 0.0,
    "stylizeNameWithEmojiSquares": False,
    "welcomeTitleTemplate": "📘 Welcome <b>{name}</b> to Vyasify Daily Quiz",
    "welcomeSubtitle": "This is a daily practice platform for aspirants of 🎯 UPSC | SSC | Regulatory Body Examinations",
    "welcomeTopicTemplate": "<blockquote>⭐ Today's Topic: <b>{topic}</b> ❞</blockquote>",
    "welcomeDurationTemplate": "<blockquote>⏳ Time per Question: <b>{duration} Seconds</b></blockquote>",
    "welcomeBullets": [
        "🔷 Daily 10 questions strictly aligned to UPSC Prelims-oriented topics",
        "✅ Correct Answer: +{correct_marks} Marks",
        "❌ Negative Marking: -{wrong_marks} Marks",
        "⏭ Skip: 0 Marks",
        "📝 Timed questions to build exam temperament",
        "📊 Score, Rank & Percentile for self-benchmarking",
        "📖 Simple explanations for concept clarity"
    ],
    "welcomeCtaText": "👇 Tap below to start today’s quiz",
    "startButtonText": "▶ Start Today's Quiz",
    "helpButtonText": "ℹ How it works",
    "backHomeButtonText": "🔙 Back to Menu",
    "howItWorksTitle": "ℹ️ <b>How Vyasify Quiz Works</b>",
    "howItWorksCycleHeader": "📅 <b>Daily Cycle:</b>",
    "howItWorksCycleBody": "New questions are updated exactly at <b>09:00 AM IST</b>.\n• Before 9 AM: You get Yesterday's quiz.\n• After 9 AM: You get Today's quiz.",
    "howItWorksTimerHeader": "⏱ <b>Timer Rules:</b>",
    "howItWorksTimerBody": "Today, each question has a limit of <b>{duration} seconds</b>.\nIf the time runs out, the question is marked as <b>Skipped</b>.",
    "howItWorksLeaderboardHeader": "🏆 <b>Leaderboard:</b>",
    "howItWorksLeaderboardBody": "Ranks are calculated based on:\n1. Highest Score\n2. Lowest Time Taken\nThe leaderboard resets daily at 09:00 AM.",
    "howItWorksFooter": "Good luck with your preparation! 📝",
    "countdownEnabled": True,
    "countdownDelaySeconds": 0.4,
    "countdownInitText": "⏳ Initializing...",
    "countdownFrames": [
        "Quiz Starts in 3️⃣",
        "Quiz Starts in 3️⃣ .",
        "Quiz Starts in 3️⃣ . .",
        "Quiz Starts in 2️⃣",
        "Quiz Starts in 2️⃣ .",
        "Quiz Starts in 2️⃣ . .",
        "Quiz Starts in 1️⃣",
        "Quiz Starts in 1️⃣ .",
        "Quiz Starts in 1️⃣ . ."
    ],
    "countdownFinalTemplate": "🚀 Goodluck <b>{name}</b>! 🤝",
    "scorecardGeneratingNotice": "🙏 Thank you for attempting today's quiz, <b>{name}</b>!\nGenerating your scorecard...",
    "scorecardHeader": "🏁 <b>Quiz Finished!</b>",
    "scorecardAttemptedLabel": "📝 Attempted:",
    "scorecardCorrectLabel": "✅ Correct:",
    "scorecardWrongLabel": "❌ Wrong:",
    "scorecardSkippedLabel": "⏭ Skipped:",
    "scorecardMarksLabel": "🎯 Marks:",
    "scorecardTimeLabel": "⏱ Time:",
    "scorecardLeaderboardTitle": "🏆 <b>Daily Leaderboard (Top 10)</b>",
    "leaderboardRowTemplate": "{rank}. <b>{name}</b> — {score} | {time}",
    "explanationsHeader": "📖 <b>Explanations</b>",
    "explanationsDivider": "------------------------------------------------"
}

# ================= DYNAMIC CONFIG CACHE =================

_cached_config = None
_cached_config_time = 0

def get_bot_config():
    """
    Fetches dynamic configuration from Vyasify API with a 5-second TTL cache.
    Ensures instant reflection of any changes made in Jarvis Admin Settings
    without requiring a server or bot restart.
    """
    global _cached_config, _cached_config_time
    now = time.time()
    if _cached_config is not None and (now - _cached_config_time) < 5:
        return _cached_config
    
    try:
        res = requests.get(CONFIG_API_URL, timeout=4)
        if res.status_code == 200:
            data = res.json()
            if data.get("success") and "config" in data:
                merged = {**DEFAULT_CONFIG, **data["config"]}
                _cached_config = merged
                _cached_config_time = now
                return _cached_config
    except Exception as e:
        print(f"[Config API] Fetch failed, using cached/fallback: {e}")
    
    if _cached_config is not None:
        return _cached_config
    return DEFAULT_CONFIG

# ================= GLOBAL STATE =================

sessions = {}
leaderboard = {}

# ================= HELPER FUNCTIONS =================

def now_ist():
    """Returns current datetime in IST (UTC + 5:30)."""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def get_quiz_date_key():
    """
    Determines the active quiz date based on the cycle hour from dynamic config.
    - Time < cycleHour: Fetch Yesterday's Quiz
    - Time >= cycleHour: Fetch Today's Quiz
    """
    cfg = get_bot_config()
    cycle_hr = int(cfg.get("cycleHour", DEFAULT_CYCLE_HOUR))
    now = now_ist()
    
    if now.hour < cycle_hr:
        target_date = now.date() - timedelta(days=1)
    else:
        target_date = now.date()
        
    return target_date.isoformat()

def clean_text(text):
    """Robust text cleaner for Telegram HTML parsing."""
    if not text:
        return ""
    text = str(text).replace('\\n', '\n').replace('\r\n', '\n')
    return html.escape(text)

def format_time(seconds):
    """Formats seconds into '1m 41s' style."""
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"

def truncate(text, limit=100):
    if not text:
        return ""
    text = str(text).replace("\\n", " ").strip()
    return text if len(text) <= limit else text[:limit - 3] + "..."

def stylize_name_squared(input_str: str) -> str:
    """
    Transforms characters to enclosed alphanumeric emoji block letters:
    e.g. SURYA -> 🆂🆄🆁🆈🅰
    """
    if not input_str:
        return ""
    special_map = {
        'A': '🅰',
        'B': '🅱',
        'O': '🅾',
        'P': '🅿'
    }
    result = []
    for char in input_str.upper():
        if char in special_map:
            result.append(special_map[char])
        elif 'A' <= char <= 'Z':
            result.append(chr(0x1F170 + (ord(char) - 65)))
        else:
            result.append(char)
    return "".join(result)

def render_template(template_str: str, variables: dict) -> str:
    """Replaces placeholders like {name}, {topic}, {duration}, {correct_marks}, etc."""
    if not template_str:
        return ""
    rendered = str(template_str)
    for k, v in variables.items():
        rendered = rendered.replace(f"{{{k}}}", str(v if v is not None else ""))
    return rendered

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

def get_live_leaderboard(date_key):
    """
    Fetches the live ranked leaderboard for the specified date from Vyasify API.
    Falls back to local leaderboard.json if API is unavailable.
    """
    try:
        res = requests.get(f"{LEADERBOARD_API_URL}?date={date_key}", timeout=4)
        if res.status_code == 200:
            data = res.json()
            if data.get("success") and "leaderboard" in data:
                return data["leaderboard"]
    except Exception as e:
        print(f"[Leaderboard API] Get error, falling back to local: {e}")

    load_leaderboard()
    current_data = leaderboard.get(date_key, {})
    ranked = sorted(
        current_data.values(),
        key=lambda x: (-x["score"], x["time"])
    )
    return ranked

def fetch_questions(date_key=None):
    """
    Fetches questions for a specific date (YYYY-MM-DD).
    Supports multi-tier retrieval: Direct CSV -> Cached CSV -> Apps Script WebApp -> Vyasify API
    """
    cfg = get_bot_config()
    default_t = int(cfg.get("defaultTimeSeconds", DEFAULT_TIME))

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
                            row["_time"] = default_t
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
                clean_q["_time"] = int(time_val) if time_val.isdigit() else default_t
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
                    "topic": "Daily Current Affairs",
                    "question": q.get("question", ""),
                    "option_a": opts[0] if len(opts) > 0 else "",
                    "option_b": opts[1] if len(opts) > 1 else "",
                    "option_c": opts[2] if len(opts) > 2 else "",
                    "option_d": opts[3] if len(opts) > 3 else "",
                    "correct_option": clean_correct,
                    "explanation": q.get("explanation", ""),
                    "source": q.get("sourceArticleTitle", "Vyasify Current Affairs"),
                    "time": default_t,
                    "_time": default_t,
                    "marks": cfg.get("correctMark", DEFAULT_CORRECT_MARK),
                    "negative_ratio": cfg.get("negativeRatio", 0.66)
                }
                valid_questions.append(row_fmt)
            if valid_questions:
                return valid_questions
    except Exception as e:
        print(f"Direct API Fetch Error: {e}")

    return []

# ================= KEYBOARDS =================

def start_keyboard():
    cfg = get_bot_config()
    start_label = cfg.get("startButtonText") or "▶ Start Today's Quiz"
    help_label = cfg.get("helpButtonText") or "ℹ How it works"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(start_label, callback_data="action_start")],
        [InlineKeyboardButton(help_label, callback_data="action_help")]
    ])

def skip_keyboard(idx):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ Skip Question", callback_data=f"skip:{idx}")]
    ])

def back_home_keyboard():
    cfg = get_bot_config()
    back_label = cfg.get("backHomeButtonText") or "🔙 Back to Menu"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(back_label, callback_data="action_home")]
    ])

# ================= HANDLERS =================

def welcome_user(update: Update, context: CallbackContext):
    """Sends the customizable Welcome Message with instant config reflection."""
    cfg = get_bot_config()
    user = update.effective_user
    raw_name = user.first_name if (user and user.first_name) else "ASPIRANT"

    if cfg.get("stylizeNameWithEmojiSquares"):
        disp_name = stylize_name_squared(raw_name)
    else:
        disp_name = raw_name.upper()

    if not cfg.get("botEnabled", True):
        maintenance_msg = "⚠️ <b>Daily Quiz Bot is currently undergoing scheduled maintenance.</b>\nPlease check back shortly!"
        if update.callback_query:
            update.callback_query.edit_message_text(text=maintenance_msg, parse_mode=ParseMode.HTML)
        elif user:
            context.bot.send_message(chat_id=user.id, text=maintenance_msg, parse_mode=ParseMode.HTML)
        return

    # Fetch questions for current cycle
    questions = fetch_questions()
    default_t = int(cfg.get("defaultTimeSeconds", DEFAULT_TIME))
    if questions and "topic" in questions[0] and questions[0]["topic"].strip():
        topic_name = questions[0]["topic"].strip()
        q_duration = questions[0].get("_time", default_t)
    else:
        topic_name = "Daily Current Affairs"
        q_duration = default_t

    corr_m = cfg.get("correctMark", DEFAULT_CORRECT_MARK)
    corr_m_str = str(int(corr_m)) if corr_m == int(corr_m) else str(corr_m)
    wrong_m = cfg.get("wrongMark", DEFAULT_WRONG_MARK)
    wrong_m_str = str(wrong_m)
    neg_ratio = cfg.get("negativeRatio", 0.66)

    template_vars = {
        "name": clean_text(disp_name),
        "topic": clean_text(topic_name),
        "duration": q_duration,
        "correct_marks": corr_m_str,
        "wrong_marks": wrong_m_str,
        "negative_ratio": neg_ratio,
    }

    title = render_template(cfg.get("welcomeTitleTemplate", "📘 Welcome <b>{name}</b> to Vyasify Daily Quiz"), template_vars)
    subtitle = cfg.get("welcomeSubtitle", "")
    topic_box = render_template(cfg.get("welcomeTopicTemplate", "<blockquote>⭐ Today's Topic: <b>{topic}</b> ❞</blockquote>"), template_vars)
    duration_box = render_template(cfg.get("welcomeDurationTemplate", "<blockquote>⏳ Time per Question: <b>{duration} Seconds</b></blockquote>"), template_vars)

    bullet_lines = []
    for b in cfg.get("welcomeBullets", []):
        bullet_lines.append(render_template(b, template_vars))
    bullets_text = "\n".join(bullet_lines)

    cta = cfg.get("welcomeCtaText", "👇 Tap below to start today’s quiz")

    parts = [title]
    if subtitle:
        parts.append(subtitle)
    if topic_box:
        parts.append(topic_box)
    if duration_box:
        parts.append(duration_box)
    if bullets_text:
        parts.append(bullets_text)
    if cta:
        parts.append(cta)

    text = "\n\n".join(parts)

    try:
        if update.callback_query:
            update.callback_query.edit_message_text(
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )
        elif user:
            context.bot.send_message(
                chat_id=user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )
    except:
        if user:
            context.bot.send_message(
                chat_id=user.id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )

def show_how_it_works(update: Update, context: CallbackContext):
    """Displays the How It Works information with dynamic templates."""
    cfg = get_bot_config()
    questions = fetch_questions()
    default_t = int(cfg.get("defaultTimeSeconds", DEFAULT_TIME))
    if questions:
        q_duration = questions[0].get("_time", default_t)
    else:
        q_duration = default_t

    cycle_hr = int(cfg.get("cycleHour", DEFAULT_CYCLE_HOUR))
    cycle_time_str = f"{cycle_hr:02d}:00 AM IST"

    vars = {
        "duration": q_duration,
        "cycle_hour": cycle_hr,
        "cycle_time": cycle_time_str,
    }

    title = cfg.get("howItWorksTitle", "ℹ️ <b>How Vyasify Quiz Works</b>")
    cycle_h = cfg.get("howItWorksCycleHeader", "📅 <b>Daily Cycle:</b>")
    cycle_b = render_template(cfg.get("howItWorksCycleBody", ""), vars)
    timer_h = cfg.get("howItWorksTimerHeader", "⏱ <b>Timer Rules:</b>")
    timer_b = render_template(cfg.get("howItWorksTimerBody", ""), vars)
    lb_h = cfg.get("howItWorksLeaderboardHeader", "🏆 <b>Leaderboard:</b>")
    lb_b = render_template(cfg.get("howItWorksLeaderboardBody", ""), vars)
    footer = cfg.get("howItWorksFooter", "Good luck with your preparation! 📝")

    parts = [
        title,
        f"{cycle_h}\n{cycle_b}",
        f"{timer_h}\n{timer_b}",
        f"{lb_h}\n{lb_b}",
        footer
    ]
    text = "\n\n".join(p for p in parts if p.strip())

    update.callback_query.edit_message_text(
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=back_home_keyboard()
    )

def start_quiz_session(update: Update, context: CallbackContext):
    """Initializes the quiz session with dynamic Build-Up Wave animation & configs."""
    cfg = get_bot_config()
    uid = update.effective_user.id
    raw_name = update.effective_user.first_name if update.effective_user.first_name else "Aspirant"
    
    if cfg.get("stylizeNameWithEmojiSquares"):
        disp_name = stylize_name_squared(raw_name)
    else:
        disp_name = raw_name

    # Check bot enabled
    if not cfg.get("botEnabled", True):
        context.bot.send_message(uid, "⚠️ <b>Daily Quiz Bot is currently paused by admin.</b>", parse_mode=ParseMode.HTML)
        return

    # 1. Cleanup previous session
    if uid in sessions:
        try:
            if sessions[uid].get("poll_msg_id"):
                context.bot.stop_poll(uid, sessions[uid]["poll_msg_id"])
        except: pass
        del sessions[uid]
    
    for job in context.job_queue.get_jobs_by_name(str(uid)):
        job.schedule_removal()

    # 2. Fetch Data & Lock Date Key
    current_date_key = get_quiz_date_key()
    questions = fetch_questions(current_date_key)
    
    if not questions:
        context.bot.send_message(uid, "❌ <b>No quiz available for the current cycle.</b> \n(Next update at 09:00 AM IST).", parse_mode=ParseMode.HTML)
        return

    # Cap max questions if configured
    max_q = int(cfg.get("maxQuestions", 10))
    if max_q and len(questions) > max_q:
        questions = questions[:max_q]

    # 3. Dynamic Build-Up Wave Animation
    if cfg.get("countdownEnabled", True):
        try:
            init_text = cfg.get("countdownInitText", "⏳ Initializing...")
            msg = context.bot.send_message(uid, init_text, parse_mode=ParseMode.HTML)
            delay = float(cfg.get("countdownDelaySeconds", 0.4))
            frames = cfg.get("countdownFrames", [])
            for frame in frames:
                time.sleep(delay)
                msg.edit_text(frame, parse_mode=ParseMode.HTML)
            
            # Final Greeting
            final_template = cfg.get("countdownFinalTemplate", "🚀 Goodluck <b>{name}</b>! 🤝")
            final_text = render_template(final_template, {"name": clean_text(disp_name)})
            time.sleep(delay)
            msg.edit_text(final_text, parse_mode=ParseMode.HTML)
            time.sleep(delay)
        except Exception as e:
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
        "name": update.effective_user.full_name or raw_name,
        "disp_name": disp_name,
        "explanations": [],
        "correct_mark": float(cfg.get("correctMark", DEFAULT_CORRECT_MARK)),
        "wrong_mark": float(cfg.get("wrongMark", DEFAULT_WRONG_MARK)),
        "divider": cfg.get("explanationsDivider", "------------------------------------------------"),
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
    divider = s.get("divider", "------------------------------------------------")
    
    s["explanations"].append(
        f"<b>Q{s['index']+1}.</b> {clean_text(q['question'])}\n\n{status_emoji}\n\n📘 <b>Explanation:</b>\n{clean_text(q.get('explanation', 'N/A'))}\n\n{divider}"
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
    correct_m = s.get("correct_mark", DEFAULT_CORRECT_MARK)
    wrong_m = s.get("wrong_mark", DEFAULT_WRONG_MARK)
    divider = s.get("divider", "------------------------------------------------")

    if selected == correct_idx:
        s["marks"] += correct_m
        s["correct"] += 1
        status_emoji = "✅ <b>Correct</b>"
    else:
        s["marks"] -= wrong_m
        s["wrong"] += 1
        status_emoji = "❌ <b>Wrong</b>"

    s["explanations"].append(
        f"<b>Q{s['index']+1}.</b> {clean_text(q['question'])}\n\n{status_emoji}\n\n📘 <b>Explanation:</b>\n{clean_text(q.get('explanation', 'N/A'))}\n\n{divider}"
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
    cfg = get_bot_config()

    duration = time.time() - s["start_time"]
    final_score = round(s["marks"], 2)
    time_str = format_time(duration)
    user_display = s.get("disp_name", s["name"].upper())

    # 1. Local fallback save
    leaderboard.setdefault(day_key, {})[str(uid)] = {
        "name": user_display,
        "score": final_score,
        "time": int(duration),
        "platform": "telegram"
    }
    save_leaderboard()

    # 2. Sync to Vyasify Web API
    try:
        requests.post(
            LEADERBOARD_API_URL,
            json={
                "date": day_key,
                "userId": str(uid),
                "name": user_display,
                "score": final_score,
                "time": int(duration),
                "platform": "telegram"
            },
            timeout=4
        )
    except Exception as e:
        print(f"[Leaderboard Sync] API post error: {e}")

    # Notice
    gen_notice = render_template(
        cfg.get("scorecardGeneratingNotice", "🙏 Thank you for attempting today's quiz, <b>{name}</b>!\nGenerating your scorecard..."),
        {"name": clean_text(user_display)}
    )
    context.bot.send_message(
        uid,
        gen_notice,
        parse_mode=ParseMode.HTML
    )
    time.sleep(1)

    attempted_count = s["correct"] + s["wrong"]
    
    header = cfg.get("scorecardHeader", "🏁 <b>Quiz Finished!</b>")
    lbl_att = cfg.get("scorecardAttemptedLabel", "📝 Attempted:")
    lbl_cor = cfg.get("scorecardCorrectLabel", "✅ Correct:")
    lbl_wro = cfg.get("scorecardWrongLabel", "❌ Wrong:")
    lbl_skp = cfg.get("scorecardSkippedLabel", "⏭ Skipped:")
    lbl_mrk = cfg.get("scorecardMarksLabel", "🎯 Marks:")
    lbl_tim = cfg.get("scorecardTimeLabel", "⏱ Time:")
    lb_title = cfg.get("scorecardLeaderboardTitle", "🏆 <b>Daily Leaderboard (Top 10)</b>")
    row_tmpl = cfg.get("leaderboardRowTemplate", "{rank}. <b>{name}</b> — {score} | {time}")

    res_text = (
        f"{header}\n\n"
        f"{lbl_att} {attempted_count}/{s['total_q']}\n"
        f"{lbl_cor} {s['correct']}\n"
        f"{lbl_wro} {s['wrong']}\n"
        f"{lbl_skp} {s['skipped']}\n"
        f"{lbl_mrk} {final_score}\n"
        f"{lbl_tim} {time_str}\n\n"
        f"{lb_title}\n\n"
    )

    ranked = get_live_leaderboard(day_key)[:10]

    for i, r in enumerate(ranked, 1):
        r_time = format_time(r['time'])
        platform = r.get("platform", "telegram")
        platform_icon = "🌐" if platform == "web" else "📱"
        platform_badge = "🌐 Web" if platform == "web" else "📱 Bot"
        row_str = render_template(row_tmpl, {
            "rank": i,
            "name": f"{platform_icon} {clean_text(r['name'])}",
            "score": r['score'],
            "time": r_time,
            "platform": platform_badge,
            "platform_badge": platform_badge,
            "platform_icon": platform_icon
        })
        res_text += f"{row_str}\n"

    context.bot.send_message(uid, res_text, parse_mode=ParseMode.HTML)

    exp_header = cfg.get("explanationsHeader", "📖 <b>Explanations</b>")
    buffer = f"{exp_header}\n\n"
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
    cfg = get_bot_config()
    
    ranked = get_live_leaderboard(day_key)[:10]
    
    if not ranked:
        update.message.reply_text("📉 No records yet for today's quiz cycle (since 9 AM). Be the first!", parse_mode=ParseMode.HTML)
        return

    lb_title = cfg.get("scorecardLeaderboardTitle", "🏆 <b>Daily Leaderboard (Top 10)</b>")
    row_tmpl = cfg.get("leaderboardRowTemplate", "{rank}. <b>{name}</b> — {score} | {time}")

    text = f"{lb_title} ({day_key})\n\n"
    for i, r in enumerate(ranked, 1):
        r_time = format_time(r['time'])
        platform = r.get("platform", "telegram")
        platform_icon = "🌐" if platform == "web" else "📱"
        platform_badge = "🌐 Web" if platform == "web" else "📱 Bot"
        row_str = render_template(row_tmpl, {
            "rank": i,
            "name": f"{platform_icon} {clean_text(r['name'])}",
            "score": r['score'],
            "time": r_time,
            "platform": platform_badge,
            "platform_badge": platform_badge,
            "platform_icon": platform_icon
        })
        text += f"{row_str}\n"
    
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

    print("✅ Vyasify Dynamic Bot is Online...")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
