import os
import csv
import time
import json
import html
import re
import requests
from io import StringIO
from datetime import datetime, timedelta, timezone

try:
    from telegram import (
        Update,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        Poll,
        ParseMode,
        WebAppInfo
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
except ImportError:
    # Compatibility fallback for python-telegram-bot v20+
    from telegram import (
        Update,
        InlineKeyboardButton,
        InlineKeyboardMarkup,
        Poll,
        WebAppInfo,
    )
    from telegram.constants import ParseMode
    from telegram.ext import (
        CommandHandler,
        CallbackQueryHandler,
        PollAnswerHandler,
        MessageHandler,
        filters as Filters,
        CallbackContext,
    )
    try:
        from telegram.ext import Updater
    except ImportError:
        Updater = None

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
USERS_FILE = "users.json"
users_db = {}

def load_users():
    global users_db
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, "r", encoding="utf-8") as f:
                users_db = json.load(f)
        except Exception as e:
            print(f"[Users DB] Error loading users: {e}")
            users_db = {}
    
    # Auto-seed from leaderboard so past quiz participants are preserved
    try:
        for day, participants in leaderboard.items():
            if isinstance(participants, dict):
                for uid_str, p in participants.items():
                    if uid_str not in users_db and not str(uid_str).startswith("web_"):
                        users_db[str(uid_str)] = {
                            "id": int(uid_str) if str(uid_str).isdigit() else uid_str,
                            "name": p.get("name", "Aspirant"),
                            "source": "leaderboard"
                        }
    except Exception as e:
        print(f"[Users DB] Error seeding from leaderboard: {e}")

    # Ensure Admin ID is always registered
    if str(ADMIN_ID) not in users_db:
        users_db[str(ADMIN_ID)] = {
            "id": ADMIN_ID,
            "name": "Surya Vyas",
            "source": "admin"
        }
    save_users()

def save_users():
    try:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            json.dump(users_db, f, indent=2)
    except Exception as e:
        print(f"[Users DB] Error saving users: {e}")

def record_user(user):
    if not user or not getattr(user, "id", None):
        return
    uid_str = str(user.id)
    first_name = user.first_name or ""
    username = user.username or ""
    if uid_str not in users_db:
        users_db[uid_str] = {
            "id": user.id,
            "name": first_name,
            "username": username,
            "joined_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        }
        save_users()
    else:
        changed = False
        if first_name and users_db[uid_str].get("name") != first_name:
            users_db[uid_str]["name"] = first_name
            changed = True
        if username and users_db[uid_str].get("username") != username:
            users_db[uid_str]["username"] = username
            changed = True
        if changed:
            save_users()

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

def normalize_newlines(text):
    """Converts escaped newlines (\n, \r\n) and literal slash-n (/n) into real newline characters."""
    if not text:
        return ""
    s = str(text)
    s = s.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r'(?<=[^\s/])\s*/n\s*', '\n', s)
    s = s.replace(" /n ", "\n").replace("/n ", "\n").replace(" /n", "\n")
    return s.strip()

def clean_text(text):
    """Robust text cleaner for Telegram HTML parsing."""
    if not text:
        return ""
    text = normalize_newlines(text)
    return html.escape(text)

def format_time(seconds):
    """Formats seconds into '1m 41s' style."""
    m, s = divmod(int(seconds), 60)
    return f"{m}m {s}s"

def truncate(text, limit=100):
    if not text:
        return ""
    text = normalize_newlines(text)
    text = re.sub(r'\s+', ' ', text).strip()
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

def parse_flexible_date(date_str):
    """
    Parses dates in any common format: DD-MM-YYYY, YYYY-MM-DD, DD/MM/YYYY, etc.
    Strips leading apostrophes and handles single digit days/months.
    """
    if not date_str:
        return None
    cleaned = str(date_str).strip().lstrip("'").strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%y", "%d/%m/%y", "%d.%m.%Y"):
        try:
            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            pass
    m = re.match(r"^(\d{1,4})[-/.](\d{1,2})[-/.](\d{1,4})$", cleaned)
    if m:
        p1, p2, p3 = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if p1 > 1000:
            return datetime(p1, p2, p3).date()
        elif p3 > 1000:
            return datetime(p3, p2, p1).date()
    return None

def extract_correct_option_id(correct_str):
    """
    Safely extracts option ID (0=A, 1=B, 2=C, 3=D) from various formats:
    'A', 'Option B', '(C)', 'D.', etc.
    """
    if not correct_str:
        return 0
    s = str(correct_str).strip().upper()
    match = re.search(r'[A-D]', s)
    if match:
        return ord(match.group(0)) - 65
    return 0

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
    Supports multi-tier retrieval:
    1. Direct CSV (gid=0 - daily_questions)
    2. Dynamic Active Month Tab CSV via gviz (e.g. November 2026)
    3. Cached Published CSV Fallback
    4. Apps Script Web App Endpoint
    5. Vyasify Web API Fallback
    With smart candidate date fallbacks (target_date -> today_date -> latest available date).
    """
    cfg = get_bot_config()
    default_t = int(cfg.get("defaultTimeSeconds", DEFAULT_TIME))

    # Candidate dates to evaluate in order of preference
    candidate_dates = []
    if date_key:
        try:
            candidate_dates.append(datetime.strptime(date_key, "%Y-%m-%d").date())
        except Exception:
            pass

    cycle_date_str = get_quiz_date_key()
    try:
        cycle_date = datetime.strptime(cycle_date_str, "%Y-%m-%d").date()
        if cycle_date not in candidate_dates:
            candidate_dates.append(cycle_date)
    except Exception:
        pass

    today_ist = now_ist().date()
    if today_ist not in candidate_dates:
        candidate_dates.append(today_ist)

    # Primary target for Month tab generation
    primary_date = candidate_dates[0] if candidate_dates else today_ist
    month_name = primary_date.strftime("%B %Y")
    month_gviz_url = f"https://docs.google.com/spreadsheets/d/15JMXTiM1AbI0LAh85R26pBo459vDG-G1i59Vqz6a3wg/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(month_name)}"

    urls_to_try = [
        QUIZ_CSV_URL,
        month_gviz_url,
        FALLBACK_CSV_URL,
    ]

    # Tier 1, 2, 3: Direct CSVs
    all_parsed_rows = []
    for url in urls_to_try:
        try:
            r = requests.get(url, timeout=12)
            if r.status_code != 200:
                continue
            content_decoded = r.content.decode("utf-8-sig")
            csv_rows = list(csv.DictReader(StringIO(content_decoded)))
            if not csv_rows:
                continue

            for row in csv_rows:
                row = {k.strip(): v for k, v in row.items()}
                d_obj = parse_flexible_date(row.get("date"))
                if not d_obj:
                    continue
                time_val = str(row.get("time", "")).strip()
                if time_val and time_val.isdigit():
                    row["_time"] = max(5, min(600, int(time_val)))
                else:
                    row["_time"] = default_t
                row["_date_obj"] = d_obj
                row["question"] = normalize_newlines(row.get("question", ""))
                row["explanation"] = normalize_newlines(row.get("explanation", ""))
                row["option_a"] = row.get("option_a", "").strip() or "Option A"
                row["option_b"] = row.get("option_b", "").strip() or "Option B"
                row["option_c"] = row.get("option_c", "").strip() or "Option C"
                row["option_d"] = row.get("option_d", "").strip() or "Option D"
                all_parsed_rows.append(row)

            # Check candidate dates against all_parsed_rows
            for c_date in candidate_dates:
                matches = [r for r in all_parsed_rows if r.get("_date_obj") == c_date]
                if matches:
                    return matches
        except Exception as e:
            print(f"CSV Fetch Error ({url}): {e}")

    # Tier 4: Google Apps Script Web App Endpoint
    for c_date in candidate_dates:
        try:
            sheet_date = c_date.strftime("%d-%m-%Y")
            res = requests.get(f"{APPS_SCRIPT_WEBAPP_URL}?date={sheet_date}", timeout=10)
            if res.status_code == 200:
                data = res.json()
                qs = data.get("questions", [])
                valid_questions = []
                for q in qs:
                    clean_q = {k.strip(): v for k, v in q.items()}
                    time_val = str(clean_q.get("time", "")).strip()
                    clean_q["_time"] = max(5, min(600, int(time_val))) if time_val.isdigit() else default_t
                    clean_q["option_a"] = clean_q.get("option_a", "").strip() or "Option A"
                    clean_q["option_b"] = clean_q.get("option_b", "").strip() or "Option B"
                    clean_q["option_c"] = clean_q.get("option_c", "").strip() or "Option C"
                    clean_q["option_d"] = clean_q.get("option_d", "").strip() or "Option D"
                    clean_q["_date_obj"] = c_date
                    valid_questions.append(clean_q)
                if valid_questions:
                    return valid_questions
        except Exception as e:
            print(f"Apps Script WebApp Fetch Error: {e}")

    # Tier 5: Direct Vyasify Web API Fallback
    for c_date in candidate_dates:
        try:
            iso_str = c_date.isoformat()
            api_res = requests.get(f"https://vyasify.in/api/current-affairs/daily-quiz?date={iso_str}", timeout=10)
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
                        "date": c_date.strftime("%d-%m-%Y"),
                        "topic": "Daily Current Affairs",
                        "question": q.get("question", ""),
                        "option_a": (opts[0] if len(opts) > 0 else "").strip() or "Option A",
                        "option_b": (opts[1] if len(opts) > 1 else "").strip() or "Option B",
                        "option_c": (opts[2] if len(opts) > 2 else "").strip() or "Option C",
                        "option_d": (opts[3] if len(opts) > 3 else "").strip() or "Option D",
                        "correct_option": clean_correct,
                        "explanation": q.get("explanation", ""),
                        "source": q.get("sourceArticleTitle", "Vyasify Current Affairs"),
                        "time": default_t,
                        "_time": default_t,
                        "_date_obj": c_date,
                        "marks": cfg.get("correctMark", DEFAULT_CORRECT_MARK),
                        "negative_ratio": cfg.get("negativeRatio", 0.66)
                    }
                    valid_questions.append(row_fmt)
                if valid_questions:
                    return valid_questions
        except Exception as e:
            print(f"Direct API Fetch Error: {e}")

    # Final safety: If all candidate dates had 0 questions, check if ANY date in all_parsed_rows has questions
    if all_parsed_rows:
        available_dates = sorted(list(set(r["_date_obj"] for r in all_parsed_rows if "_date_obj" in r)))
        if available_dates:
            latest_d = available_dates[-1]
            latest_matches = [r for r in all_parsed_rows if r.get("_date_obj") == latest_d]
            if latest_matches:
                print(f"[fetch_questions] Fallback to latest available date in sheet: {latest_d} ({len(latest_matches)} questions)")
                return latest_matches

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
    """Sends the customizable Welcome Message with instant config reflection.
    Also handles deep-links: ?start=exp_{date}_{q_index} to serve group explanations."""
    # Check for group explanation deep-link: ?start=exp_{date}_{q_index}
    if context and context.args and len(context.args) > 0 and str(context.args[0]).startswith("exp_"):
        serve_group_explanation_dm(update, context)
        return

    # Check for group subscription deep-link: ?start=grp_sub
    if context and context.args and len(context.args) > 0 and str(context.args[0]) == "grp_sub":
        user = update.effective_user
        if user:
            record_user(user)
        sub_msg = (
            "✅ <b>Subscribed to Vyasify Daily Quiz!</b>\n\n"
            "You are now registered for:\n"
            "• 🔔 5:00 PM daily group quiz alerts\n"
            "• 🏆 Daily leaderboard ranking notifications\n"
            "• 💡 UPSC Current Affairs answer keys & notes\n\n"
            "Ready to test yourself? Tap below to attempt today's timed quiz!"
        )
        context.bot.send_message(
            chat_id=user.id,
            text=sub_msg,
            parse_mode=ParseMode.HTML,
            reply_markup=start_keyboard()
        )
        return

    cfg = get_bot_config()
    user = update.effective_user
    if user:
        record_user(user)
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

    # Lock date key to the actual questions date if available
    if questions and "_date_obj" in questions[0]:
        current_date_key = questions[0]["_date_obj"].isoformat()
    elif questions and "date" in questions[0]:
        actual_d = parse_flexible_date(questions[0]["date"])
        if actual_d:
            current_date_key = actual_d.isoformat()

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
        truncate(q.get("option_a", "")) or "Option A",
        truncate(q.get("option_b", "")) or "Option B",
        truncate(q.get("option_c", "")) or "Option C",
        truncate(q.get("option_d", "")) or "Option D"
    ]
    correct_id = extract_correct_option_id(q.get("correct_option"))
    poll_duration = max(5, min(600, int(q.get("_time", 90))))
    
    try:
        poll = context.bot.send_poll(
            uid,
            "Select the correct option:",
            options,
            type=Poll.QUIZ,
            correct_option_id=correct_id,
            open_period=poll_duration,
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

    # ── Check if this is a group quiz poll answer ──
    if answer.poll_id in group_poll_registry:
        try:
            record_user(answer.user)
            selected_idx = answer.option_ids[0] if answer.option_ids else -1
            store_group_answer(uid, answer.poll_id, selected_idx)
        except Exception as e:
            print(f"[GroupQuiz] Error processing group poll answer: {e}")
        return

    s = sessions.get(uid)

    if not s or s["locked"] or answer.poll_id != s["poll_id"]:
        return

    s["locked"] = True
    
    for job in context.job_queue.get_jobs_by_name(str(uid)):
        job.schedule_removal()

    q = s["questions"][s["index"]]
    correct_idx = extract_correct_option_id(q.get("correct_option"))
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
    if update.effective_user:
        record_user(update.effective_user)
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

# ================= BROADCAST HANDLERS =================

def broadcast_command_handler(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or user.id != ADMIN_ID:
        if update.message:
            update.message.reply_text("⛔️ <b>Access Denied:</b> This command is restricted to the bot administrator.", parse_mode=ParseMode.HTML)
        return

    msg = update.message
    if not msg:
        return

    # Check for reply to another message (copy message)
    reply_to = msg.reply_to_message
    broadcast_text = ""
    photo_file_id = None
    caption_text = None
    is_reply_copy = False

    if reply_to:
        is_reply_copy = True
    elif msg.photo:
        photo_file_id = msg.photo[-1].file_id
        raw_caption = msg.caption or ""
        caption_text = re.sub(r"^/broadcast\s*", "", raw_caption, flags=re.IGNORECASE).strip()
    else:
        raw_text = msg.text or ""
        broadcast_text = re.sub(r"^/broadcast\s*", "", raw_text, flags=re.IGNORECASE).strip()

    if not is_reply_copy and not photo_file_id and not broadcast_text:
        help_text = (
            "📢 <b>Broadcast Console (Admin Only)</b>\n\n"
            "You can broadcast to all users in 3 ways:\n\n"
            "1️⃣ <b>Text Broadcast:</b>\n"
            "<code>/broadcast Your message here</code>\n\n"
            "2️⃣ <b>Photo with Caption:</b>\n"
            "Send any photo with caption: <code>/broadcast Your caption here</code>\n\n"
            "3️⃣ <b>Reply Broadcast (Any media):</b>\n"
            "Reply to any text, photo, audio, or document with <code>/broadcast</code> to replicate it to all users.\n\n"
            f"👥 <b>Current Registered Users:</b> {len(users_db)}"
        )
        msg.reply_text(help_text, parse_mode=ParseMode.HTML)
        return

    targets = [int(uid) for uid in users_db.keys() if str(uid).isdigit()]
    if not targets:
        msg.reply_text("⚠️ <b>No users registered</b> to broadcast to.", parse_mode=ParseMode.HTML)
        return

    status_msg = msg.reply_text(
        f"🚀 <b>Broadcast Initiated!</b>\nTargeting <b>{len(targets)}</b> aspirants...\nPlease wait.",
        parse_mode=ParseMode.HTML
    )

    sent = 0
    blocked = 0
    failed = 0

    for uid in targets:
        try:
            if is_reply_copy:
                context.bot.copy_message(
                    chat_id=uid,
                    from_chat_id=reply_to.chat_id,
                    message_id=reply_to.message_id
                )
            elif photo_file_id:
                context.bot.send_photo(
                    chat_id=uid,
                    photo=photo_file_id,
                    caption=caption_text if caption_text else None,
                    parse_mode=ParseMode.HTML if caption_text else None
                )
            else:
                context.bot.send_message(
                    chat_id=uid,
                    text=broadcast_text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=False
                )
            sent += 1
            time.sleep(0.04)  # ~25 msgs/sec
        except Exception as e:
            err_msg = str(e).lower()
            if "blocked" in err_msg or "deactivated" in err_msg or "chat not found" in err_msg:
                blocked += 1
            else:
                failed += 1
            print(f"[Broadcast] Failed to send to {uid}: {e}")

    summary = (
        f"✅ <b>Broadcast Completed!</b>\n\n"
        f"👥 <b>Total Targets:</b> {len(targets)}\n"
        f"📨 <b>Delivered:</b> {sent}\n"
        f"🚫 <b>Blocked/Deactivated:</b> {blocked}\n"
        f"⚠️ <b>Failed:</b> {failed}"
    )
    msg.reply_text(summary, parse_mode=ParseMode.HTML)

def users_stats_handler(update: Update, context: CallbackContext):
    user = update.effective_user
    if not user or user.id != ADMIN_ID:
        return
    update.message.reply_text(
        f"👥 <b>Vyasify Quiz Bot Audience</b>\n\nTotal Registered Users: <b>{len(users_db)}</b>",
        parse_mode=ParseMode.HTML
    )

def photo_broadcast_handler(update: Update, context: CallbackContext):
    msg = update.message
    if not msg or not msg.photo:
        return
    caption = msg.caption or ""
    if caption.strip().lower().startswith("/broadcast"):
        broadcast_command_handler(update, context)

# ================= GROUP QUIZ ENGINE =================
# Persistent tracking: which dates have already been posted to group
GROUP_QUIZ_POSTED_FILE = "group_quiz_posted.json"
group_quiz_posted = {}  # {date_key: True}

# Maps group poll_id -> question metadata for group explanation links
# { poll_id: { "date_key": str, "q_index": int, "q": dict } }
group_poll_registry = {}

GROUP_ANSWERS_FILE = "group_answers.json"
group_answers = {}  # uid_str -> { poll_id: { selected, date_key, q_index, updated_at } }

def load_group_answers():
    global group_answers
    if os.path.exists(GROUP_ANSWERS_FILE):
        try:
            with open(GROUP_ANSWERS_FILE, "r", encoding="utf-8") as f:
                group_answers = json.load(f)
        except Exception as e:
            print(f"[GroupAnswers] Load error: {e}")
            group_answers = {}

def save_group_answers():
    try:
        with open(GROUP_ANSWERS_FILE, "w", encoding="utf-8") as f:
            json.dump(group_answers, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[GroupAnswers] Save error: {e}")

def sync_group_answer_to_api(uid_str, date_key, q_index, poll_id, selected_idx):
    """Syncs group poll answer asynchronously to Vyasify Web API."""
    def _do_sync():
        api_url = "https://vyasify.in/api/telegram-bot/group-answers"
        payload = {
            "userId": uid_str,
            "dateKey": date_key,
            "qIndex": q_index,
            "pollId": poll_id,
            "selected": selected_idx,
        }
        try:
            headers = {"Content-Type": "application/json"}
            if BOT_TOKEN:
                headers["x-bot-secret"] = BOT_TOKEN
            requests.post(api_url, json=payload, headers=headers, timeout=5)
        except Exception as err:
            print(f"[GroupAnswers] API sync warning ({poll_id}): {err}")

    import threading
    t = threading.Thread(target=_do_sync, daemon=True)
    t.start()

def store_group_answer(uid, poll_id, selected_idx):
    uid_str = str(uid)
    if uid_str not in group_answers:
        group_answers[uid_str] = {}

    reg = group_poll_registry.get(poll_id, {})
    date_key = reg.get("date_key", "")
    q_index = reg.get("q_index", 0)

    group_answers[uid_str][poll_id] = {
        "selected": selected_idx,
        "date_key": date_key,
        "q_index": q_index,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    save_group_answers()

    if date_key:
        sync_group_answer_to_api(uid_str, date_key, q_index, poll_id, selected_idx)

def load_group_quiz_posted():
    global group_quiz_posted
    if os.path.exists(GROUP_QUIZ_POSTED_FILE):
        try:
            with open(GROUP_QUIZ_POSTED_FILE, "r", encoding="utf-8") as f:
                group_quiz_posted = json.load(f)
        except Exception:
            group_quiz_posted = {}

def save_group_quiz_posted():
    try:
        with open(GROUP_QUIZ_POSTED_FILE, "w", encoding="utf-8") as f:
            json.dump(group_quiz_posted, f, indent=2)
    except Exception as e:
        print(f"[GroupQuiz] Error saving posted log: {e}")


CHANNEL_TARGET_FILE = "channel_target.json"

def get_saved_channel_id():
    """Returns saved channel target ID or @username from file or environment."""
    env_target = os.environ.get("GROUP_CHAT_ID")
    if env_target and env_target.strip():
        return env_target.strip()
    if os.path.exists(CHANNEL_TARGET_FILE):
        try:
            with open(CHANNEL_TARGET_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                val = data.get("channel_id")
                if val:
                    return str(val).strip()
        except Exception:
            pass
    return None

def save_channel_target(channel_id):
    """Persists channel ID or username so bot knows where to post quizzes automatically."""
    try:
        with open(CHANNEL_TARGET_FILE, "w", encoding="utf-8") as f:
            json.dump({"channel_id": str(channel_id).strip(), "updated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        print(f"[ChannelTarget] Saved active target channel: {channel_id}")
    except Exception as e:
        print(f"[ChannelTarget] Save error: {e}")

def is_group_admin(bot, chat_id, user_id, sender_chat_id=None):
    """Returns True if user_id is a Telegram group admin/creator, anonymous channel admin, OR bot admin."""
    if user_id == ADMIN_ID:
        return True
    if sender_chat_id and sender_chat_id == chat_id:
        return True
    try:
        member = bot.get_chat_member(chat_id, user_id)
        return member.status in ("creator", "administrator")
    except Exception as e:
        print(f"[is_group_admin] Check error for user {user_id} in chat {chat_id}: {e}")
        return False


def _make_group_explanation_buttons(date_key, q_index):
    """
    Creates inline keyboard for group quiz questions:
    - Button 1: Opens Vyasify Mini App explanation (using standard URL for 100% group compatibility)
    - Button 2: Link to Vyasify current-affairs page
    """
    tg_app_url = f"https://vyasify.in/tg-explain?date={date_key}&q={q_index}"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "💡 View Full Explanation",
                url=tg_app_url
            )
        ],
        [
            InlineKeyboardButton(
                "📖 Read Today's Editorial on Vyasify",
                url="https://vyasify.in/current-affairs"
            )
        ]
    ])


def post_group_quiz(bot, chat_id, questions, date_key, posting_msg_id=None):
    """
    Posts all quiz questions to a channel or group chat as open-ended (no timer) quiz polls.
    Handles Telegram channel anonymity requirements automatically (is_anonymous=True).
    Each poll includes explanation buttons and native quiz lightbulb tooltips.
    """
    cfg = get_bot_config()
    max_q = int(cfg.get("maxQuestions", 10))
    if len(questions) > max_q:
        questions = questions[:max_q]

    # Check if target is a channel (channels STRICTLY require is_anonymous=True)
    is_channel = False
    try:
        target_info = bot.get_chat(chat_id)
        if target_info and target_info.type == "channel":
            is_channel = True
    except Exception as c_err:
        print(f"[post_group_quiz] Chat info lookup: {c_err}")
        if isinstance(chat_id, str) and chat_id.startswith("@"):
            is_channel = True

    # In channels, polls MUST be anonymous (is_anonymous=True)
    poll_is_anon = True if is_channel else True

    # Announcement header
    topic = questions[0].get("topic", "Daily Current Affairs") if questions else "Daily Current Affairs"
    header_text = (
        f"🎯 <b>Vyasify Daily Quiz — {date_key}</b>\n\n"
        f"📌 <b>Topic:</b> {html.escape(topic)}\n\n"
        f"📋 <b>{len(questions)} questions</b> • Attempt anytime\n"
        f"💡 Tap <i>View Full Explanation</i> under each question for detailed UPSC analysis\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    try:
        bot.send_message(chat_id=chat_id, text=header_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        print(f"[GroupQuiz] Header post failed: {e}")

    for idx, q in enumerate(questions):
        options = [
            truncate(q.get("option_a", ""), 100) or "Option A",
            truncate(q.get("option_b", ""), 100) or "Option B",
            truncate(q.get("option_c", ""), 100) or "Option C",
            truncate(q.get("option_d", ""), 100) or "Option D",
        ]
        correct_id = extract_correct_option_id(q.get("correct_option"))

        # Telegram quiz explanation limit: 200 chars — use as short hook only
        raw_exp = q.get("explanation", "") or ""
        short_exp = raw_exp[:160].strip()
        if len(raw_exp) > 160:
            short_exp += "... Visit vyasify.in/current-affairs for full analysis."
        else:
            short_exp = (short_exp + " | For detailed UPSC notes visit vyasify.in/current-affairs").strip()
        short_exp = short_exp[:200]  # Hard cap

        # Clean options: ensure distinct, non-empty, and max 100 chars
        seen_opts = set()
        clean_options = []
        for opt_idx, opt in enumerate(options):
            opt_str = opt.strip() if opt else f"Option {chr(65+opt_idx)}"
            if opt_str in seen_opts:
                opt_str = f"{opt_str} ({chr(65+opt_idx)})"
            seen_opts.add(opt_str)
            clean_options.append(opt_str[:100])

        if correct_id < 0 or correct_id >= len(clean_options):
            correct_id = 0

        clean_q = normalize_newlines(q.get('question', ''))

        # Post full Question Statement as formatted HTML message
        # This completely solves character limit truncation and renders all statements & line breaks perfectly!
        question_post_text = f"📝 <b>QUESTION {idx+1}</b>\n\n{html.escape(clean_q)}"
        try:
            bot.send_message(
                chat_id=chat_id,
                text=question_post_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True
            )
            time.sleep(0.3)
        except Exception as q_err:
            print(f"[GroupQuiz] Question statement post error ({idx+1}): {q_err}")

        # Poll question: compact voting prompt
        poll_question = f"📊 Q{idx+1}. Select your answer:"

        poll_msg = None
        # Attempt 1: send with explanation buttons and anonymous mode
        try:
            poll_msg = bot.send_poll(
                chat_id=chat_id,
                question=poll_question,
                options=clean_options,
                type=Poll.QUIZ,
                correct_option_id=correct_id,
                explanation=short_exp if short_exp else None,
                explanation_parse_mode=None,
                is_anonymous=poll_is_anon,
                reply_markup=_make_group_explanation_buttons(date_key, idx)
            )
        except Exception as err1:
            print(f"[GroupQuiz] send_poll attempt 1 error for Q{idx+1}: {err1}. Retrying with is_anonymous=True...")
            try:
                poll_msg = bot.send_poll(
                    chat_id=chat_id,
                    question=poll_question,
                    options=clean_options,
                    type=Poll.QUIZ,
                    correct_option_id=correct_id,
                    explanation=short_exp if short_exp else None,
                    explanation_parse_mode=None,
                    is_anonymous=True,
                    reply_markup=_make_group_explanation_buttons(date_key, idx)
                )
            except Exception as err2:
                print(f"[GroupQuiz] send_poll attempt 2 error for Q{idx+1}: {err2}. Retrying without markup...")
                try:
                    poll_msg = bot.send_poll(
                        chat_id=chat_id,
                        question=poll_question,
                        options=clean_options,
                        type=Poll.QUIZ,
                        correct_option_id=correct_id,
                        explanation=short_exp if short_exp else None,
                        explanation_parse_mode=None,
                        is_anonymous=True
                    )
                except Exception as direct_err:
                    print(f"[GroupQuiz] Fatal: send_poll failed completely for Q{idx+1}: {direct_err}")
                    try:
                        bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"⚠️ <b>Poll Error for Q{idx+1}:</b> {html.escape(str(direct_err))}\n\n"
                                "Please ensure @vyasify_quiz_bot is an <b>Administrator</b> in this channel with permission to post messages and polls."
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                    continue

        if poll_msg and poll_msg.poll:
            group_poll_registry[poll_msg.poll.id] = {
                "date_key": date_key,
                "q_index": idx,
                "q": q,
            }
        time.sleep(0.8)

    # Footer
    footer_text = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ All {len(questions)} questions posted!\n"
        f"📖 <a href='https://vyasify.in/current-affairs'>Read Today's Full Editorial on Vyasify</a>\n"
        f"💬 Tap <b>Leave a comment</b> below to discuss your answers!"
    )
    try:
        bot.send_message(
            chat_id=chat_id,
            text=footer_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )
    except Exception as e:
        print(f"[GroupQuiz] Footer post failed: {e}")

    # Mark date as posted so auto-scheduler doesn't duplicate
    group_quiz_posted[date_key] = True
    save_group_quiz_posted()
    print(f"[GroupQuiz] Posted {len(questions)} questions to channel/group {chat_id} for date {date_key}")


def channel_post_handler(update: Update, context: CallbackContext):
    """
    Handles messages posted in channels where the bot is an administrator.
    When /quiz (or /quiz@vyasify_quiz_bot) is posted in the channel:
    1. Intercepts the post and saves the channel ID/username as active target.
    2. Deletes the trigger message so the channel feed remains clean.
    3. Posts today's daily quiz polls directly into the channel!
    """
    post = update.channel_post
    if not post:
        return

    text = (post.text or post.caption or "").strip()
    if not text:
        return

    first_word = text.split()[0].lower()
    if first_word in ("/quiz", "/quiz@vyasify_quiz_bot"):
        chat_id = post.chat.id
        print(f"[ChannelQuiz] /quiz triggered in channel: {post.chat.title} ({chat_id})")

        # Save this channel ID as active target
        save_channel_target(chat_id)

        # Delete trigger post if bot has delete permission
        try:
            post.delete()
        except Exception as del_err:
            print(f"[ChannelQuiz] Note: could not delete trigger post: {del_err}")

        date_key = get_quiz_date_key()
        questions = fetch_questions(date_key)
        if not questions:
            try:
                context.bot.send_message(
                    chat_id=chat_id,
                    text="❌ <b>No quiz questions found for today's cycle.</b>\nQuestions are updated at 09:00 AM IST.",
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
            return

        post_group_quiz(context.bot, chat_id, questions, date_key)

def quiz_command_handler(update: Update, context: CallbackContext):
    """
    /quiz — Posts today's quiz questions to the group.
    Restricted to group admins and bot admin only.
    If used in private DM, allows admin to trigger group post or user to attempt DM quiz.
    """
    msg = update.message
    if not msg:
        return

    chat = update.effective_chat
    user = update.effective_user

    # If used in private chat (DM)
    if chat.type in ("private",):
        if user and user.id == ADMIN_ID:
            # Check if admin provided target as argument, e.g. /quiz @vyasify
            target = None
            if context.args and len(context.args) > 0:
                target = context.args[0].strip()
                save_channel_target(target)
            else:
                target = get_saved_channel_id()

            if target:
                msg.reply_text(f"🚀 Admin trigger received! Posting today's quiz to <b>{target}</b>...", parse_mode=ParseMode.HTML)
                date_key = get_quiz_date_key()
                questions = fetch_questions(date_key)
                if questions:
                    post_group_quiz(context.bot, target, questions, date_key)
                    msg.reply_text(f"✅ Quiz posted successfully to {target}!", parse_mode=ParseMode.HTML)
                else:
                    msg.reply_text("❌ No quiz questions found for today's cycle.", parse_mode=ParseMode.HTML)
                return
            else:
                msg.reply_text(
                    "ℹ️ <b>Channel not yet linked.</b>\n\n"
                    "To post to your channel, send:\n"
                    "<code>/quiz @YourChannelUsername</code> (e.g. <code>/quiz @vyasify</code>)\n\n"
                    "Or send <code>/quiz</code> directly inside your channel!\n\n"
                    "<i>Ensure @vyasify_quiz_bot is added as an <b>Administrator</b> in your channel with 'Post Messages' permission.</i>",
                    parse_mode=ParseMode.HTML
                )
                return
        else:
            msg.reply_text(
                "ℹ️ <b>The /quiz command posts daily polls to Telegram channels.</b>\n\n"
                "To attempt today's timed quiz here in private chat, tap /start below!",
                parse_mode=ParseMode.HTML,
                reply_markup=start_keyboard()
            )
            return

    # In group: check admin status
    sender_chat_id = msg.sender_chat.id if msg.sender_chat else None
    if not is_group_admin(context.bot, chat.id, user.id if user else 0, sender_chat_id):
        try:
            msg.reply_text("🔒 Only group admins can run the daily quiz in this group.", parse_mode=ParseMode.HTML)
        except Exception:
            pass
        return

    date_key = get_quiz_date_key()

    # Fetch questions
    questions = fetch_questions(date_key)
    if not questions:
        msg.reply_text(
            "❌ <b>No quiz questions found for today's cycle.</b>\nQuestions are updated at 09:00 AM IST.",
            parse_mode=ParseMode.HTML
        )
        return

    # Warn if already posted today (but allow force-repost by admin)
    if group_quiz_posted.get(date_key):
        try:
            warning = msg.reply_text(
                f"⚠️ Today's quiz ({date_key}) was already posted to this group.\n"
                "Posting again in 3 seconds...",
                parse_mode=ParseMode.HTML
            )
            time.sleep(3)
            warning.delete()
        except Exception:
            pass

    # Confirm and start posting
    try:
        confirm = msg.reply_text(
            f"📡 <b>Posting today's quiz ({date_key})...</b>",
            parse_mode=ParseMode.HTML
        )
    except Exception:
        confirm = None

    post_group_quiz(context.bot, chat.id, questions, date_key)

    try:
        if confirm:
            confirm.delete()
    except Exception:
        pass


def auto_post_group_quiz(context: CallbackContext):
    """
    Daily job: auto-posts quiz to channel at 5:00 PM IST if not already manually posted.
    Uses saved channel ID, GROUP_CHAT_ID env var, or config.
    """
    target = get_saved_channel_id()
    if not target:
        cfg = get_bot_config()
        target = str(cfg.get("groupChatId", "")).strip()

    if not target or target in ("", "0"):
        print("[AutoQuiz] No channel target configured. Skipping auto-post.")
        return

    # Can be numeric ID or @channel_username
    try:
        group_chat_id = int(target)
    except ValueError:
        group_chat_id = target

    date_key = get_quiz_date_key()

    # Skip if already posted manually today
    if group_quiz_posted.get(date_key):
        print(f"[AutoQuiz] Quiz for {date_key} already posted. Skipping auto-post.")
        return

    questions = fetch_questions(date_key)
    if not questions:
        print(f"[AutoQuiz] No questions available for {date_key}. Skipping.")
        return

    print(f"[AutoQuiz] Auto-posting quiz for {date_key} to group {group_chat_id}...")
    post_group_quiz(context.bot, group_chat_id, questions, date_key)


# ================= GROUP EXPLANATION DM HANDLER =================

def serve_group_explanation_dm(update: Update, context: CallbackContext):
    """
    Called when a user opens the bot from t.me/vyasify_quiz_bot?start=exp_{date}_{q_index}
    Delivers the full UPSC-style explanation in a private DM.

    Key design: This is ALWAYS available — even if the student only answered Q3 and not Q7.
    The explanation is served from the global group_poll_registry which stores ALL posted
    group questions, regardless of whether the student answered them or not.
    """
    user = update.effective_user
    if user:
        record_user(user)

    args = context.args  # e.g. ["exp_2026_11_17_2"]
    if not args or not args[0].startswith("exp_"):
        # Regular /start — route to normal welcome
        welcome_user(update, context)
        return

    param = args[0]  # exp_2026_11_17_2
    # Parse: exp_{YYYY}_{MM}_{DD}_{q_index}
    # Stored as exp_YYYY_MM_DD_N where date dashes became underscores
    parts = param.split("_")
    # parts = ['exp', 'YYYY', 'MM', 'DD', 'N']  (5 parts if date was YYYY-MM-DD)
    if len(parts) < 5:
        welcome_user(update, context)
        return

    try:
        q_index = int(parts[-1])
        date_key = f"{parts[1]}-{parts[2]}-{parts[3]}"  # Reconstruct YYYY-MM-DD
    except (ValueError, IndexError):
        welcome_user(update, context)
        return

    # Expiry Check: explanations stay active until today's cycle begins (9:00 AM IST)
    current_active_date = get_quiz_date_key()
    if date_key < current_active_date:
        context.bot.send_message(
            chat_id=user.id,
            text=(
                f"⏰ <b>This Quiz Has Expired</b>\n\n"
                f"The explanation window for <b>{date_key}</b> has ended.\n\n"
                f"👉 <b>Attempt today's questions</b> in the Vyasify group chat, "
                f"or send <b>Hi</b> here to practice today's test with our live timer!"
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=start_keyboard()
        )
        return

    # Look up the question from live registry first (fastest)
    q_data = None
    for pid, entry in group_poll_registry.items():
        if entry["date_key"] == date_key and entry["q_index"] == q_index:
            q_data = entry["q"]
            break

    # Fallback: re-fetch from sheet if registry was cleared (e.g. after bot restart)
    if not q_data:
        questions = fetch_questions(date_key)
        if questions and q_index < len(questions):
            q_data = questions[q_index]

    if not q_data:
        context.bot.send_message(
            chat_id=user.id,
            text=(
                "⚠️ <b>Explanation not found</b> for this question.\n\n"
                "📖 <a href='https://vyasify.in/current-affairs'>Visit Vyasify Daily Current Affairs</a> "
                "for the full editorial and UPSC analysis."
            ),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )
        return

    # Build the full explanation message
    opts = ["A", "B", "C", "D"]
    correct_idx = extract_correct_option_id(q_data.get("correct_option"))
    correct_letter = opts[correct_idx] if 0 <= correct_idx <= 3 else "A"
    correct_text = q_data.get(f"option_{correct_letter.lower()}", "")

    raw_explanation = q_data.get("explanation", "") or ""
    if not raw_explanation.strip():
        raw_explanation = "For the detailed explanation and contextual notes, visit Vyasify Daily Current Affairs."

    exp_msg = (
        f"📘 <b>Q{q_index + 1}. {clean_text(q_data.get('question', ''))}</b>\n\n"
        f"✅ <b>Correct Answer:</b> ({correct_letter}) {clean_text(correct_text)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📖 <b>Explanation:</b>\n{clean_text(raw_explanation)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>For more in-depth editorial notes, Prelims PYQ analysis, and GS mapping:</b>\n"
        f"<a href='https://vyasify.in/current-affairs'>📰 vyasify.in/current-affairs</a>"
    )

    # Chunk if too long (Telegram 4096 char limit per message)
    if len(exp_msg) <= 4096:
        context.bot.send_message(
            chat_id=user.id,
            text=exp_msg,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=False
        )
    else:
        # Split at the divider
        chunks = [exp_msg[i:i+4000] for i in range(0, len(exp_msg), 4000)]
        for chunk in chunks:
            context.bot.send_message(
                chat_id=user.id,
                text=chunk,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False
            )
            time.sleep(0.3)

    # Offer full quiz invite after explanation
    context.bot.send_message(
        chat_id=user.id,
        text=(
            "🎯 <b>Want to attempt today's full quiz privately with scoring?</b>\n"
            "Tap /start to begin the bot quiz with marks, leaderboard, and scorecard!"
        ),
        parse_mode=ParseMode.HTML
    )


# ================= MAIN =================

def main():
    load_leaderboard()
    load_users()
    load_group_quiz_posted()
    load_group_answers()

    updater = Updater(BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    jq = updater.job_queue

    dp.add_handler(CommandHandler("start", welcome_user))
    dp.add_handler(CommandHandler("leaderboard", show_leaderboard))
    dp.add_handler(CommandHandler("broadcast", broadcast_command_handler))
    dp.add_handler(CommandHandler("users", users_stats_handler))
    dp.add_handler(CommandHandler("quiz", quiz_command_handler))

    # Channel post listener: catches /quiz posted directly inside Telegram channels
    try:
        if hasattr(Filters, 'update') and hasattr(Filters.update, 'channel_posts'):
            dp.add_handler(MessageHandler(Filters.update.channel_posts, channel_post_handler))
        elif hasattr(Filters, 'update') and hasattr(Filters.update, 'channel_post'):
            dp.add_handler(MessageHandler(Filters.update.channel_post, channel_post_handler))
        elif hasattr(Filters, 'UpdateType') and hasattr(Filters.UpdateType, 'CHANNEL_POSTS'):
            dp.add_handler(MessageHandler(Filters.UpdateType.CHANNEL_POSTS, channel_post_handler))
        elif hasattr(Filters, 'UpdateType') and hasattr(Filters.UpdateType, 'CHANNEL_POST'):
            dp.add_handler(MessageHandler(Filters.UpdateType.CHANNEL_POST, channel_post_handler))
        else:
            dp.add_handler(MessageHandler(Filters.all, channel_post_handler))
        print("[Setup] Registered channel_post_handler for channel triggers.")
    except Exception as chan_err:
        print(f"[Setup] Channel post handler registration note: {chan_err}")

    dp.add_handler(CallbackQueryHandler(button_handler))

    # Register visible bot commands in Telegram UI
    try:
        from telegram import BotCommand
        commands = [
            BotCommand("start", "Attempt today's timed prelims quiz"),
            BotCommand("quiz", "Post daily quiz in group (Admin)"),
            BotCommand("leaderboard", "View daily top rankers"),
        ]
        updater.bot.set_my_commands(commands)
    except Exception as cmd_err:
        print(f"[Commands] set_my_commands warning: {cmd_err}")
    dp.add_handler(PollAnswerHandler(poll_answer_handler))
    dp.add_handler(MessageHandler(Filters.photo, photo_broadcast_handler))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, text_handler))

    # ── Schedule auto-post at 17:00 IST (11:30 UTC) daily ──
    # IST = UTC+5:30, so 17:00 IST = 11:30 UTC
    IST_OFFSET = timedelta(hours=5, minutes=30)
    now_ist_dt = datetime.utcnow() + IST_OFFSET
    target_ist = now_ist_dt.replace(hour=17, minute=0, second=0, microsecond=0)
    if now_ist_dt >= target_ist:
        # Already past 5PM today, schedule for tomorrow
        target_ist += timedelta(days=1)
    delay_seconds = (target_ist - now_ist_dt).total_seconds()

    jq.run_once(auto_post_group_quiz, delay_seconds, name="auto_quiz_first_run")

    # After first fire, schedule recurring daily at 11:30 UTC (5PM IST)
    # We use run_repeating with 86400s (24h) starting from that first fire point
    # run_once callback re-schedules itself via run_repeating
    def schedule_daily_quiz(context_inner: CallbackContext):
        auto_post_group_quiz(context_inner)
        # Re-arm for next day
        context_inner.job_queue.run_once(schedule_daily_quiz, 86400, name="auto_quiz_daily")

    # Patch first run to also re-arm
    def first_run_quiz(context_inner: CallbackContext):
        auto_post_group_quiz(context_inner)
        context_inner.job_queue.run_once(schedule_daily_quiz, 86400, name="auto_quiz_daily")

    # Cancel the plain first_run job, replace with re-arming one
    for job in jq.get_jobs_by_name("auto_quiz_first_run"):
        job.schedule_removal()
    jq.run_once(first_run_quiz, delay_seconds, name="auto_quiz")

    print(f"✅ Vyasify Dynamic Bot is Online...")
    print(f"📅 Group auto-quiz scheduled: first fire in {int(delay_seconds/60)}m {int(delay_seconds%60)}s")
    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
