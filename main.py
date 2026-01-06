#!/usr/bin/env python3
"""
Quiz bot with fixes and a fallback synchronous send when queues are full.
"""

import os
import csv
import time
import asyncio
import requests
import re
import uuid
import sys
import random
import json
from io import StringIO
from datetime import datetime, timezone, timedelta, time as dtime
import logging
import signal
import unittest

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    PollAnswerHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from telegram.helpers import escape_markdown
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

# ================= LOGGING =================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ================= TIMEZONE =================
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


def today_date():
    return now_ist().date()


# ================= CONFIG =================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN and "test" not in sys.argv:
    raise RuntimeError("BOT_TOKEN environment variable is required")

_admin_env = os.environ.get("ADMIN_IDS")
if _admin_env:
    try:
        ADMIN_IDS = {int(x.strip()) for x in _admin_env.split(",") if x.strip()}
    except Exception:
        logger.warning("Invalid ADMIN_IDS environment variable; falling back to default admin set.")
        ADMIN_IDS = {2053638316}
else:
    ADMIN_IDS = {2053638316}

OFFENSIVE_WORDS = {"fuck", "shit", "bitch", "asshole", "idiot", "stupid"}

QUIZ_CSV_URL = os.environ.get("QUIZ_CSV_URL", "")
if not QUIZ_CSV_URL:
    QUIZ_CSV_URL = (
        "https://docs.google.com/spreadsheets/d/e/"
        "2PACX-1vT6NEUPMF8_uGPSXuX5pfxKypuJIdmCMIUs1p6vWe3YRwQK-o5qd_adVHG6XCjUNyg00EsnNMJZqz8C"
        "/pub?output=csv"
    )

DEFAULT_QUESTION_TIME = int(os.environ.get("DEFAULT_QUESTION_TIME", "20"))
TRANSITION_DELAY = float(os.environ.get("TRANSITION_DELAY", "1"))
DEFAULT_MARKS_PER_QUESTION = float(os.environ.get("DEFAULT_MARKS_PER_QUESTION", "2"))
DEFAULT_NEGATIVE_RATIO = float(os.environ.get("DEFAULT_NEGATIVE_RATIO", str(1 / 3)))

QUIZ_SWITCH_HOUR = int(os.environ.get("QUIZ_SWITCH_HOUR", "17"))
GRACE_SECONDS = float(os.environ.get("GRACE_SECONDS", "0.5"))
SESSION_TTL = int(os.environ.get("SESSION_TTL", str(30 * 60)))
CSV_FETCH_ATTEMPTS = int(os.environ.get("CSV_FETCH_ATTEMPTS", "3"))
CSV_FETCH_BACKOFF_BASE = float(os.environ.get("CSV_FETCH_BACKOFF_BASE", "1.0"))

FORCE_PRELOAD_FINAL_WINDOW_SECONDS = int(os.environ.get("FORCE_PRELOAD_FINAL_WINDOW_SECONDS", "30"))
RATE_LIMIT_LATE_MSG_SECONDS = float(os.environ.get("RATE_LIMIT_LATE_MSG_SECONDS", "5.0"))

# Throttling / send queue config
MAX_CONCURRENT_SENDS = int(os.environ.get("MAX_CONCURRENT_SENDS", "8"))
SEND_JITTER_MAX = float(os.environ.get("SEND_JITTER_MAX", "0.5"))
SEND_RETRY_MAX = int(os.environ.get("SEND_RETRY_MAX", "4"))
SEND_BASE_BACKOFF = float(os.environ.get("SEND_BASE_BACKOFF", "0.5"))

# Queue bounding
MAX_USER_QUEUE_SIZE = int(os.environ.get("MAX_USER_QUEUE_SIZE", "500"))
MAX_ADMIN_QUEUE_SIZE = int(os.environ.get("MAX_ADMIN_QUEUE_SIZE", "200"))

# Persistence file for daily_scores
DAILY_SCORES_FILE = os.environ.get("DAILY_SCORES_FILE", "daily_scores.json")

# ================= STATE (in-process) =================
sessions = {}  # user_id -> session dict (ephemeral)
daily_scores = {}  # user_id -> {"name", "score", "time", "quiz_date_key"}
current_quiz_date_key = None

# Cached normalized questions keyed by quiz_date_key
_cached_quizzes = {}  # quiz_date_key -> list of normalized rows
_cached_quiz_lock = asyncio.Lock()

# Send queues and semaphore to throttle outgoing Telegram API calls
_send_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
_send_queue_user = asyncio.Queue(maxsize=MAX_USER_QUEUE_SIZE)
_send_queue_admin = asyncio.Queue(maxsize=MAX_ADMIN_QUEUE_SIZE)
_background_tasks = []
_shutdown_event = asyncio.Event()

# Locks
_update_lock = asyncio.Lock()

# Metrics
metrics = {
    "skipped_questions": 0,
    "late_answers_rejected": 0,
    "late_answers_treated_as_timeout": 0,
    "preload_failures": 0,
    "timeout_lateness_count": 0,
    "user_skips": 0,
    "send_retries": 0,
    "send_failures": 0,
    "queue_rejections_user": 0,
    "queue_rejections_admin": 0,
}

# ================= HELPERS =================


def fetch_csv(url):
    r = requests.get(f"{url}&_ts={int(time.time())}", timeout=15)
    r.raise_for_status()
    return list(csv.DictReader(StringIO(r.content.decode("utf-8-sig"))))


def fetch_csv_with_retries(url, attempts=CSV_FETCH_ATTEMPTS, backoff_base=CSV_FETCH_BACKOFF_BASE):
    delay = backoff_base
    last_exc = None
    for i in range(attempts):
        try:
            return fetch_csv(url)
        except Exception as e:
            last_exc = e
            logger.warning("CSV fetch failed (attempt %d/%d): %s", i + 1, attempts, e)
            time.sleep(delay)
            delay *= 2
    logger.error("CSV fetch failed after %d attempts", attempts)
    metrics["preload_failures"] += 1
    raise last_exc


def contains_offensive(text):
    if not text:
        return False
    words = set(re.findall(r"\b\w+\b", text.lower()))
    return bool(words & OFFENSIVE_WORDS)


def parse_correct_option(opt):
    if not opt:
        return None
    opt = opt.strip().upper()
    if len(opt) != 1 or opt < "A" or opt > "D":
        return None
    return ord(opt) - 65


def safe_escape(text):
    if not text:
        return ""
    try:
        return escape_markdown(text, version=2)
    except Exception:
        return re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', text)


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
        r["_date_obj"] = parsed.date()

        try:
            tval = float(r.get("time", DEFAULT_QUESTION_TIME))
            r["_time_limit"] = max(1, int(tval))
        except Exception:
            r["_time_limit"] = DEFAULT_QUESTION_TIME

        try:
            r["_marks"] = float(r.get("marks", DEFAULT_MARKS_PER_QUESTION))
            if r["_marks"] < 0:
                r["_marks"] = DEFAULT_MARKS_PER_QUESTION
        except Exception:
            r["_marks"] = DEFAULT_MARKS_PER_QUESTION

        try:
            nr = float(r.get("negative_ratio", DEFAULT_NEGATIVE_RATIO))
            if nr < 0 or nr > 1:
                r["_neg_ratio"] = DEFAULT_NEGATIVE_RATIO
            else:
                r["_neg_ratio"] = nr
        except Exception:
            r["_neg_ratio"] = DEFAULT_NEGATIVE_RATIO

        if parse_correct_option(r.get("correct_option")) is None:
            r["_invalid_reason"] = "invalid_correct_option"
        normalized.append(r)
    return normalized


def effective_date_for_now():
    now = now_ist()
    cutoff = dtime(hour=QUIZ_SWITCH_HOUR, minute=0)
    if now.time() < cutoff:
        return (now.date() - timedelta(days=1))
    return now.date()


def get_effective_quiz_date(rows):
    eff = effective_date_for_now()
    available = sorted({r["_date_obj"] for r in rows})
    valid = [d for d in available if d <= eff]
    return valid[-1] if valid else None


# ================= CACHED PRELOADS & PERSISTENCE =================


async def preload_quiz_if_needed():
    global current_quiz_date_key, _cached_quizzes
    try:
        rows = normalize_sheet_rows(fetch_csv_with_retries(QUIZ_CSV_URL))
    except Exception:
        logger.exception("Failed to fetch CSV in preload_quiz_if_needed")
        return None

    quiz_date = get_effective_quiz_date(rows)
    if not quiz_date:
        logger.info("No quiz date available in CSV during preload")
        return None

    quiz_date_key = quiz_date.isoformat()
    async with _cached_quiz_lock:
        if quiz_date_key not in _cached_quizzes:
            _cached_quizzes[quiz_date_key] = [r for r in rows if r["_date_obj"] == quiz_date]
            logger.info("Cached quiz for date %s with %d questions", quiz_date_key, len(_cached_quizzes[quiz_date_key]))
        async with _update_lock:
            if quiz_date_key != current_quiz_date_key:
                daily_scores.clear()
                current_quiz_date_key = quiz_date_key
                logger.info("Updated current_quiz_date_key to %s via preload", quiz_date_key)
    return quiz_date_key


def load_daily_scores_from_disk():
    global daily_scores
    try:
        if os.path.exists(DAILY_SCORES_FILE):
            with open(DAILY_SCORES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    daily_scores.update(data)
                    logger.info("Loaded daily_scores from disk (%d entries)", len(daily_scores))
    except Exception:
        logger.exception("Failed to load daily_scores from disk")


def persist_daily_scores_to_disk():
    try:
        with open(DAILY_SCORES_FILE, "w", encoding="utf-8") as f:
            json.dump(daily_scores, f)
        logger.debug("Persisted daily_scores to disk (%d entries)", len(daily_scores))
    except Exception:
        logger.exception("Failed to persist daily_scores to disk")


# ================= SEND QUEUE & WORKER (admin priority) =================


async def send_with_retries(bot, chat_id, send_coro_factory, max_retries=SEND_RETRY_MAX):
    attempt = 0
    backoff = SEND_BASE_BACKOFF
    while attempt <= max_retries:
        attempt += 1
        try:
            async with _send_semaphore:
                jitter = random.uniform(0, SEND_JITTER_MAX)
                if jitter:
                    await asyncio.sleep(jitter)
                coro = send_coro_factory()
                result = await coro
                return result
        except RetryAfter as e:
            wait = getattr(e, "retry_after", None) or (backoff * attempt)
            logger.warning("RetryAfter received, sleeping %.2fs", wait)
            metrics["send_retries"] += 1
            await asyncio.sleep(wait)
            backoff *= 2
            continue
        except (TimedOut, NetworkError) as e:
            logger.warning("Transient network error on send attempt %d: %s", attempt, e)
            metrics["send_retries"] += 1
            await asyncio.sleep(backoff * attempt)
            backoff *= 2
            continue
        except TelegramError as e:
            logger.exception("TelegramError on send: %s", e)
            metrics["send_failures"] += 1
            raise
        except Exception as e:
            logger.exception("Unexpected error during send: %s", e)
            metrics["send_failures"] += 1
            await asyncio.sleep(backoff * attempt)
            backoff *= 2
            continue
    raise RuntimeError("Exceeded send retries")


async def _send_worker_loop(app):
    bot = app.bot
    logger.info("BACKGROUND: send worker started")
    while not _shutdown_event.is_set():
        job = None
        try:
            # Prefer admin queue
            try:
                job = _send_queue_admin.get_nowait()
            except asyncio.QueueEmpty:
                try:
                    job = await asyncio.wait_for(_send_queue_user.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
            if not job:
                continue

            result = None
            try:
                # Execute send with retries; this returns the Message object on success
                result = await send_with_retries(bot, job["chat_id"], job["factory"])
            except Exception:
                logger.exception("Failed to send job: %s", job.get("type"))
            finally:
                # mark done on the appropriate queue
                if job.get("priority") == "admin":
                    try:
                        _send_queue_admin.task_done()
                    except Exception:
                        pass
                else:
                    try:
                        _send_queue_user.task_done()
                    except Exception:
                        pass

            # Record poll_message_id into session if this was a poll send
            if job.get("type") == "poll" and job.get("session_user_id") and result is not None:
                uid = job.get("session_user_id")
                s = sessions.get(uid)
                if s:
                    try:
                        async with s["lock"]:
                            s["poll_message_id"] = getattr(result, "message_id", None)
                            s["last_activity"] = time.time()
                    except Exception:
                        logger.exception("Failed to set poll_message_id for user %s after send", uid)

        except Exception:
            logger.exception("Unexpected error in send worker loop")


async def enqueue_send_poll(chat_id, factory, session_user_id=None, priority="user"):
    job = {"type": "poll", "chat_id": chat_id, "factory": factory, "session_user_id": session_user_id, "priority": priority}
    if priority == "admin":
        try:
            _send_queue_admin.put_nowait(job)
            return True
        except asyncio.QueueFull:
            metrics["queue_rejections_admin"] += 1
            logger.warning("Admin send queue full; rejecting admin job for chat %s", chat_id)
            return False
    else:
        try:
            _send_queue_user.put_nowait(job)
            return True
        except asyncio.QueueFull:
            metrics["queue_rejections_user"] += 1
            logger.warning("User send queue full; rejecting user job for chat %s", chat_id)
            return False


# ================= EXPLANATION RECORDER =================


def record_explanation(session, q, q_no, reason=None):
    if any(e.get("q_no") == q_no for e in session["explanations"]):
        return
    question_text = q.get("question", "").replace("\\n", "\n")
    explanation_text = q.get("explanation", "").replace("\\n", "\n")
    if reason:
        explanation_text = f"{reason}\n\n{explanation_text}"
    session["explanations"].append({
        "q_no": q_no,
        "question": question_text,
        "explanation": explanation_text,
    })


# ================= GREETING & UI =================


async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])
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
    await context.bot.send_message(chat_id=user_id, text=text, reply_markup=keyboard, parse_mode="MarkdownV2")


def _skip_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_q")]])


# ================= COMMANDS & HANDLERS =================


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    data = query.data or ""
    try:
        await query.answer()
    except Exception:
        pass

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
            parse_mode="MarkdownV2",
        )
        return

    if data == "skip_q":
        await handle_skip_callback(query, context)
        return


# ================= QUIZ START (uses cached quizzes) =================


async def start_quiz(context, user_id, name):
    global current_quiz_date_key, _cached_quizzes

    async with _cached_quiz_lock:
        if not current_quiz_date_key or current_quiz_date_key not in _cached_quizzes:
            await preload_quiz_if_needed()

    if not current_quiz_date_key or current_quiz_date_key not in _cached_quizzes:
        await context.bot.send_message(chat_id=user_id, text="❌ Today’s quiz is not yet available.")
        return

    questions = _cached_quizzes.get(current_quiz_date_key, [])

    sessions[user_id] = {
        "questions": questions,
        "index": 0,
        "current_q_index": 0,
        "score": 0,
        "attempted": 0,
        "wrong": 0,
        "marks": 0.0,
        "skipped_user": 0,
        "skipped_timeout": 0,
        "skipped_invalid": 0,
        "start": time.time(),
        "transitioned": False,
        "poll_message_id": None,
        "timeout_handle": None,
        "timeout_token": None,
        "q_deadline": None,
        "name": name,
        "explanations": [],
        "lock": asyncio.Lock(),
        "last_activity": time.time(),
        "last_action": None,
        "in_question": False,
        "quiz_date_key": current_quiz_date_key,
        "skip_disabled": False,
        "last_late_msg_ts": 0.0,
    }

    try:
        await context.bot.send_message(chat_id=user_id, text="✅ Quiz queued. You will receive the first question shortly.")
    except Exception:
        pass

    try:
        await send_question(context, user_id)
    except Exception:
        logger.exception("Error while starting quiz for user %s", user_id)
        sessions.pop(user_id, None)
        await context.bot.send_message(chat_id=user_id, text="❌ An error occurred. Please try again later.")


# ================= QUIZ FLOW & TIMING (enqueue sends) =================


def _cancel_timeout_handle(s):
    h = s.get("timeout_handle")
    if h:
        try:
            h.cancel()
        except Exception:
            logger.exception("Failed to cancel timeout handle")
    s["timeout_handle"] = None
    s["timeout_token"] = None


def _treat_late_answer_as_timeout_in_lock(s, user_id):
    if not s:
        return False
    try:
        h = s.get("timeout_handle")
        if h:
            try:
                h.cancel()
            except Exception:
                pass
    except Exception:
        pass
    s["timeout_handle"] = None
    s["timeout_token"] = None

    s["skipped_timeout"] = s.get("skipped_timeout", 0) + 1
    metrics["skipped_questions"] += 1
    metrics["late_answers_treated_as_timeout"] += 1

    try:
        q_index = s.get("index", 0)
        q = s["questions"][q_index]
        record_explanation(s, q, q_index + 1, reason="Skipped due to timeout (answer arrived late).")
    except Exception:
        logger.exception("Failed to record explanation for late-answer timeout for user %s", user_id)

    s["in_question"] = False
    s["last_action"] = ("late_answer_treated_as_timeout", time.time())
    s["transitioned"] = True
    s["index"] = s.get("index", 0) + 1
    return True


async def question_timeout_by_handle(context, user_id, q_index, token):
    s = sessions.get(user_id)
    if not s:
        return
    if s.get("timeout_token") != token:
        logger.debug("Stale timeout callback ignored for user %s", user_id)
        return

    async with s["lock"]:
        if s.get("transitioned"):
            return
        if s.get("timeout_token") != token:
            logger.debug("Stale timeout callback ignored inside lock for user %s", user_id)
            return

        loop = asyncio.get_running_loop()
        now = loop.time()
        deadline = s.get("q_deadline", 0)

        if now < deadline - 0.001:
            try:
                s["timeout_handle"] = loop.call_at(deadline, lambda uid=user_id, qi=q_index, t=token: asyncio.create_task(question_timeout_by_handle(context, uid, qi, t)))
            except Exception:
                logger.exception("Failed to reschedule early timeout")
            return

        lateness = max(0.0, now - deadline)
        if lateness > 0.001:
            metrics["timeout_lateness_count"] += 1
            logger.debug("Timeout handler lateness for user %s: %.3fs", user_id, lateness)

        s["skipped_timeout"] = s.get("skipped_timeout", 0) + 1
        metrics["skipped_questions"] += 1

        try:
            record_explanation(s, s["questions"][q_index], q_index + 1, reason="Skipped due to timeout.")
            s["last_action"] = ("timeout", time.time())
            s["in_question"] = False
            s["timeout_token"] = None
            s["timeout_handle"] = None
            await advance_question(context, user_id)
        except Exception:
            logger.exception("Error in question_timeout_by_handle for user %s", user_id)


async def send_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    s["last_activity"] = time.time()

    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    q = s["questions"][s["index"]]
    s["current_q_index"] = s["index"]
    s["transitioned"] = False

    if q.get("_invalid_reason"):
        metrics["skipped_questions"] += 1
        logger.warning("Skipping invalid question for user %s at index %s: reason=%s", user_id, s["index"], q.get("_invalid_reason"))
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid data in the source. It will not be counted.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid question data (please contact content team)."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    question_text = q.get("question", "").replace("\\n", "\n")
    safe_question = safe_escape(f"Q{s['index'] + 1}. {question_text}")

    correct_idx = parse_correct_option(q.get("correct_option"))
    if correct_idx is None:
        metrics["skipped_questions"] += 1
        logger.warning("Invalid correct_option encountered at send_question for user %s index %s", user_id, s["index"])
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid data.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid question data (please contact content team)."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    raw_options = [
        q.get("option_a", "") or "",
        q.get("option_b", "") or "",
        q.get("option_c", "") or "",
        q.get("option_d", "") or "",
    ]
    options = [o for o in raw_options if o and o.strip()]
    if len(options) < 2:
        metrics["skipped_questions"] += 1
        logger.warning("Not enough options for user %s question index %s; skipping", user_id, s["index"])
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid options.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid options in source."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    if correct_idx < 0 or correct_idx >= 4 or not raw_options[correct_idx].strip():
        metrics["skipped_questions"] += 1
        logger.warning("Correct option index invalid for user %s question index %s", user_id, s["index"])
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid correct option.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid correct option in source."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    def factory():
        return context.bot.send_poll(
            chat_id=user_id,
            question="Choose the correct answer:",
            options=raw_options,
            type="quiz",
            correct_option_id=correct_idx,
            open_period=q.get("_time_limit", DEFAULT_QUESTION_TIME),
            is_anonymous=False,
            reply_markup=_skip_keyboard(),
        )

    priority = "user"
    enqueued = await enqueue_send_poll(user_id, factory, session_user_id=user_id, priority=priority)
    if not enqueued:
        # Fallback: attempt synchronous send with retries so user still receives the poll
        logger.warning("SEND-FALLBACK: queue full for user %s; attempting synchronous send", user_id)
        try:
            result = await send_with_retries(context.bot, user_id, factory)
            # record poll_message_id into session
            if result:
                try:
                    async with s["lock"]:
                        s["poll_message_id"] = getattr(result, "message_id", None)
                        s["last_activity"] = time.time()
                except Exception:
                    logger.exception("Failed to set poll_message_id after fallback send for user %s", user_id)
        except Exception:
            logger.exception("SEND-FALLBACK failed for user %s", user_id)
            try:
                await context.bot.send_message(chat_id=user_id, text="⚠️ High load right now. Please try again in a few seconds.")
            except Exception:
                pass
            return

    s["in_question"] = True
    loop = asyncio.get_running_loop()
    now = loop.time()
    deadline = now + q.get("_time_limit", DEFAULT_QUESTION_TIME)
    s["q_deadline"] = deadline
    token = uuid.uuid4().hex
    s["timeout_token"] = token
    try:
        s["timeout_handle"] = loop.call_at(deadline, lambda uid=user_id, qi=s["index"], t=token: asyncio.create_task(question_timeout_by_handle(context, uid, qi, t)))
    except Exception:
        logger.exception("Failed to schedule timeout handle for user %s", user_id)
        s["timeout_handle"] = None
        s["timeout_token"] = None

    s["last_action"] = ("sent_poll", time.time())


async def handle_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    poll_answer = update.poll_answer
    user = poll_answer.user
    if not user:
        logger.warning("Received anonymous poll answer; ignoring")
        return
    user_id = user.id
    s = sessions.get(user_id)
    if not s:
        return

    treated_late = False

    async with s["lock"]:
        if s.get("transitioned"):
            la = s.get("last_action")
            if la and la[0] in ("skip", "timeout") and (time.time() - la[1]) < 2.0:
                now_ts = time.time()
                if now_ts - s.get("last_late_msg_ts", 0.0) > RATE_LIMIT_LATE_MSG_SECONDS:
                    try:
                        await context.bot.send_message(chat_id=user_id, text="ℹ️ Your answer arrived after the question was skipped or timed out and was not counted.")
                    except Exception:
                        pass
                    s["last_late_msg_ts"] = now_ts
            return

        loop = asyncio.get_running_loop()
        now = loop.time()
        deadline = s.get("q_deadline", 0)
        if deadline and now > deadline + GRACE_SECONDS:
            metrics["late_answers_treated_as_timeout"] += 1
            logger.info("Late answer arrived for user %s (now=%.3f deadline=%.3f); treating as timeout-skip", user_id, now, deadline)

            _cancel_timeout_handle(s)
            _treat_late_answer_as_timeout_in_lock(s, user_id)

            now_ts = time.time()
            if now_ts - s.get("last_late_msg_ts", 0.0) > RATE_LIMIT_LATE_MSG_SECONDS:
                try:
                    await context.bot.send_message(chat_id=user_id, text="⏱ Your answer arrived after time expired and has been recorded as a timeout skip.")
                except Exception:
                    pass
                s["last_late_msg_ts"] = now_ts

            treated_late = True
        else:
            _cancel_timeout_handle(s)

            q = s["questions"][s["index"]]
            s["attempted"] += 1
            s["last_activity"] = time.time()
            s["in_question"] = False
            s["last_action"] = ("answered", time.time())

            marks_for_q = q.get("_marks", DEFAULT_MARKS_PER_QUESTION)
            neg_ratio_for_q = q.get("_neg_ratio", DEFAULT_NEGATIVE_RATIO)

            selected = poll_answer.option_ids[0] if poll_answer.option_ids else None
            correct_idx = parse_correct_option(q.get("correct_option"))

            if selected is not None and correct_idx is not None and selected == correct_idx:
                s["score"] += 1
                s["marks"] += marks_for_q
            else:
                s["wrong"] += 1
                s["marks"] -= marks_for_q * neg_ratio_for_q

            record_explanation(s, q, s["index"] + 1)

    if treated_late:
        await asyncio.sleep(TRANSITION_DELAY)
        await send_question(context, user_id)
        return

    await advance_question(context, user_id)


async def handle_skip_callback(query, context):
    user = query.from_user
    if not user:
        try:
            await query.answer("Unable to identify you.")
        except Exception:
            pass
        return
    user_id = user.id

    s = sessions.get(user_id)
    if not s:
        try:
            await query.answer()
            await context.bot.send_message(chat_id=user_id, text="ℹ️ You don't have an active quiz right now.")
        except Exception:
            pass
        return

    async with s["lock"]:
        if s.get("transitioned"):
            try:
                await query.answer("This question has already been processed.")
            except Exception:
                pass
            return

        if s.get("skip_disabled"):
            try:
                await query.answer("Skip already processed.")
            except Exception:
                pass
            return

        _cancel_timeout_handle(s)

        q_index = s["index"]
        q = s["questions"][q_index]
        s["explanations"].append({
            "q_no": q_index + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped by user."
        })

        s["skipped_user"] = s.get("skipped_user", 0) + 1
        metrics["user_skips"] += 1

        s["transitioned"] = True
        s["index"] += 1
        s["in_question"] = False
        s["last_action"] = ("skip", time.time())
        s["skip_disabled"] = True

        poll_msg_id = s.get("poll_message_id")

    try:
        if poll_msg_id:
            await context.bot.edit_message_reply_markup(chat_id=user_id, message_id=poll_msg_id, reply_markup=None)
    except Exception:
        pass

    try:
        await context.bot.send_message(chat_id=user_id, text=f"⏭ Question {q_index + 1} skipped. Moving to next question...")
    except Exception:
        pass

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)


async def advance_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    async with s["lock"]:
        if s.get("transitioned"):
            return
        s["transitioned"] = True
        s["index"] += 1
        s["in_question"] = False

    if s["index"] >= len(s["questions"]):
        await finish_quiz(context, user_id)
        return

    await asyncio.sleep(TRANSITION_DELAY)
    await send_question(context, user_id)


# ================= RESULT & ADMIN =================


async def finish_quiz(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    total = len(s["questions"])
    skipped_user = s.get("skipped_user", 0)
    skipped_timeout = s.get("skipped_timeout", 0)
    skipped_invalid = s.get("skipped_invalid", 0)
    skipped_total = skipped_user + skipped_timeout + skipped_invalid
    time_taken = int(time.time() - s["start"])

    session_quiz_key = s.get("quiz_date_key", current_quiz_date_key)
    if user_id not in daily_scores:
        daily_scores[user_id] = {
            "name": s["name"],
            "score": round(s["marks"], 2),
            "time": time_taken,
            "quiz_date_key": session_quiz_key,
            "skipped_user": skipped_user,
            "skipped_timeout": skipped_timeout,
            "skipped_invalid": skipped_invalid,
        }
        persist_daily_scores_to_disk()

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
            f"⏭ Skipped: {skipped_total} (You: {skipped_user}; Timeout: {skipped_timeout}; System: {skipped_invalid})\n"
            f"🎯 Marks: {round(s['marks'],2)}\n"
            f"⏱ Time: {time_taken//60}m {time_taken%60}s\n\n"
            "🏆 *Daily Leaderboard (Top 10)*\n"
            f"{leaderboard}"
        ),
        parse_mode="MarkdownV2"
    )

    if s["explanations"]:
        header = "📖 *Simple Explanations*\n\n"
        chunk = header
        for exp in s["explanations"]:
            q_no = exp.get("q_no")
            q_text = exp.get("question", "")
            expl_text = exp.get("explanation", "")
            safe_q_text = safe_escape(q_text)
            safe_expl_text = safe_escape(expl_text)
            formatted = f"*Q{q_no}.* {safe_q_text}\n*📘Explanation:* {safe_expl_text}"
            if len(chunk) + len(formatted) > 3800:
                await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="MarkdownV2")
                chunk = header
            chunk += formatted + "\n\n"
        if chunk.strip() != header.strip():
            await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="MarkdownV2")

    try:
        if s.get("timeout_handle"):
            try:
                s["timeout_handle"].cancel()
            except Exception:
                pass
    except Exception:
        pass

    sessions.pop(user_id, None)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    if contains_offensive(text):
        await update.message.reply_text("❌ Please maintain respectful language. Send Hi to start the QUIZ.")
        return
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)


def _compute_attempted_count_and_leaderboard():
    finished_users = set(daily_scores.keys())
    in_progress_attempted = {uid for uid, s in sessions.items() if s.get("attempted", 0) > 0}
    unique_attempted = finished_users | in_progress_attempted
    attempted_count = len(unique_attempted)
    ranked = sorted(daily_scores.values(), key=lambda x: (-x["score"], x["time"]))[:10]
    return attempted_count, ranked


async def admin_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    attempted_count, ranked = _compute_attempted_count_and_leaderboard()
    quiz_date = current_quiz_date_key or "N/A"
    leaderboard_text = ""
    for i, r in enumerate(ranked, 1):
        name = safe_escape(str(r.get("name", "Unknown")))
        score = r.get("score", 0)
        t = r.get("time", 0)
        m, sec = divmod(int(t), 60)
        leaderboard_text += f"{i}. {name} — {score} | {m}m {sec}s\n"
    if not leaderboard_text:
        leaderboard_text = "No completed attempts recorded yet.\n"
    safe_quiz_date = safe_escape(str(quiz_date))
    text = (
        f"*Admin Daily Leaderboard*\n\n"
        f"*Quiz date key:* `{safe_quiz_date}`\n"
        f"*Total attempted:* {attempted_count}\n\n"
        f"*Top 10*\n"
        f"{leaderboard_text}"
    )
    await update.message.reply_text(text, parse_mode="MarkdownV2")


async def admin_metrics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    text = (
        f"*Metrics*\n\n"
        f"skipped_questions: {metrics['skipped_questions']}\n"
        f"late_answers_rejected: {metrics['late_answers_rejected']}\n"
        f"late_answers_treated_as_timeout: {metrics['late_answers_treated_as_timeout']}\n"
        f"preload_failures: {metrics['preload_failures']}\n"
        f"timeout_lateness_count: {metrics['timeout_lateness_count']}\n"
        f"user_skips: {metrics['user_skips']}\n"
        f"send_retries: {metrics['send_retries']}\n"
        f"send_failures: {metrics['send_failures']}\n"
        f"queue_rejections_user: {metrics['queue_rejections_user']}\n"
        f"queue_rejections_admin: {metrics['queue_rejections_admin']}\n"
    )
    await update.message.reply_text(text, parse_mode="MarkdownV2")


async def admin_force_preload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    args = context.args or []

    if sessions:
        risky = []
        now_loop = asyncio.get_event_loop().time()
        for uid, s in sessions.items():
            total_q = len(s.get("questions", []))
            idx = s.get("index", 0)
            if idx >= 0 and idx == total_q - 1 and s.get("in_question"):
                deadline = s.get("q_deadline") or 0
                if deadline and (deadline - now_loop) <= FORCE_PRELOAD_FINAL_WINDOW_SECONDS:
                    risky.append(uid)
        if risky and "confirm" not in args:
            await update.message.reply_text(
                "⚠️ Some users are in the final question window. Running /force_preload now may corrupt results.\n"
                "If you really want to proceed, run: /force_preload confirm",
                parse_mode="MarkdownV2"
            )
            return

    if sessions and "confirm" not in args and not risky:
        await update.message.reply_text(
            "⚠️ There are active quizzes in progress. Running force preload now may mix results across quiz dates.\n"
            "If you really want to proceed, run: /force_preload confirm",
            parse_mode="MarkdownV2"
        )
        return

    await update.message.reply_text("⏳ Fetching quiz CSV and initializing quiz date...", parse_mode="MarkdownV2")

    try:
        quiz_date_key = await preload_quiz_if_needed()
    except Exception:
        logger.exception("Force preload failed to fetch CSV")
        await update.message.reply_text("❌ Failed to fetch CSV. Check logs for details.", parse_mode="MarkdownV2")
        return

    if not quiz_date_key:
        await update.message.reply_text("⚠️ No valid quiz date found in the CSV for the effective date.", parse_mode="MarkdownV2")
        return

    await update.message.reply_text(f"✅ Preloaded quiz for *{safe_escape(quiz_date_key)}*.", parse_mode="MarkdownV2")


# ================= BACKGROUND TASKS =================


async def _wait_until_next_cutoff():
    now = now_ist()
    today_cutoff = datetime.combine(now.date(), dtime(hour=QUIZ_SWITCH_HOUR, minute=0), IST)
    if now >= today_cutoff:
        next_cutoff = today_cutoff + timedelta(days=1)
    else:
        next_cutoff = today_cutoff
    return (next_cutoff - now).total_seconds()


def _session_integrity_check():
    for uid, s in sessions.items():
        total = len(s["questions"])
        attempted = s.get("attempted", 0)
        skipped_sum = s.get("skipped_user", 0) + s.get("skipped_timeout", 0) + s.get("skipped_invalid", 0)
        if attempted + skipped_sum > total:
            logger.warning("Integrity issue for user %s: attempted(%s)+skipped(%s) > total(%s)", uid, attempted, skipped_sum, total)
        if attempted + skipped_sum < total:
            logger.warning("Integrity issue for user %s: attempted(%s)+skipped(%s) < total(%s)", uid, attempted, skipped_sum, total)


async def daily_preload_task(app):
    global current_quiz_date_key
    logger.info("BACKGROUND: daily preload task started")
    while not _shutdown_event.is_set():
        wait_seconds = await _wait_until_next_cutoff()
        logger.info("Preload task sleeping for %.0f seconds until next cutoff", wait_seconds)
        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            pass

        if _shutdown_event.is_set():
            break

        try:
            await preload_quiz_if_needed()
        except Exception:
            logger.exception("Failed to fetch CSV at cutoff; will retry next day.")
            metrics["preload_failures"] += 1
            await asyncio.sleep(60)
            continue


async def cleanup_sessions_task():
    logger.info("BACKGROUND: cleanup task started")
    while not _shutdown_event.is_set():
        now_ts = time.time()
        to_remove = []
        for uid, s in list(sessions.items()):
            if s.get("in_question"):
                continue
            if now_ts - s.get("last_activity", now_ts) > SESSION_TTL:
                to_remove.append(uid)
        for uid in to_remove:
            s = sessions.get(uid)
            if not s:
                continue
            logger.info("Cleaning up idle session for user %s", uid)
            try:
                if s.get("timeout_handle"):
                    s["timeout_handle"].cancel()
            except Exception:
                pass
            sessions.pop(uid, None)

        _session_integrity_check()

        try:
            await asyncio.wait_for(_shutdown_event.wait(), timeout=60)
            break
        except asyncio.TimeoutError:
            continue


# ================= GRACEFUL SHUTDOWN =================


async def _shutdown(app):
    logger.info("Shutdown initiated")
    _shutdown_event.set()
    for t in _background_tasks:
        try:
            t.cancel()
        except Exception:
            pass
    for uid, s in list(sessions.items()):
        try:
            if s.get("timeout_handle"):
                s["timeout_handle"].cancel()
        except Exception:
            pass
    try:
        await app.shutdown()
    except Exception:
        logger.exception("Error during app.shutdown()")
    logger.info("Shutdown complete")


# ================= UNIT-TEST SCAFFOLDING =================

class SessionHelpersTest(unittest.TestCase):
    def test_normalize_parsing(self):
        rows = [
            {"date": "01-01-2026", "time": "10", "marks": "3", "negative_ratio": "0.25", "question": "Q", "option_a": "A", "option_b": "B", "option_c": "", "option_d": "", "correct_option": "A", "explanation": "E"},
        ]
        norm = normalize_sheet_rows(rows)
        self.assertEqual(len(norm), 1)
        self.assertEqual(norm[0]["_time_limit"], 10)
        self.assertEqual(norm[0]["_marks"], 3.0)
        self.assertAlmostEqual(norm[0]["_neg_ratio"], 0.25)

    def test_treat_late_answer_as_timeout_in_lock_basic(self):
        user_id = 12345
        q = {"question": "Q1", "explanation": "E1", "option_a": "A", "option_b": "B", "option_c": "C", "option_d": "D", "correct_option": "A"}
        s = {
            "questions": [q],
            "index": 0,
            "skipped_timeout": 0,
            "skipped_user": 0,
            "skipped_invalid": 0,
            "timeout_handle": None,
            "timeout_token": "tok",
            "explanations": [],
            "in_question": True,
            "transitioned": False,
        }
        result = _treat_late_answer_as_timeout_in_lock(s, user_id)
        self.assertTrue(result)
        self.assertEqual(s["skipped_timeout"], 1)
        self.assertEqual(s["index"], 1)
        self.assertTrue(s["transitioned"])
        self.assertFalse(s["in_question"])
        self.assertTrue(any("Skipped due to timeout" in e["explanation"] for e in s["explanations"]))


def run_unit_tests():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SessionHelpersTest)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


# ================= MAIN =================


def main():
    if "test" in sys.argv:
        ok = run_unit_tests()
        sys.exit(0 if ok else 2)

    load_daily_scores_from_disk()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(PollAnswerHandler(handle_answer))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CommandHandler("admin_leaderboard", admin_leaderboard))
    app.add_handler(CommandHandler("admin_metrics", admin_metrics))
    app.add_handler(CommandHandler("force_preload", admin_force_preload))

    async def _start_background_tasks(application):
        # preload and start workers
        await preload_quiz_if_needed()
        logger.info("BACKGROUND: starting send worker, preload and cleanup tasks")
        t_send = asyncio.create_task(_send_worker_loop(application))
        t_preload = asyncio.create_task(daily_preload_task(application))
        t_cleanup = asyncio.create_task(cleanup_sessions_task())
        _background_tasks.extend([t_send, t_preload, t_cleanup])

    app.post_init = _start_background_tasks

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(_shutdown(app)))
        except NotImplementedError:
            pass

    app.run_polling()


if __name__ == "__main__":
    main()
