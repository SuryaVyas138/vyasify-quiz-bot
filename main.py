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
import html
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
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError

# ---------------- logging ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------- timezone ----------------
IST = timezone(timedelta(hours=5, minutes=30))


def now_ist():
    return datetime.now(IST)


# ---------------- config ----------------
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
TRANSITION_DELAY = float(os.environ.get("TRANSITION_DELAY", "0.8"))
DEFAULT_MARKS_PER_QUESTION = float(os.environ.get("DEFAULT_MARKS_PER_QUESTION", "2"))
DEFAULT_NEGATIVE_RATIO = float(os.environ.get("DEFAULT_NEGATIVE_RATIO", str(1 / 3)))

QUIZ_SWITCH_HOUR = int(os.environ.get("QUIZ_SWITCH_HOUR", "17"))
GRACE_SECONDS = float(os.environ.get("GRACE_SECONDS", "0.5"))
SESSION_TTL = int(os.environ.get("SESSION_TTL", str(30 * 60)))
CSV_FETCH_ATTEMPTS = int(os.environ.get("CSV_FETCH_ATTEMPTS", "3"))
CSV_FETCH_BACKOFF_BASE = float(os.environ.get("CSV_FETCH_BACKOFF_BASE", "1.0"))

FORCE_PRELOAD_FINAL_WINDOW_SECONDS = int(os.environ.get("FORCE_PRELOAD_FINAL_WINDOW_SECONDS", "30"))
RATE_LIMIT_LATE_MSG_SECONDS = float(os.environ.get("RATE_LIMIT_LATE_MSG_SECONDS", "5.0"))

# Throttling / send queue config (kept for scale but not used for immediate sends)
MAX_CONCURRENT_SENDS = int(os.environ.get("MAX_CONCURRENT_SENDS", "8"))
SEND_JITTER_MAX = float(os.environ.get("SEND_JITTER_MAX", "0.12"))
SEND_RETRY_MAX = int(os.environ.get("SEND_RETRY_MAX", "4"))
SEND_BASE_BACKOFF = float(os.environ.get("SEND_BASE_BACKOFF", "0.25"))

# Queue bounding
MAX_USER_QUEUE_SIZE = int(os.environ.get("MAX_USER_QUEUE_SIZE", "500"))
MAX_ADMIN_QUEUE_SIZE = int(os.environ.get("MAX_ADMIN_QUEUE_SIZE", "200"))

DAILY_SCORES_FILE = os.environ.get("DAILY_SCORES_FILE", "daily_scores.json")

# ---------------- state ----------------
sessions = {}  # user_id -> session dict
daily_scores = {}
current_quiz_date_key = None

_cached_quizzes = {}
_cached_quiz_lock = asyncio.Lock()

_send_semaphore = asyncio.Semaphore(MAX_CONCURRENT_SENDS)
_send_queue_user = asyncio.Queue(maxsize=MAX_USER_QUEUE_SIZE)
_send_queue_admin = asyncio.Queue(maxsize=MAX_ADMIN_QUEUE_SIZE)
_background_tasks = []
_shutdown_event = asyncio.Event()

_update_lock = asyncio.Lock()

metrics = {
    "skipped_questions": 0,
    "late_answers_treated_as_timeout": 0,
    "preload_failures": 0,
    "timeout_lateness_count": 0,
    "user_skips": 0,
    "send_retries": 0,
    "send_failures": 0,
    "queue_rejections_user": 0,
    "queue_rejections_admin": 0,
}

# ---------------- helpers ----------------


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
            if 0 <= nr <= 1:
                r["_neg_ratio"] = nr
            else:
                r["_neg_ratio"] = DEFAULT_NEGATIVE_RATIO
        except Exception:
            r["_neg_ratio"] = DEFAULT_NEGATIVE_RATIO

        for k in ("question", "option_a", "option_b", "option_c", "option_d", "correct_option", "explanation"):
            if k not in r or r.get(k) is None:
                r[k] = ""

        if parse_correct_option(r.get("correct_option")) is None:
            r["_invalid_reason"] = "invalid_correct_option"
        normalized.append(r)
    return normalized


def effective_date_for_now():
    now = now_ist()
    cutoff = dtime(hour=QUIZ_SWITCH_HOUR, minute=0)
    return (now.date() - timedelta(days=1)) if now.time() < cutoff else now.date()


def get_effective_quiz_date(rows):
    eff = effective_date_for_now()
    available = sorted({r["_date_obj"] for r in rows})
    valid = [d for d in available if d <= eff]
    return valid[-1] if valid else None


# ---------------- preload & persistence ----------------


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
            logger.info("Cached quiz for %s (%d questions)", quiz_date_key, len(_cached_quizzes[quiz_date_key]))
        async with _update_lock:
            if quiz_date_key != current_quiz_date_key:
                daily_scores.clear()
                current_quiz_date_key = quiz_date_key
                logger.info("Set current_quiz_date_key = %s", quiz_date_key)
    return quiz_date_key


def load_daily_scores_from_disk():
    global daily_scores
    try:
        if os.path.exists(DAILY_SCORES_FILE):
            with open(DAILY_SCORES_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    daily_scores.update(data)
                    logger.info("Loaded daily_scores (%d entries)", len(daily_scores))
    except Exception:
        logger.exception("Failed to load daily_scores from disk")


def persist_daily_scores_to_disk():
    try:
        with open(DAILY_SCORES_FILE, "w", encoding="utf-8") as f:
            json.dump(daily_scores, f)
        logger.debug("Persisted daily_scores (%d entries)", len(daily_scores))
    except Exception:
        logger.exception("Failed to persist daily_scores to disk")


# ---------------- send helpers ----------------


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
                return await send_coro_factory()
        except RetryAfter as e:
            wait = getattr(e, "retry_after", None) or (backoff * attempt)
            metrics["send_retries"] += 1
            await asyncio.sleep(wait)
            backoff *= 2
        except (TimedOut, NetworkError) as e:
            metrics["send_retries"] += 1
            await asyncio.sleep(backoff * attempt)
            backoff *= 2
        except TelegramError as e:
            metrics["send_failures"] += 1
            raise
        except Exception:
            metrics["send_failures"] += 1
            await asyncio.sleep(backoff * attempt)
            backoff *= 2
    raise RuntimeError("Exceeded send retries")


async def enqueue_send_poll(chat_id, factory, session_user_id=None, priority="user"):
    job = {"type": "poll", "chat_id": chat_id, "factory": factory, "session_user_id": session_user_id, "priority": priority}
    try:
        queue = _send_queue_admin if priority == "admin" else _send_queue_user
        queue.put_nowait(job)
        return True
    except asyncio.QueueFull:
        if priority == "admin":
            metrics["queue_rejections_admin"] += 1
        else:
            metrics["queue_rejections_user"] += 1
        logger.warning("Queue full (%s); rejecting job for chat %s", priority, chat_id)
        return False


# ---------------- UI helpers ----------------


def _skip_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⏭ Skip", callback_data="skip_q")]])


async def send_greeting(context, user_id, name):
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Today’s Quiz", callback_data="start_quiz")],
        [InlineKeyboardButton("ℹ️ How it works", callback_data="how_it_works")]
    ])
    text = (
        "📘 <b>Welcome to Vyasify Daily Quiz</b>\n\n"
        "This is a daily practice platform for aspirants of 🎯 <b>UPSC | SSC | Regulatory Body Examinations</b>\n\n"
        "🔹 <b>Daily 10 questions</b> strictly aligned to <b>UPSC Prelims-oriented topics</b>\n\n"
        "✅ Correct Answer: 2 Marks\n"
        "❌ Negative Marking: -1/3 Marks\n"
        "🚫 Skipped: 0 Marks\n\n"
        "📝 Timed questions to build exam temperament\n"
        "📊 Score, Rank & Percentile for self-benchmarking\n"
        "📖 Simple explanations for concept clarity\n\n"
        "👇 <b>Tap below to start today’s quiz</b>"
    )
    await context.bot.send_message(chat_id=user_id, text=text, reply_markup=keyboard, parse_mode="HTML")


# ---------------- core quiz flow ----------------


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
        text = (
            "ℹ️ <b>How the Daily Quiz Works</b>\n\n"
            "• 10 exam-oriented questions daily\n"
            "• Timed per question\n"
            "• UPSC-style marking\n"
            "• Leaderboard based on first attempt\n"
            "• Explanations after completion"
        )
        await context.bot.send_message(chat_id=query.from_user.id, text=text, parse_mode="HTML")
        return
    if data == "skip_q":
        await handle_skip_callback(query, context)
        return


async def start_quiz(context, user_id, name):
    global current_quiz_date_key, _cached_quizzes

    async with _cached_quiz_lock:
        if not current_quiz_date_key or current_quiz_date_key not in _cached_quizzes:
            await preload_quiz_if_needed()

    if not current_quiz_date_key or current_quiz_date_key not in _cached_quizzes:
        await context.bot.send_message(chat_id=user_id, text="❌ Today’s quiz is not yet available.")
        return

    questions = _cached_quizzes.get(current_quiz_date_key, [])
    if not questions:
        await context.bot.send_message(chat_id=user_id, text="⚠️ No questions found for today’s quiz. Please try again later.")
        return

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
        "_advancing": False,
    }

    # Immediately send the first question synchronously (no "queued" message)
    try:
        await send_question(context, user_id, immediate=True)
    except Exception:
        logger.exception("Error while starting quiz for user %s", user_id)
        sessions.pop(user_id, None)
        try:
            await context.bot.send_message(chat_id=user_id, text="❌ An error occurred. Please try again later.")
        except Exception:
            pass


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
    s["skip_disabled"] = False
    return True


async def question_timeout_by_handle(context, user_id, q_index, token):
    s = sessions.get(user_id)
    if not s:
        return
    if s.get("timeout_token") != token:
        return

    async with s["lock"]:
        if s.get("transitioned") or s.get("timeout_token") != token:
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

        s["skipped_timeout"] = s.get("skipped_timeout", 0) + 1
        metrics["skipped_questions"] += 1

        try:
            record_explanation(s, s["questions"][q_index], q_index + 1, reason="Skipped due to timeout.")
            s["last_action"] = ("timeout", time.time())
            s["in_question"] = False
            s["timeout_token"] = None
            s["timeout_handle"] = None
            s["skip_disabled"] = False
        except Exception:
            logger.exception("Error in question_timeout_by_handle for user %s", user_id)

    await advance_question(context, user_id)


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


async def send_question(context, user_id, immediate=False):
    """
    immediate=True -> send synchronously via send_poll (no enqueue) to avoid any lag.
    If immediate send fails, fall back to send_with_retries or enqueue.
    """
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
    s["skip_disabled"] = False
    s["poll_message_id"] = None

    if q.get("_invalid_reason"):
        metrics["skipped_questions"] += 1
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid data. It will not be counted.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid question data."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    question_text = q.get("question", "").replace("\\n", "\n").strip() or "Choose the correct answer:"
    correct_idx = parse_correct_option(q.get("correct_option"))
    raw_options = [
        q.get("option_a", "") or "",
        q.get("option_b", "") or "",
        q.get("option_c", "") or "",
        q.get("option_d", "") or "",
    ]
    options = [o for o in raw_options if o and o.strip()]

    if correct_idx is None or len(options) < 2 or correct_idx < 0 or correct_idx >= 4 or not raw_options[correct_idx].strip():
        metrics["skipped_questions"] += 1
        try:
            await context.bot.send_message(chat_id=user_id, text=f"⚠️ Question {s['index'] + 1} skipped due to invalid options/correct answer.")
        except Exception:
            pass
        s["explanations"].append({
            "q_no": s["index"] + 1,
            "question": q.get("question", ""),
            "explanation": "Skipped due to invalid options/correct answer."
        })
        s["skipped_invalid"] = s.get("skipped_invalid", 0) + 1
        s["last_action"] = ("auto_skip_invalid", time.time())
        await advance_question(context, user_id)
        return

    def factory():
        return context.bot.send_poll(
            chat_id=user_id,
            question=question_text,
            options=raw_options,
            type="quiz",
            correct_option_id=correct_idx,
            open_period=q.get("_time_limit", DEFAULT_QUESTION_TIME),
            is_anonymous=False,
            reply_markup=_skip_keyboard(),
        )

    # Immediate synchronous send path (zero perceived lag)
    if immediate:
        try:
            # show typing indicator briefly
            try:
                await context.bot.send_chat_action(chat_id=user_id, action="typing")
            except Exception:
                pass

            result = await context.bot.send_poll(
                chat_id=user_id,
                question=question_text,
                options=raw_options,
                type="quiz",
                correct_option_id=correct_idx,
                open_period=q.get("_time_limit", DEFAULT_QUESTION_TIME),
                is_anonymous=False,
                reply_markup=_skip_keyboard(),
            )
            async with s["lock"]:
                s["poll_message_id"] = getattr(result, "message_id", None)
                s["last_activity"] = time.time()
            logger.info("SEND-IMMEDIATE: sent poll for user %s index=%s", user_id, s["index"])
        except Exception:
            logger.exception("SEND-IMMEDIATE failed for user %s; will attempt reliable send", user_id)
            # fall through to reliable send below

    # If immediate didn't set poll_message_id, try reliable send (retries) or enqueue
    if not s.get("poll_message_id"):
        try:
            result = await send_with_retries(context.bot, user_id, factory)
            if result:
                async with s["lock"]:
                    s["poll_message_id"] = getattr(result, "message_id", None)
                    s["last_activity"] = time.time()
        except Exception:
            logger.exception("Reliable send failed; attempting enqueue/fallback for user %s", user_id)
            enqueued = await enqueue_send_poll(user_id, factory, session_user_id=user_id, priority="user")
            if not enqueued:
                try:
                    result = await context.bot.send_poll(
                        chat_id=user_id,
                        question=question_text,
                        options=raw_options,
                        type="quiz",
                        correct_option_id=correct_idx,
                        open_period=q.get("_time_limit", DEFAULT_QUESTION_TIME),
                        is_anonymous=False,
                        reply_markup=_skip_keyboard(),
                    )
                    async with s["lock"]:
                        s["poll_message_id"] = getattr(result, "message_id", None)
                        s["last_activity"] = time.time()
                except Exception:
                    logger.exception("Final fallback send failed for user %s", user_id)
                    try:
                        await context.bot.send_message(chat_id=user_id, text="⚠️ Unable to send question right now. Please try again.")
                    except Exception:
                        pass
                    return

    s["in_question"] = True
    loop = asyncio.get_running_loop()
    deadline = loop.time() + q.get("_time_limit", DEFAULT_QUESTION_TIME)
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
            _cancel_timeout_handle(s)
            _treat_late_answer_as_timeout_in_lock(s, user_id)
            now_ts = time.time()
            if now_ts - s.get("last_late_msg_ts", 0.0) > RATE_LIMIT_LATE_MSG_SECONDS:
                try:
                    await context.bot.send_message(chat_id=user_id, text="⏱ Your answer arrived after time expired and was recorded as a timeout skip.")
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
        await advance_question(context, user_id)
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

        s["in_question"] = False
        s["last_action"] = ("skip", time.time())
        s["skip_disabled"] = True

        poll_msg_id = s.get("poll_message_id")

    # Remove inline keyboard so Skip visually disables
    try:
        if poll_msg_id:
            await context.bot.edit_message_reply_markup(chat_id=user_id, message_id=poll_msg_id, reply_markup=None)
    except Exception:
        pass

    try:
        await context.bot.send_message(chat_id=user_id, text=f"⏭ Question {q_index + 1} skipped. Moving to next question...")
    except Exception:
        pass

    await advance_question(context, user_id)


async def advance_question(context, user_id):
    s = sessions.get(user_id)
    if not s:
        return

    async with s["lock"]:
        if s.get("_advancing"):
            logger.debug("ADVANCE: already advancing for user %s; skipping duplicate call", user_id)
            return
        s["_advancing"] = True

        try:
            current_q = s.get("current_q_index", 0)
            if s.get("index", 0) <= current_q:
                s["index"] = current_q + 1
                logger.debug("ADVANCE: incremented index for user %s to %s", user_id, s["index"])
            else:
                logger.debug("ADVANCE: index already advanced for user %s (index=%s current_q=%s)", user_id, s["index"], current_q)

            s["transitioned"] = False
            s["in_question"] = False
            s["skip_disabled"] = False
        finally:
            s["_advancing"] = False

    s = sessions.get(user_id)
    if not s:
        return

    if s["index"] >= len(s["questions"]):
        logger.info("ADVANCE: user %s completed quiz (index=%s total=%s)", user_id, s["index"], len(s["questions"]))
        await finish_quiz(context, user_id)
        return

    await asyncio.sleep(TRANSITION_DELAY)
    # Always send next question immediately to avoid perceived lag
    await send_question(context, user_id, immediate=True)


# ---------------- results & admin ----------------


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
    leaderboard_lines = []
    for i, r in enumerate(ranked, 1):
        m, sec = divmod(r["time"], 60)
        leaderboard_lines.append(f"{i}. {html.escape(str(r['name']))} — {r['score']} | {m}m {sec}s")
    leaderboard = "\n".join(leaderboard_lines) or "No completed attempts recorded yet."

    text = (
        "🏁 <b>Quiz Finished!</b>\n\n"
        f"📝 Attempted: {s['attempted']}/{total}\n"
        f"✅ Correct: {s['score']}\n"
        f"❌ Wrong: {s['wrong']}\n"
        f"⏭ Skipped: {skipped_total} (You: {skipped_user}; Timeout: {skipped_timeout}; System: {skipped_invalid})\n"
        f"🎯 Marks: {round(s['marks'],2)}\n"
        f"⏱ Time: {time_taken//60}m {time_taken%60}s\n\n"
        "🏆 <b>Daily Leaderboard (Top 10)</b>\n"
        f"{html.escape(leaderboard)}"
    )
    await context.bot.send_message(chat_id=user_id, text=text, parse_mode="HTML")

    if s["explanations"]:
        header = "📖 <b>Simple Explanations</b>\n\n"
        chunk = header
        for exp in s["explanations"]:
            q_no = exp.get("q_no")
            q_text = html.escape(exp.get("question", ""))
            expl_text = html.escape(exp.get("explanation", ""))
            formatted = f"<b>Q{q_no}.</b> {q_text}\n<b>📘Explanation:</b> {expl_text}"
            if len(chunk) + len(formatted) > 3800:
                await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="HTML")
                chunk = header
            chunk += formatted + "\n\n"
        if chunk.strip() != header.strip():
            await context.bot.send_message(chat_id=user_id, text=chunk, parse_mode="HTML")

    try:
        if s.get("timeout_handle"):
            s["timeout_handle"].cancel()
    except Exception:
        pass

    sessions.pop(user_id, None)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text or ""
    if contains_offensive(text):
        await update.message.reply_text("❌ Please maintain respectful language. Send Hi to start the QUIZ.")
        return
    await send_greeting(context, update.effective_user.id, update.effective_user.first_name)


async def admin_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    finished_users = set(daily_scores.keys())
    in_progress_attempted = {uid for uid, s in sessions.items() if s.get("attempted", 0) > 0}
    unique_attempted = finished_users | in_progress_attempted
    attempted_count = len(unique_attempted)
    ranked = sorted(daily_scores.values(), key=lambda x: (-x["score"], x["time"]))[:10]

    lines = []
    for i, r in enumerate(ranked, 1):
        name = html.escape(str(r.get("name", "Unknown")))
        score = r.get("score", 0)
        t = r.get("time", 0)
        m, sec = divmod(int(t), 60)
        lines.append(f"{i}. {name} — {score} | {m}m {sec}s")
    leaderboard_text = "\n".join(lines) or "No completed attempts recorded yet."
    text = (
        f"<b>Admin Daily Leaderboard</b>\n\n"
        f"<b>Quiz date key:</b> {html.escape(str(current_quiz_date_key or 'N/A'))}\n"
        f"<b>Total attempted:</b> {attempted_count}\n\n"
        f"<b>Top 10</b>\n"
        f"{html.escape(leaderboard_text)}"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def admin_metrics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return
    text = (
        f"<b>Metrics</b>\n\n"
        f"skipped_questions: {metrics['skipped_questions']}\n"
        f"late_answers_treated_as_timeout: {metrics['late_answers_treated_as_timeout']}\n"
        f"preload_failures: {metrics['preload_failures']}\n"
        f"timeout_lateness_count: {metrics['timeout_lateness_count']}\n"
        f"user_skips: {metrics['user_skips']}\n"
        f"send_retries: {metrics['send_retries']}\n"
        f"send_failures: {metrics['send_failures']}\n"
        f"queue_rejections_user: {metrics['queue_rejections_user']}\n"
        f"queue_rejections_admin: {metrics['queue_rejections_admin']}\n"
    )
    await update.message.reply_text(text, parse_mode="HTML")


async def admin_force_preload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ You are not authorized to use this command.")
        return

    args = context.args or []

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
            "⚠️ Some users are in the final question window. Run /force_preload confirm to proceed anyway.",
            parse_mode="HTML"
        )
        return
    if sessions and "confirm" not in args and not risky:
        await update.message.reply_text(
            "⚠️ There are active quizzes in progress. Run /force_preload confirm to proceed anyway.",
            parse_mode="HTML"
        )
        return

    await update.message.reply_text("⏳ Fetching quiz CSV and initializing quiz date...", parse_mode="HTML")
    try:
        quiz_date_key = await preload_quiz_if_needed()
    except Exception:
        logger.exception("Force preload failed to fetch CSV")
        await update.message.reply_text("❌ Failed to fetch CSV. Check logs for details.", parse_mode="HTML")
        return
    if not quiz_date_key:
        await update.message.reply_text("⚠️ No valid quiz date found for the effective date.", parse_mode="HTML")
        return
    await update.message.reply_text(f"✅ Preloaded quiz for <b>{html.escape(quiz_date_key)}</b>.", parse_mode="HTML")


# ---------------- background tasks ----------------


async def _wait_until_next_cutoff():
    now = now_ist()
    today_cutoff = datetime.combine(now.date(), dtime(hour=QUIZ_SWITCH_HOUR, minute=0), IST)
    next_cutoff = today_cutoff if now < today_cutoff else today_cutoff + timedelta(days=1)
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
    logger.info("BACKGROUND: daily preload task started")
    while not _shutdown_event.is_set():
        wait_seconds = await _wait_until_next_cutoff()
        logger.info("Preload task sleeping for %.0f seconds", wait_seconds)
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


# ---------------- shutdown ----------------


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


# ---------------- unit tests (basic) ----------------


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


def run_unit_tests():
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(SessionHelpersTest)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


# ---------------- main ----------------


async def _post_init(application):
    try:
        me = await application.bot.get_me()
        logger.info("Bot started as @%s (id=%s)", me.username, me.id)
    except Exception:
        logger.exception("Failed to get bot identity; check BOT_TOKEN and network")

    await preload_quiz_if_needed()
    logger.info("BACKGROUND: starting send worker, preload and cleanup tasks")
    t_send = asyncio.create_task(_send_worker_loop(application))
    t_preload = asyncio.create_task(daily_preload_task(application))
    t_cleanup = asyncio.create_task(cleanup_sessions_task())
    _background_tasks.extend([t_send, t_preload, t_cleanup])


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

    app.post_init = _post_init

    # Ensure we receive messages, callback_query and poll_answer updates
    app.run_polling(allowed_updates=["message", "callback_query", "poll_answer"])


if __name__ == "__main__":
    main()
```
