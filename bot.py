"""
ربات تلگرام آگهی خرید و فروش - نسخه تک‌فایلی (همه‌چیز در یک فایل).
Python 3.12 + aiogram 3.x
آماده دیپلوی مستقیم روی Render، بدون نیاز به هیچ فایل یا پوشه دیگری
به‌جز requirements.txt، render.yaml و Procfile.

توکن ربات و شناسه ادمین فقط از متغیرهای محیطی BOT_TOKEN و ADMIN_ID خوانده می‌شوند؛
هیچ اطلاعات حساسی در این فایل هاردکد نشده است.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, Optional, Union

import aiosqlite
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import BaseFilter, Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import CallbackQuery, ErrorEvent, Message, TelegramObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# ============================================================================
# تنظیمات (از متغیرهای محیطی)
# ============================================================================
BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
_admin_id_raw: str = os.getenv("ADMIN_ID", "0")

if not BOT_TOKEN:
    raise RuntimeError(
        "BOT_TOKEN تنظیم نشده است. آن را در Environment Variables پنل Render وارد کنید."
    )

try:
    ADMIN_ID: int = int(_admin_id_raw)
except ValueError as exc:
    raise RuntimeError("ADMIN_ID باید یک عدد صحیح باشد.") from exc

if ADMIN_ID == 0:
    raise RuntimeError(
        "ADMIN_ID تنظیم نشده است. شناسه عددی تلگرام خودت را در Environment Variables وارد کن "
        "(برای گرفتنش به ربات @userinfobot پیام بده)."
    )

DB_PATH: str = os.getenv("DB_PATH", "database.db")
CHANNEL_ID: str = os.getenv("CHANNEL_ID", "")  # یوزرنیم (@channel) یا آیدی عددی کانال برای پست خودکار آگهی‌ها
PORT: int = int(os.getenv("PORT", "8080"))
WEBHOOK_HOST: str = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("WEBHOOK_URL", "")
WEBHOOK_PATH: str = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL: Optional[str] = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

TITLE_MIN_LEN, TITLE_MAX_LEN = 3, 100
DESCRIPTION_MIN_LEN, DESCRIPTION_MAX_LEN = 10, 1000
PRICE_MAX_LEN = 50
CONTACT_MIN_LEN, CONTACT_MAX_LEN = 3, 100
LATEST_LISTINGS_LIMIT = 10

# --- تنظیمات ضد اسپم ---
# اگه کاربر RATE_LIMIT_COUNT پیام/کلیک توی RATE_LIMIT_WINDOW ثانیه بفرسته،
# به مدت RATE_LIMIT_COOLDOWN ثانیه نادیده گرفته می‌شود (برای کاهش فشار روی سرور).
RATE_LIMIT_COUNT = 5
RATE_LIMIT_WINDOW = 10.0
RATE_LIMIT_COOLDOWN = 600.0  # ۱۰ دقیقه

DEFAULT_CATEGORIES = [
    "موبایل و تبلت", "لپ‌تاپ و کامپیوتر", "لوازم خانگی",
    "وسایل نقلیه", "املاک", "پوشاک", "سایر",
]

# ============================================================================
# لاگ‌گیری
# ============================================================================
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/errors.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


# ============================================================================
# دیتابیس (SQLite با aiosqlite)
# ============================================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY, username TEXT, full_name TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0, is_blocked INTEGER NOT NULL DEFAULT 0,
    joined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS listings (
    id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
    listing_type TEXT NOT NULL CHECK (listing_type IN ('buy','sell')),
    category_id INTEGER NOT NULL, title TEXT NOT NULL, description TEXT NOT NULL,
    price TEXT NOT NULL, contact_info TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','approved','rejected')),
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (category_id) REFERENCES categories (id)
);
CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT, listing_id INTEGER NOT NULL,
    reporter_id INTEGER NOT NULL, reason TEXT NOT NULL, created_at TEXT NOT NULL,
    FOREIGN KEY (listing_id) REFERENCES listings (id)
);
CREATE TABLE IF NOT EXISTS banned_words (
    id INTEGER PRIMARY KEY AUTOINCREMENT, word TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_listings_status ON listings (status);
CREATE INDEX IF NOT EXISTS idx_listings_user ON listings (user_id);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self.conn = await aiosqlite.connect(self.path)
        self.conn.row_factory = aiosqlite.Row
        await self.conn.execute("PRAGMA foreign_keys = ON;")
        await self.conn.commit()

    async def close(self) -> None:
        if self.conn is not None:
            await self.conn.close()
            self.conn = None

    async def init_db(self) -> None:
        assert self.conn is not None
        await self.conn.executescript(SCHEMA)
        await self.conn.commit()
        for name in DEFAULT_CATEGORIES:
            await self.conn.execute("INSERT OR IGNORE INTO categories (name) VALUES (?);", (name,))
        await self.conn.commit()
        logger.info("دیتابیس مقداردهی اولیه شد.")

    async def add_user(self, user_id: int, username: str | None, full_name: str) -> None:
        assert self.conn is not None
        await self.conn.execute(
            """INSERT INTO users (user_id, username, full_name, joined_at) VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id) DO UPDATE SET username=excluded.username, full_name=excluded.full_name;""",
            (user_id, username, full_name, _now()),
        )
        await self.conn.commit()

    async def get_user(self, user_id: int):
        assert self.conn is not None
        cur = await self.conn.execute("SELECT * FROM users WHERE user_id = ?;", (user_id,))
        return await cur.fetchone()

    async def is_blocked(self, user_id: int) -> bool:
        user = await self.get_user(user_id)
        return bool(user["is_blocked"]) if user else False

    async def block_user(self, user_id: int) -> None:
        assert self.conn is not None
        await self.conn.execute("UPDATE users SET is_blocked = 1 WHERE user_id = ?;", (user_id,))
        await self.conn.commit()

    async def unblock_user(self, user_id: int) -> None:
        assert self.conn is not None
        await self.conn.execute("UPDATE users SET is_blocked = 0 WHERE user_id = ?;", (user_id,))
        await self.conn.commit()

    async def count_users(self) -> int:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM users;")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def count_blocked_users(self) -> int:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM users WHERE is_blocked = 1;")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def get_all_user_ids(self) -> list[int]:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT user_id FROM users WHERE is_blocked = 0;")
        rows = await cur.fetchall()
        return [r["user_id"] for r in rows]

    async def get_categories(self):
        assert self.conn is not None
        cur = await self.conn.execute("SELECT * FROM categories ORDER BY id;")
        return list(await cur.fetchall())

    async def create_listing(self, user_id, listing_type, category_id, title, description, price, contact_info) -> int:
        assert self.conn is not None
        cur = await self.conn.execute(
            """INSERT INTO listings (user_id, listing_type, category_id, title, description, price, contact_info, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?);""",
            (user_id, listing_type, category_id, title, description, price, contact_info, _now()),
        )
        await self.conn.commit()
        assert cur.lastrowid is not None
        return cur.lastrowid

    async def get_listing(self, listing_id: int) -> Optional[dict[str, Any]]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """SELECT l.*, c.name AS category_name, u.full_name AS seller_name, u.username AS seller_username
               FROM listings l JOIN categories c ON c.id = l.category_id JOIN users u ON u.user_id = l.user_id
               WHERE l.id = ?;""",
            (listing_id,),
        )
        row = await cur.fetchone()
        return dict(row) if row else None

    async def get_latest_listings(self, limit: int = 10) -> list[dict[str, Any]]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """SELECT l.*, c.name AS category_name, u.full_name AS seller_name, u.username AS seller_username
               FROM listings l JOIN categories c ON c.id = l.category_id JOIN users u ON u.user_id = l.user_id
               WHERE l.status = 'approved' ORDER BY l.created_at DESC LIMIT ?;""",
            (limit,),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def search_listings(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        assert self.conn is not None
        like_query = f"%{query}%"
        cur = await self.conn.execute(
            """SELECT l.*, c.name AS category_name, u.full_name AS seller_name, u.username AS seller_username
               FROM listings l JOIN categories c ON c.id = l.category_id JOIN users u ON u.user_id = l.user_id
               WHERE l.status = 'approved' AND (l.title LIKE ? OR l.description LIKE ?)
               ORDER BY l.created_at DESC LIMIT ?;""",
            (like_query, like_query, limit),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_pending_listings(self) -> list[dict[str, Any]]:
        assert self.conn is not None
        cur = await self.conn.execute(
            """SELECT l.*, c.name AS category_name, u.full_name AS seller_name, u.username AS seller_username
               FROM listings l JOIN categories c ON c.id = l.category_id JOIN users u ON u.user_id = l.user_id
               WHERE l.status = 'pending' ORDER BY l.created_at ASC;"""
        )
        return [dict(r) for r in await cur.fetchall()]

    async def delete_listing(self, listing_id: int, requester_id: int) -> bool:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT user_id FROM listings WHERE id = ?;", (listing_id,))
        row = await cur.fetchone()
        if row is None or row["user_id"] != requester_id:
            return False
        await self.conn.execute("DELETE FROM listings WHERE id = ?;", (listing_id,))
        await self.conn.commit()
        return True

    async def approve_listing(self, listing_id: int) -> None:
        assert self.conn is not None
        await self.conn.execute("UPDATE listings SET status = 'approved' WHERE id = ?;", (listing_id,))
        await self.conn.commit()

    async def reject_listing(self, listing_id: int) -> None:
        assert self.conn is not None
        await self.conn.execute("UPDATE listings SET status = 'rejected' WHERE id = ?;", (listing_id,))
        await self.conn.commit()

    async def count_listings(self) -> int:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM listings;")
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def count_listings_by_status(self, status: str) -> int:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT COUNT(*) AS c FROM listings WHERE status = ?;", (status,))
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def add_report(self, listing_id: int, reporter_id: int, reason: str) -> None:
        assert self.conn is not None
        await self.conn.execute(
            "INSERT INTO reports (listing_id, reporter_id, reason, created_at) VALUES (?, ?, ?, ?);",
            (listing_id, reporter_id, reason, _now()),
        )
        await self.conn.commit()

    async def get_banned_words(self) -> list[str]:
        assert self.conn is not None
        cur = await self.conn.execute("SELECT word FROM banned_words;")
        return [r["word"] for r in await cur.fetchall()]

    async def add_banned_word(self, word: str) -> None:
        assert self.conn is not None
        await self.conn.execute("INSERT OR IGNORE INTO banned_words (word) VALUES (?);", (word.strip(),))
        await self.conn.commit()


db = Database(DB_PATH)


# ============================================================================
# FSM States
# ============================================================================
class ListingForm(StatesGroup):
    choosing_category = State()
    entering_title = State()
    entering_description = State()
    entering_price = State()
    entering_contact = State()
    confirming = State()


class SearchForm(StatesGroup):
    entering_query = State()


class ReportForm(StatesGroup):
    entering_reason = State()


class AdminForm(StatesGroup):
    broadcasting = State()
    banning_user = State()
    unbanning_user = State()
    adding_word = State()


# ============================================================================
# فیلترها و توابع کمکی
# ============================================================================
class IsAdmin(BaseFilter):
    def __init__(self, admin_id: int):
        self.admin_id = admin_id

    async def __call__(self, event: Union[Message, CallbackQuery]) -> bool:
        return event.from_user is not None and event.from_user.id == self.admin_id


def contains_bad_words(text: str, banned_words: Iterable[str]) -> bool:
    normalized = text.lower()
    return any(w.lower() in normalized for w in banned_words if w.strip())


class AntiSpamMiddleware(BaseMiddleware):
    """
    ضد اسپم سبک و کاملاً در حافظه (بدون تماس با دیتابیس یا سرویس بیرونی).

    اگر کاربر بیش از RATE_LIMIT_COUNT پیام/کلیک در عرض RATE_LIMIT_WINDOW ثانیه بفرستد،
    به مدت RATE_LIMIT_COOLDOWN ثانیه به‌طور کامل نادیده گرفته می‌شود (نه پردازش هندلر،
    نه پاسخ تکراری) تا فشار روی سرور و روی API تلگرام کم شود. یک‌بار در لحظه‌ی مسدود شدن
    به کاربر اطلاع داده می‌شود، بعد از آن تا پایان cooldown کاملاً سکوت می‌شود.

    ادمین اصلی (ADMIN_ID) از این محدودیت معاف است تا مدیریت/broadcast مختل نشود.
    """

    def __init__(self) -> None:
        self._timestamps: dict[int, deque] = defaultdict(deque)
        self._muted_until: dict[int, float] = {}
        self._warned: set[int] = set()

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = getattr(event, "from_user", None)
        if user is None or user.id == ADMIN_ID:
            return await handler(event, data)

        user_id = user.id
        now = time.monotonic()

        muted_until = self._muted_until.get(user_id)
        if muted_until is not None:
            if now < muted_until:
                # کاربر مسدود موقت است؛ کاملاً نادیده گرفته می‌شود (بدون فراخوانی هندلر).
                if isinstance(event, CallbackQuery):
                    try:
                        await event.answer()
                    except Exception:
                        pass
                return None
            # زمان استراحت تمام شده، وضعیت پاک می‌شود.
            del self._muted_until[user_id]
            self._warned.discard(user_id)
            self._timestamps[user_id].clear()

        window = self._timestamps[user_id]
        window.append(now)
        while window and now - window[0] > RATE_LIMIT_WINDOW:
            window.popleft()

        if len(window) > RATE_LIMIT_COUNT:
            self._muted_until[user_id] = now + RATE_LIMIT_COOLDOWN
            window.clear()
            if user_id not in self._warned:
                self._warned.add(user_id)
                warning_text = (
                    "🚫 به‌دلیل ارسال پیام زیاد در زمان کوتاه، به مدت ۱۰ دقیقه امکان "
                    "استفاده از ربات موقتاً غیرفعال شد. لطفاً بعداً دوباره امتحان کنید."
                )
                try:
                    if isinstance(event, Message):
                        await event.answer(warning_text)
                    elif isinstance(event, CallbackQuery):
                        await event.answer(warning_text, show_alert=True)
                except Exception:
                    logger.exception("ارسال هشدار ضد اسپم با خطا مواجه شد.")
            return None

        return await handler(event, data)


# ============================================================================
# کیبوردها
# ============================================================================
def main_menu_kb(is_admin: bool = False):
    b = InlineKeyboardBuilder()
    b.button(text="💰 ثبت آگهی فروش", callback_data="menu:sell")
    b.button(text="🛒 ثبت آگهی خرید", callback_data="menu:buy")
    b.button(text="🔍 جستجوی آگهی", callback_data="menu:search")
    b.button(text="🆕 آخرین آگهی‌ها", callback_data="menu:latest")
    if is_admin:
        b.button(text="🛠 پنل مدیریت", callback_data="menu:admin")
        b.adjust(2, 2, 1)
    else:
        b.adjust(2, 2)
    return b.as_markup()


def categories_kb(categories):
    b = InlineKeyboardBuilder()
    for cat in categories:
        b.button(text=cat["name"], callback_data=f"cat:{cat['id']}")
    b.button(text="🔙 بازگشت", callback_data="menu:back")
    b.adjust(2)
    return b.as_markup()


def cancel_kb():
    b = InlineKeyboardBuilder()
    b.button(text="🚫 انصراف", callback_data="cancel")
    return b.as_markup()


def confirm_listing_kb():
    b = InlineKeyboardBuilder()
    b.button(text="✅ ثبت آگهی", callback_data="listing:confirm")
    b.button(text="🚫 انصراف", callback_data="cancel")
    b.adjust(2)
    return b.as_markup()


def listing_detail_kb(listing_id: int, owner_id: int, viewer_id: int, seller_username: str | None):
    b = InlineKeyboardBuilder()
    if seller_username:
        b.button(text="📞 تماس با فروشنده", url=f"https://t.me/{seller_username}")
    b.button(text="🚩 گزارش تخلف", callback_data=f"report:{listing_id}")
    if viewer_id == owner_id:
        b.button(text="🗑 حذف آگهی", callback_data=f"listing:delete:{listing_id}")
    b.adjust(1)
    return b.as_markup()


def admin_panel_kb():
    b = InlineKeyboardBuilder()
    b.button(text="📊 آمار کاربران", callback_data="admin:stats_users")
    b.button(text="📦 آمار آگهی‌ها", callback_data="admin:stats_listings")
    b.button(text="✅ آگهی‌های در انتظار تأیید", callback_data="admin:pending")
    b.button(text="📢 ارسال پیام همگانی", callback_data="admin:broadcast")
    b.button(text="🚫 مسدود کردن کاربر", callback_data="admin:ban")
    b.button(text="♻️ رفع مسدودی کاربر", callback_data="admin:unban")
    b.button(text="✳️ افزودن کلمه فیلترشده", callback_data="admin:addword")
    b.button(text="🔙 بازگشت به منو", callback_data="menu:back")
    b.adjust(2, 2, 2, 1, 1)
    return b.as_markup()


def approve_reject_kb(listing_id: int):
    b = InlineKeyboardBuilder()
    b.button(text="✅ تأیید", callback_data=f"admin:approve:{listing_id}")
    b.button(text="❌ رد", callback_data=f"admin:reject:{listing_id}")
    b.adjust(2)
    return b.as_markup()


def format_listing(listing: dict[str, Any]) -> str:
    listing_type_fa = "فروش 💰" if listing["listing_type"] == "sell" else "خرید 🛒"
    return (
        f"📌 {listing['title']}\n"
        f"🏷 نوع: {listing_type_fa}\n"
        f"📂 دسته: {listing['category_name']}\n"
        f"📝 {listing['description']}\n"
        f"💵 قیمت: {listing['price']}\n"
        f"📞 تماس: {listing['contact_info']}\n"
        f"👤 فروشنده: {listing['seller_name']}\n"
        f"🕒 تاریخ ثبت: {listing['created_at'][:16].replace('T', ' ')}\n"
        f"🆔 شناسه آگهی: {listing['id']}"
    )


# ============================================================================
# روتر: شروع کار / منوی اصلی
# ============================================================================
start_router = Router(name="start")


@start_router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user = message.from_user
    if user is None:
        return
    if await db.is_blocked(user.id):
        await message.answer("⛔️ دسترسی شما به این ربات مسدود شده است.")
        return
    await db.add_user(user.id, user.username, user.full_name)
    is_admin = user.id == ADMIN_ID
    await message.answer(
        f"👋 سلام {user.full_name} عزیز!\n\nبه ربات آگهی خرید و فروش خوش آمدید 🛍\n"
        "از منوی زیر یکی از گزینه‌ها را انتخاب کنید:",
        reply_markup=main_menu_kb(is_admin=is_admin),
    )


@start_router.callback_query(F.data == "menu:back")
async def back_to_menu(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    if callback.message:
        try:
            await callback.message.edit_text("🏠 منوی اصلی:", reply_markup=main_menu_kb(is_admin=is_admin))
        except Exception:
            await callback.message.answer("🏠 منوی اصلی:", reply_markup=main_menu_kb(is_admin=is_admin))
    await callback.answer()


@start_router.callback_query(F.data == "cancel")
async def cancel_action(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    is_admin = callback.from_user.id == ADMIN_ID
    text = "❌ عملیات لغو شد.\n\n🏠 منوی اصلی:"
    if callback.message:
        try:
            await callback.message.edit_text(text, reply_markup=main_menu_kb(is_admin=is_admin))
        except Exception:
            await callback.message.answer(text, reply_markup=main_menu_kb(is_admin=is_admin))
    await callback.answer()


# ============================================================================
# روتر: ثبت آگهی خرید/فروش
# ============================================================================
listings_router = Router(name="listings")


@listings_router.callback_query(F.data.in_({"menu:sell", "menu:buy"}))
async def start_listing(callback: CallbackQuery, state: FSMContext) -> None:
    if await db.is_blocked(callback.from_user.id):
        await callback.answer("⛔️ شما مسدود شده‌اید.", show_alert=True)
        return
    listing_type = "sell" if callback.data == "menu:sell" else "buy"
    await state.update_data(listing_type=listing_type)
    categories = await db.get_categories()
    if callback.message:
        await callback.message.edit_text("📂 لطفاً دسته‌بندی آگهی را انتخاب کنید:", reply_markup=categories_kb(categories))
    await state.set_state(ListingForm.choosing_category)
    await callback.answer()


@listings_router.callback_query(ListingForm.choosing_category, F.data.startswith("cat:"))
async def choose_category(callback: CallbackQuery, state: FSMContext) -> None:
    category_id = int(callback.data.split(":")[1])
    await state.update_data(category_id=category_id)
    if callback.message:
        await callback.message.edit_text("📝 عنوان آگهی را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_title)
    await callback.answer()


@listings_router.message(ListingForm.entering_title, F.text)
async def enter_title(message: Message, state: FSMContext) -> None:
    title = message.text.strip()
    if not (TITLE_MIN_LEN <= len(title) <= TITLE_MAX_LEN):
        await message.answer(f"⚠️ عنوان باید بین {TITLE_MIN_LEN} تا {TITLE_MAX_LEN} کاراکتر باشد. دوباره وارد کنید:")
        return
    banned_words = await db.get_banned_words()
    if contains_bad_words(title, banned_words):
        await message.answer("⚠️ عنوان شامل کلمات غیرمجاز است. دوباره وارد کنید:")
        return
    await state.update_data(title=title)
    await message.answer("🖊 توضیحات کامل آگهی را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_description)


@listings_router.message(ListingForm.entering_description, F.text)
async def enter_description(message: Message, state: FSMContext) -> None:
    description = message.text.strip()
    if not (DESCRIPTION_MIN_LEN <= len(description) <= DESCRIPTION_MAX_LEN):
        await message.answer(f"⚠️ توضیحات باید بین {DESCRIPTION_MIN_LEN} تا {DESCRIPTION_MAX_LEN} کاراکتر باشد. دوباره وارد کنید:")
        return
    banned_words = await db.get_banned_words()
    if contains_bad_words(description, banned_words):
        await message.answer("⚠️ توضیحات شامل کلمات غیرمجاز است. دوباره وارد کنید:")
        return
    await state.update_data(description=description)
    await message.answer("💵 قیمت را وارد کنید (فقط عدد یا عبارت «توافقی»):", reply_markup=cancel_kb())
    await state.set_state(ListingForm.entering_price)


@listings_router.message(ListingForm.entering_price, F.text)
async def enter_price(message: Message, state: FSMContext) -> None:
    price = message.text.strip()
    if not (1 <= len(price) <= PRICE_MAX_LEN):
        await message.answer("⚠️ قیمت واردشده نامعتبر است. دوباره وارد کنید:")
        return
    await state.update_data(price=price)
    await message.answer(
        "📞 شماره تماس یا آیدی تلگرام خود را برای درج در آگهی وارد کنید:\n(مثال: 09121234567 یا @username)",
        reply_markup=cancel_kb(),
    )
    await state.set_state(ListingForm.entering_contact)


@listings_router.message(ListingForm.entering_contact, F.text)
async def enter_contact(message: Message, state: FSMContext) -> None:
    contact = message.text.strip()
    if not (CONTACT_MIN_LEN <= len(contact) <= CONTACT_MAX_LEN):
        await message.answer("⚠️ اطلاعات تماس نامعتبر است. دوباره وارد کنید:")
        return
    await state.update_data(contact_info=contact)
    data = await state.get_data()
    listing_type_fa = "فروش 💰" if data["listing_type"] == "sell" else "خرید 🛒"
    preview = (
        f"📋 پیش‌نمایش آگهی شما:\n\n🏷 نوع: {listing_type_fa}\n📌 عنوان: {data['title']}\n"
        f"📝 توضیحات: {data['description']}\n💵 قیمت: {data['price']}\n📞 تماس: {contact}\n\n"
        "آیا مایل به ثبت این آگهی هستید؟"
    )
    await message.answer(preview, reply_markup=confirm_listing_kb())
    await state.set_state(ListingForm.confirming)


@listings_router.callback_query(ListingForm.confirming, F.data == "listing:confirm")
async def confirm_listing(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    user = callback.from_user
    listing_id = await db.create_listing(
        user_id=user.id, listing_type=data["listing_type"], category_id=data["category_id"],
        title=data["title"], description=data["description"], price=data["price"], contact_info=data["contact_info"],
    )
    # آگهی به‌صورت خودکار تأیید می‌شود (بدون نیاز به بررسی دستی ادمین) تا فوراً در کانال منتشر شود.
    await db.approve_listing(listing_id)
    await state.clear()
    is_admin = user.id == ADMIN_ID
    if callback.message:
        await callback.message.edit_text("✅ آگهی شما با موفقیت ثبت و منتشر شد.")
        await callback.message.answer("🏠 منوی اصلی:", reply_markup=main_menu_kb(is_admin=is_admin))
    try:
        listing = await db.get_listing(listing_id)
        listing_type_fa = "فروش 💰" if listing["listing_type"] == "sell" else "خرید 🛒"

        # اطلاع‌رسانی به ادمین (فقط جهت آگاهی، دیگر نیازی به تأیید دستی نیست)
        admin_notify_text = (
            f"🆕 آگهی جدید ثبت و منتشر شد:\n\n🆔 شناسه: {listing_id}\n"
            f"👤 کاربر: {user.full_name} (@{user.username or '---'})\n🏷 نوع: {listing_type_fa}\n"
            f"📂 دسته: {listing['category_name']}\n📌 عنوان: {listing['title']}"
        )
        await callback.bot.send_message(ADMIN_ID, admin_notify_text)

        # انتشار خودکار آگهی در کانال (در صورت تنظیم بودن CHANNEL_ID)
        if CHANNEL_ID:
            channel_text = (
                f"🏷 {listing_type_fa}\n\n"
                f"📌 {listing['title']}\n"
                f"📂 دسته: {listing['category_name']}\n"
                f"📝 {listing['description']}\n"
                f"💵 قیمت: {listing['price']}\n"
                f"📞 تماس: {listing['contact_info']}"
            )
            await callback.bot.send_message(CHANNEL_ID, channel_text)
    except Exception:
        logger.exception("ارسال اعلان آگهی جدید به ادمین/کانال با خطا مواجه شد.")
    await callback.answer()


@listings_router.callback_query(F.data.startswith("listing:delete:"))
async def delete_listing(callback: CallbackQuery) -> None:
    listing_id = int(callback.data.split(":")[2])
    success = await db.delete_listing(listing_id, callback.from_user.id)
    if success:
        await callback.answer("🗑 آگهی با موفقیت حذف شد.", show_alert=True)
        if callback.message:
            try:
                await callback.message.delete()
            except Exception:
                pass
    else:
        await callback.answer("⚠️ شما اجازه حذف این آگهی را ندارید.", show_alert=True)


# ============================================================================
# روتر: مرور آگهی‌ها، جستجو، گزارش تخلف
# ============================================================================
browse_router = Router(name="browse")


@browse_router.callback_query(F.data == "menu:latest")
async def latest_listings(callback: CallbackQuery) -> None:
    listings = await db.get_latest_listings(limit=LATEST_LISTINGS_LIMIT)
    if not listings:
        await callback.answer("فعلاً هیچ آگهی تأییدشده‌ای وجود ندارد.", show_alert=True)
        return
    await callback.answer()
    for listing in listings:
        kb = listing_detail_kb(listing["id"], listing["user_id"], callback.from_user.id, listing["seller_username"])
        if callback.message:
            await callback.message.answer(format_listing(listing), reply_markup=kb)


@browse_router.callback_query(F.data == "menu:search")
async def start_search(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message:
        await callback.message.edit_text("🔍 عبارت مورد نظر برای جستجو در آگهی‌ها را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(SearchForm.entering_query)
    await callback.answer()


@browse_router.message(SearchForm.entering_query, F.text)
async def do_search(message: Message, state: FSMContext) -> None:
    query = message.text.strip()
    await state.clear()
    if len(query) < 2:
        await message.answer("⚠️ عبارت جستجو باید حداقل ۲ کاراکتر باشد.")
        return
    results = await db.search_listings(query)
    if not results:
        await message.answer("❌ آگهی‌ای مطابق با جستجوی شما پیدا نشد.")
        return
    await message.answer(f"🔎 {len(results)} نتیجه یافت شد:")
    for listing in results:
        kb = listing_detail_kb(listing["id"], listing["user_id"], message.from_user.id, listing["seller_username"])
        await message.answer(format_listing(listing), reply_markup=kb)


@browse_router.callback_query(F.data.startswith("report:"))
async def report_listing_start(callback: CallbackQuery, state: FSMContext) -> None:
    listing_id = int(callback.data.split(":")[1])
    await state.update_data(report_listing_id=listing_id)
    if callback.message:
        await callback.message.answer("🚩 لطفاً دلیل گزارش تخلف را بنویسید:", reply_markup=cancel_kb())
    await state.set_state(ReportForm.entering_reason)
    await callback.answer()


@browse_router.message(ReportForm.entering_reason, F.text)
async def report_listing_submit(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    listing_id = data.get("report_listing_id")
    reason = message.text.strip()
    await state.clear()
    if listing_id is None:
        await message.answer("⚠️ خطایی رخ داد، دوباره تلاش کنید.")
        return
    await db.add_report(listing_id, message.from_user.id, reason)
    await message.answer("✅ گزارش شما با موفقیت ثبت شد و توسط ادمین بررسی خواهد شد.")
    try:
        await message.bot.send_message(
            ADMIN_ID,
            f"🚩 گزارش تخلف جدید برای آگهی #{listing_id}:\n\n"
            f"👤 گزارش‌دهنده: {message.from_user.full_name} (@{message.from_user.username or '---'})\n📝 دلیل: {reason}",
        )
    except Exception:
        logger.exception("ارسال اعلان گزارش تخلف به ادمین با خطا مواجه شد.")


# ============================================================================
# روتر: پنل مدیریت (فقط ADMIN_ID)
# ============================================================================
admin_router = Router(name="admin")
admin_router.message.filter(IsAdmin(ADMIN_ID))
admin_router.callback_query.filter(IsAdmin(ADMIN_ID))


@admin_router.message(Command("admin"))
async def admin_command(message: Message) -> None:
    await message.answer("🛠 پنل مدیریت:", reply_markup=admin_panel_kb())


@admin_router.callback_query(F.data == "menu:admin")
async def open_admin_panel(callback: CallbackQuery) -> None:
    if callback.message:
        await callback.message.edit_text("🛠 پنل مدیریت:", reply_markup=admin_panel_kb())
    await callback.answer()


@admin_router.callback_query(F.data == "admin:stats_users")
async def stats_users(callback: CallbackQuery) -> None:
    total = await db.count_users()
    blocked = await db.count_blocked_users()
    await callback.answer()
    if callback.message:
        await callback.message.answer(f"👥 آمار کاربران:\n\nکل کاربران: {total}\nکاربران مسدودشده: {blocked}")


@admin_router.callback_query(F.data == "admin:stats_listings")
async def stats_listings(callback: CallbackQuery) -> None:
    total = await db.count_listings()
    pending = await db.count_listings_by_status("pending")
    approved = await db.count_listings_by_status("approved")
    rejected = await db.count_listings_by_status("rejected")
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"📦 آمار آگهی‌ها:\n\nکل آگهی‌ها: {total}\n⏳ در انتظار تأیید: {pending}\n"
            f"✅ تأییدشده: {approved}\n❌ ردشده: {rejected}"
        )


@admin_router.callback_query(F.data == "admin:pending")
async def pending_listings(callback: CallbackQuery) -> None:
    listings = await db.get_pending_listings()
    await callback.answer()
    if not listings:
        if callback.message:
            await callback.message.answer("✅ در حال حاضر آگهی در انتظار تأیید وجود ندارد.")
        return
    for listing in listings:
        listing_type_fa = "فروش 💰" if listing["listing_type"] == "sell" else "خرید 🛒"
        text = (
            f"🆔 {listing['id']} | {listing_type_fa}\n"
            f"👤 {listing['seller_name']} (@{listing['seller_username'] or '---'})\n"
            f"📂 دسته: {listing['category_name']}\n📌 عنوان: {listing['title']}\n"
            f"📝 توضیحات: {listing['description']}\n💵 قیمت: {listing['price']}"
        )
        if callback.message:
            await callback.message.answer(text, reply_markup=approve_reject_kb(listing["id"]))


@admin_router.callback_query(F.data.startswith("admin:approve:"))
async def approve_listing(callback: CallbackQuery) -> None:
    listing_id = int(callback.data.split(":")[2])
    listing = await db.get_listing(listing_id)
    await db.approve_listing(listing_id)
    await callback.answer("✅ آگهی تأیید شد.")
    if callback.message and callback.message.text:
        try:
            await callback.message.edit_text(callback.message.text + "\n\n✅ تأیید شد")
        except Exception:
            pass
    if listing:
        try:
            await callback.bot.send_message(listing["user_id"], f"✅ آگهی «{listing['title']}» شما تأیید و منتشر شد.")
        except Exception:
            pass


@admin_router.callback_query(F.data.startswith("admin:reject:"))
async def reject_listing(callback: CallbackQuery) -> None:
    listing_id = int(callback.data.split(":")[2])
    listing = await db.get_listing(listing_id)
    await db.reject_listing(listing_id)
    await callback.answer("❌ آگهی رد شد.")
    if callback.message and callback.message.text:
        try:
            await callback.message.edit_text(callback.message.text + "\n\n❌ رد شد")
        except Exception:
            pass
    if listing:
        try:
            await callback.bot.send_message(listing["user_id"], f"❌ آگهی «{listing['title']}» شما توسط ادمین رد شد.")
        except Exception:
            pass


@admin_router.callback_query(F.data == "admin:broadcast")
async def start_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message:
        await callback.message.answer("📢 متن پیام همگانی را ارسال کنید:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.broadcasting)
    await callback.answer()


@admin_router.message(AdminForm.broadcasting)
async def do_broadcast(message: Message, state: FSMContext) -> None:
    await state.clear()
    user_ids = await db.get_all_user_ids()
    status_msg = await message.answer(f"⏳ در حال ارسال پیام به {len(user_ids)} کاربر...")
    sent, failed = 0, 0
    for user_id in user_ids:
        try:
            await message.copy_to(user_id)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(f"✅ ارسال پیام همگانی به پایان رسید.\n\nموفق: {sent}\nناموفق: {failed}")


@admin_router.callback_query(F.data == "admin:ban")
async def start_ban(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message:
        await callback.message.answer("🚫 شناسه عددی (User ID) کاربر موردنظر برای مسدودسازی را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.banning_user)
    await callback.answer()


@admin_router.message(AdminForm.banning_user, F.text)
async def do_ban(message: Message, state: FSMContext) -> None:
    await state.clear()
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("⚠️ شناسه واردشده معتبر نیست. باید یک عدد باشد.")
        return
    await db.block_user(int(text))
    await message.answer(f"🚫 کاربر با شناسه {text} مسدود شد.")


@admin_router.callback_query(F.data == "admin:unban")
async def start_unban(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message:
        await callback.message.answer("♻️ شناسه عددی (User ID) کاربر موردنظر برای رفع مسدودی را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.unbanning_user)
    await callback.answer()


@admin_router.message(AdminForm.unbanning_user, F.text)
async def do_unban(message: Message, state: FSMContext) -> None:
    await state.clear()
    text = message.text.strip()
    if not text.isdigit():
        await message.answer("⚠️ شناسه واردشده معتبر نیست. باید یک عدد باشد.")
        return
    await db.unblock_user(int(text))
    await message.answer(f"♻️ کاربر با شناسه {text} از حالت مسدودی خارج شد.")


@admin_router.callback_query(F.data == "admin:addword")
async def start_addword(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.message:
        await callback.message.answer("✳️ کلمه‌ای که باید در آگهی‌ها فیلتر شود را وارد کنید:", reply_markup=cancel_kb())
    await state.set_state(AdminForm.adding_word)
    await callback.answer()


@admin_router.message(AdminForm.adding_word, F.text)
async def do_addword(message: Message, state: FSMContext) -> None:
    await state.clear()
    word = message.text.strip()
    if not word:
        await message.answer("⚠️ ورودی نامعتبر است.")
        return
    await db.add_banned_word(word)
    await message.answer(f"✅ کلمه «{word}» به لیست فیلتر اضافه شد.")


# ============================================================================
# روتر: مدیریت سراسری خطا (جلوگیری از کرش ربات)
# ============================================================================
errors_router = Router(name="errors")


@errors_router.errors()
async def global_error_handler(event: ErrorEvent) -> bool:
    logger.exception("خطای پیش‌بینی‌نشده هنگام پردازش آپدیت رخ داد: %s", event.exception)
    update = event.update
    try:
        if update.message:
            await update.message.answer("⚠️ متأسفانه خطایی رخ داد. لطفاً دوباره تلاش کنید یا با /start شروع مجدد کنید.")
        elif update.callback_query:
            await update.callback_query.answer("⚠️ خطایی رخ داد. لطفاً دوباره تلاش کنید.", show_alert=True)
    except Exception:
        logger.exception("ارسال پیام خطا به کاربر هم با شکست مواجه شد.")
    return True


# ============================================================================
# راه‌اندازی Bot / Dispatcher / Webhook
# ============================================================================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())

_anti_spam = AntiSpamMiddleware()
dp.message.outer_middleware(_anti_spam)
dp.callback_query.outer_middleware(_anti_spam)

dp.include_router(admin_router)
dp.include_router(start_router)
dp.include_router(listings_router)
dp.include_router(browse_router)
dp.include_router(errors_router)


async def on_startup(bot: Bot) -> None:
    await db.connect()
    await db.init_db()
    if WEBHOOK_URL:
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        logger.info("Webhook تنظیم شد: %s", WEBHOOK_URL)
    else:
        logger.warning("RENDER_EXTERNAL_URL/WEBHOOK_URL تنظیم نشده؛ ربات در حالت Polling اجرا می‌شود.")
    logger.info("ربات با موفقیت راه‌اندازی شد.")


async def on_shutdown(bot: Bot) -> None:
    if WEBHOOK_URL:
        await bot.delete_webhook()
    await db.close()
    logger.info("ربات متوقف شد.")


async def health_check(request: web.Request) -> web.Response:
    return web.Response(text="Bot is running ✅")


def run_webhook() -> None:
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    app = web.Application()
    app.router.add_get("/", health_check)
    handler = SimpleRequestHandler(dispatcher=dp, bot=bot)
    handler.register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    logger.info("در حال اجرا روی حالت Webhook | پورت: %s", PORT)
    web.run_app(app, host="0.0.0.0", port=PORT)


async def run_polling() -> None:
    await db.connect()
    await db.init_db()
    logger.info("در حال اجرا روی حالت Polling (توسعه محلی).")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await db.close()


def main() -> None:
    if WEBHOOK_URL:
        run_webhook()
    else:
        asyncio.run(run_polling())


if __name__ == "__main__":
    main()
