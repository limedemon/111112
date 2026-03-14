import os
import sys
import subprocess
import asyncio
import logging
import random
import sqlite3
import time
from datetime import datetime
from io import BytesIO

# === АВТОМАТИЧЕСКАЯ УСТАНОВКА БИБЛИОТЕК ===
def install_requirements():
    requirements = ["aiogram==3.4.1", "Pillow==10.2.0"]
    installed_libs = []
    
    try:
        import aiogram
        installed_libs.append("aiogram")
    except ImportError: pass
    
    try:
        from PIL import Image
        installed_libs.append("Pillow")
    except ImportError: pass

    needed_install = [lib for lib in requirements if lib.split('==')[0] not in installed_libs]
    
    if needed_install:
        print(f"Не найдены необходимые библиотеки: {', '.join(needed_install)}. Начинаю автоматическую установку...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install"] + needed_install)
            print("Установка успешно завершена! Загружаю бота...")
        except Exception as e:
            print(f"Критическая ошибка при установке библиотек: {e}")
            sys.exit(1)

install_requirements()

from aiogram import Bot, Dispatcher, F, BaseMiddleware, Router
from aiogram.types import (
    Message, CallbackQuery, ReplyKeyboardMarkup, 
    KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, BufferedInputFile
)
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from PIL import Image, ImageOps, ImageDraw

# === КОНФИГУРАЦИЯ СЛОТОВ ===
TOKEN = "8770327861:AAEPgHBTjpoqhLsl8f8KMoyzo1xrtWjeyrM"

try:
    MAIN_BOT_ID = int(TOKEN.split(":")[0])
except ValueError:
    print("Ошибка: Неверный формат токена.")
    sys.exit(1)

MAIN_ADMIN_ID = 1018561747
MAIN_CHANNEL = "@L1meYT"
COOLDOWN_SECONDS = 600  # 10 минут

UPDATE_LOG_TEXT = (
    "<b>🚀 Глобальное обновление системы!\n\n"
    "• 🧹 МЕНЮ КОМАНД: Обновлена кнопка Menu, удалены несуществующие команды.\n"
    "• 👨‍💻 АДМИНАМ: Добавлены команды /gomerchant и /closemerchant для ручного управления Торговцем.\n"
    "• 💰 ВОЗВРАЩЕНИЕ МОНЕТ: За победы в битвах выдаются монеты в зависимости от ваших кубков.\n"
    "• 🛒 ГЛОБАЛЬНЫЙ МЕРЧАНТ: Раз в час в чатах появляется Торговец с картами.\n"
    "• 🌍 МАТЧМЕЙКИНГ: Команда /pvpsearch для поиска случайных противников в ЛС.\n"
    "• 🏆 ТРОФЕИ И ТОП ИГРОКОВ: Проверьте /top!\n"
    "• 🔥💧🌪🪵🪖 СИСТЕМА ЭЛЕМЕНТОВ активна.</b>"
)

# Словарь цветов рамок для редкостей
RARITY_FRAME_COLORS = {
    "⬜️Обычная⬜️": "#808080",
    "🟩Необычная🟩": "#008000",
    "🟦Редкая🟦": "#0000FF",
    "🟪Эпическая🟪": "#800080",
    "🟨Легендарная🟨": "#FFD700",
    "🟥Мифическая🟥": "#FF0000",
    "🔵Божественная🔵": "#87CEEB",
    "🟣Эксклюзивная🟣": "#9400D3",
    "🌌Галактическая🌌": "#9400D3"
}

# Преимущества элементов
ELEMENT_ADVANTAGES = {
    "💧": ["🔥", "🪖"],
    "🔥": ["🪵", "🪖"],
    "🌪": ["💧", "🪖"],
    "🪵": ["🪖"],
    "🪖": []
}

# Конфигурация цен и лимитов мерчанта
MERCHANT_CONFIG = {
    "⬜️Обычная⬜️": {"price": (1, 10), "stock": 30},
    "🟩Необычная🟩": {"price": (5, 25), "stock": 20},
    "🟦Редкая🟦": {"price": (20, 50), "stock": 10},
    "🟪Эпическая🟪": {"price": (30, 80), "stock": 5},
    "🟨Легендарная🟨": {"price": (80, 120), "stock": 4},
    "🟥Мифическая🟥": {"price": (100, 200), "stock": 3},
    "🔵Божественная🔵": {"price": (350, 600), "stock": 2},
    "🟣Эксклюзивная🟣": {"price": (800, 1200), "stock": 1},
    "🌌Галактическая🌌": {"price": (800, 1200), "stock": 1}
}

logging.basicConfig(level=logging.INFO)

# Словари систем
RUNNING_BOTS = {}
PENDING_DUELS = {} 
MATCHMAKING = {} 
SEARCH_TASKS = {} 
ACTIVE_MERCHANTS = {} # bot_id -> {active: bool, offers: list, messages: list}

# === БАЗА ДАННЫХ ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'bot_data')

def get_db_path(bot_id: int) -> str:
    if bot_id == MAIN_BOT_ID:
        return os.path.join(DATA_DIR, 'cards_bot.db')
    return os.path.join(DATA_DIR, f'child_{bot_id}.db')

def get_db_connection(bot_id: int):
    conn = sqlite3.connect(get_db_path(bot_id), timeout=20.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db(bot_id: int, admin_id: int):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT,
            balance INTEGER DEFAULT 0, last_getcard INTEGER DEFAULT 0, trophies INTEGER DEFAULT 0)''')
            
    cursor.execute('''CREATE TABLE IF NOT EXISTS cards (
            card_id INTEGER PRIMARY KEY AUTOINCREMENT, photo_id TEXT NOT NULL,
            name TEXT NOT NULL, weight REAL DEFAULT 1, rarity TEXT, reward INTEGER DEFAULT 0,
            damage INTEGER DEFAULT 0, health INTEGER DEFAULT 0, element TEXT DEFAULT '🔥')''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS inventory (
            user_id INTEGER, card_id INTEGER, amount INTEGER DEFAULT 0, is_equipped INTEGER DEFAULT 0,
            UNIQUE(user_id, card_id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)''')
    cursor.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (admin_id,))
    
    if bot_id == MAIN_BOT_ID:
        cursor.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (MAIN_ADMIN_ID,))

    cursor.execute('''CREATE TABLE IF NOT EXISTS chats (chat_id INTEGER PRIMARY KEY, type TEXT)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS events (event_type TEXT PRIMARY KEY, multiplier REAL, end_time INTEGER)''')

    if bot_id == MAIN_BOT_ID:
        cursor.execute('''CREATE TABLE IF NOT EXISTS child_bots (
            bot_id INTEGER PRIMARY KEY, token TEXT, owner_id INTEGER)''')

    cursor.execute("PRAGMA table_info(users)")
    user_cols = [col[1] for col in cursor.fetchall()]
    if 'username' not in user_cols: cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
    if 'trophies' not in user_cols: cursor.execute("ALTER TABLE users ADD COLUMN trophies INTEGER DEFAULT 0")
    if 'balance' not in user_cols: cursor.execute("ALTER TABLE users ADD COLUMN balance INTEGER DEFAULT 0")

    cursor.execute("PRAGMA table_info(cards)")
    card_cols = [col[1] for col in cursor.fetchall()]
    if 'rarity' not in card_cols: cursor.execute("ALTER TABLE cards ADD COLUMN rarity TEXT DEFAULT 'common'")
    if 'damage' not in card_cols: cursor.execute("ALTER TABLE cards ADD COLUMN damage INTEGER DEFAULT 0")
    if 'health' not in card_cols: cursor.execute("ALTER TABLE cards ADD COLUMN health INTEGER DEFAULT 0")
    if 'element' not in card_cols: cursor.execute("ALTER TABLE cards ADD COLUMN element TEXT DEFAULT '🔥'")

    cursor.execute("PRAGMA table_info(inventory)")
    inv_cols = [col[1] for col in cursor.fetchall()]
    if 'is_equipped' not in inv_cols: cursor.execute("ALTER TABLE inventory ADD COLUMN is_equipped INTEGER DEFAULT 0")

    cursor.execute("UPDATE cards SET rarity = '🟣Эксклюзивная🟣' WHERE rarity = '🌌Галактическая🌌'")

    conn.commit()
    conn.close()

# === БЕЗОПАСНЫЙ MIDDLEWARE ДЛЯ ОТСЛЕЖИВАНИЯ ВСЕГО ===
class TrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            msg = getattr(event, 'message', None)
            channel_post = getattr(event, 'channel_post', None)
            my_chat_member = getattr(event, 'my_chat_member', None)
            call = getattr(event, 'callback_query', None)

            chat = msg.chat if msg else (channel_post.chat if channel_post else (my_chat_member.chat if my_chat_member else (call.message.chat if call and call.message else None)))
            user = msg.from_user if msg else (call.from_user if call else None)

            bot: Bot = data.get('bot')

            if chat and bot:
                for attempt in range(5):
                    try:
                        conn = get_db_connection(bot.id)
                        cursor = conn.cursor()
                        
                        cursor.execute("INSERT OR IGNORE INTO chats (chat_id, type) VALUES (?, ?)", (chat.id, chat.type))
                        
                        if user and not user.is_bot:
                            cursor.execute("INSERT OR IGNORE INTO users (user_id, balance, last_getcard, trophies) VALUES (?, 0, 0, 0)", (user.id,))
                            if user.username:
                                cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (user.username, user.id))
                        
                        conn.commit()
                        conn.close()
                        break 
                    except sqlite3.OperationalError as e:
                        if "locked" in str(e): await asyncio.sleep(random.uniform(0.05, 0.2))
                        else: break
        except Exception as e:
            logging.error(f"TrackerMiddleware Error: {e}")
            
        return await handler(event, data)

# === УТИЛИТЫ ДЛЯ КУБКОВ, МОНЕТ И ПОИСКА ===
def get_win_trophies(t: int) -> int:
    if t <= 100: return random.randint(1, 10)
    elif t <= 200: return random.randint(1, 8)
    elif t <= 300: return random.randint(1, 7)
    elif t <= 400: return random.randint(1, 6)
    elif t <= 500: return random.randint(1, 5)
    else: return random.randint(1, 3)

def get_loss_trophies(t: int) -> int:
    if t <= 100: return random.randint(1, 3)
    elif t <= 200: return random.randint(2, 4)
    elif t <= 300: return random.randint(2, 6)
    elif t <= 400: return random.randint(4, 8)
    elif t <= 500: return random.randint(5, 10)
    else: return random.randint(5, 15)

def get_win_coins(t: int) -> int:
    if t <= 10: return random.randint(1, 10)
    elif t <= 50: return random.randint(5, 25)
    elif t <= 150: return random.randint(20, 40)
    elif t <= 250: return random.randint(30, 70)
    elif t <= 400: return random.randint(50, 100)
    else: return random.randint(80, 150)

def get_mm_category(t: int) -> int:
    if t <= 200: return 1
    elif t <= 400: return 2
    elif t <= 600: return 3
    elif t <= 1000: return 4
    elif t <= 1500: return 5
    else: return 6

def is_admin(user_id: int, bot_id: int) -> bool:
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return bool(result)

def get_user_id_by_input(input_data: str, bot_id: int):
    if input_data.isdigit(): return int(input_data)
    input_data = input_data.replace("@", "")
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (input_data,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

def set_event(event_type: str, multiplier: float, minutes: int, bot_id: int):
    end_time = int(time.time()) + (minutes * 60)
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO events (event_type, multiplier, end_time) VALUES (?, ?, ?)", 
                   (event_type, multiplier, end_time))
    conn.commit()
    conn.close()

def get_active_event(event_type: str, bot_id: int):
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT multiplier, end_time FROM events WHERE event_type = ?", (event_type,))
    result = cursor.fetchone()
    conn.close()
    if result:
        multiplier, end_time = result
        if int(time.time()) < end_time:
            return multiplier
        else:
            conn = get_db_connection(bot_id)
            conn.execute("DELETE FROM events WHERE event_type = ?", (event_type,))
            conn.commit()
            conn.close()
    return None

async def apply_frame(photo_bytes: BytesIO, rarity: str) -> BufferedInputFile:
    frame_color = RARITY_FRAME_COLORS.get(rarity, "#FFFFFF")
    FRAME_WIDTH = 10

    try:
        with Image.open(photo_bytes) as img:
            if img.mode != 'RGB':
                img = img.convert('RGB')
            
            width, height = img.size
            draw = ImageDraw.Draw(img)
            
            draw.line([(FRAME_WIDTH/2, 0), (FRAME_WIDTH/2, height)], fill=frame_color, width=FRAME_WIDTH)
            draw.line([(width - FRAME_WIDTH/2, 0), (width - FRAME_WIDTH/2, height)], fill=frame_color, width=FRAME_WIDTH)
            draw.line([(0, FRAME_WIDTH/2), (width, FRAME_WIDTH/2)], fill=frame_color, width=FRAME_WIDTH)
            draw.line([(0, height - FRAME_WIDTH/2), (width, height - FRAME_WIDTH/2)], fill=frame_color, width=FRAME_WIDTH)
            
            output_buffer = BytesIO()
            img.save(output_buffer, format="JPEG")
            return BufferedInputFile(output_buffer.getvalue(), filename="drop_with_frame.jpg")
    except Exception as e:
        logging.error(f"Ошибка Pillow при наложении рамки: {e}")
        return BufferedInputFile(photo_bytes.getvalue(), filename="drop_original.jpg")

async def smart_reply(message: Message, text: str, parse_mode="HTML", reply_markup=None, photo=None):
    text = f"<b>{text}</b>"
    try:
        if message.chat.type in ['group', 'supergroup']:
            if photo:
                return await message.reply_photo(photo=photo, caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
            else:
                return await message.reply(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            if photo:
                return await message.answer_photo(photo=photo, caption=text, parse_mode=parse_mode, reply_markup=reply_markup)
            else:
                return await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramRetryAfter as e:
        logging.warning(f"Flood control! Skipping message. Retry in {e.retry_after}s.")
    except TelegramBadRequest as e:
        logging.error(f"TelegramBadRequest: {e}")
    except Exception as e:
        logging.error(f"Send Error: {e}")

async def broadcast(bot: Bot, text: str, reply_markup=None):
    text = f"<b>{text}</b>"
    bot_id = bot.id
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id, type FROM chats")
    chat_rows = cursor.fetchall()
    channel_ids = {row[0] for row in chat_rows if row[1] == 'channel'}
    chat_ids = {row[0] for row in chat_rows}
    cursor.execute("SELECT user_id FROM users")
    user_ids = {row[0] for row in cursor.fetchall()}
    conn.close()

    all_targets = set(chat_ids).union(set(user_ids))
    if not reply_markup:
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="сливы карточек тут", url="https://t.me/L1meYT")]])

    success, failed = 0, 0
    if bot_id == MAIN_BOT_ID:
        all_targets.add(MAIN_CHANNEL)
        try:
            await bot.send_message(MAIN_CHANNEL, text, parse_mode="HTML")
            success += 1
        except Exception: failed += 1

    for target_id in all_targets:
        if bot_id == MAIN_BOT_ID and (target_id in channel_ids or target_id == MAIN_CHANNEL): continue
        try:
            await bot.send_message(target_id, text, parse_mode="HTML", reply_markup=reply_markup)
            success += 1
            await asyncio.sleep(0.05)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception: failed += 1
    return success, failed

async def ad_broadcaster():
    while True:
        await asyncio.sleep(10800)
        ad_text = (
            "🤖 Понравился этот бот?\n\n"
            "Ты можешь абсолютно бесплатно создать точно такого же бота для своей группы или чата!\n"
            "Переходи в наш официальный канал, там можно создать своего бота и узнать все подробности:"
        )
        markup = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Создать своего бота 🚀", url="https://t.me/L1meYT")]])
        for b_id, b_instance in list(RUNNING_BOTS.items()):
            if b_id != MAIN_BOT_ID:
                try: await broadcast(b_instance, ad_text, reply_markup=markup)
                except Exception: pass

# === ЛОГИКА ГЛОБАЛЬНОГО МЕРЧАНТА ===
def build_merchant_ui(bot_id: int):
    merch = ACTIVE_MERCHANTS.get(bot_id)
    if not merch or not merch['active']:
        return "<b>🛒 Торговец ушел! Следующий прибудет через час.</b>", None
        
    lines = ["<b>🛒 Глобальный Торговец Прибыл!</b>\n<i>Исчезнет через несколько минут...</i>\n"]
    kb = []
    
    for idx, offer in enumerate(merch['offers']):
        lines.append(f"<b>{idx+1}. {offer['name']} ({offer['element']})</b>")
        lines.append(f"<b>💎 {offer['rarity']} | ⚔️ {offer['dmg']} | ❤️ {offer['hp']}</b>")
        lines.append(f"<b>💰 Цена: {offer['price']} монет | 📦 В наличии: {offer['stock']} шт.</b>")
        lines.append("<b>━━━━━━━━━━━━━━━━━━</b>")
        
        if offer['stock'] > 0:
            kb.append([InlineKeyboardButton(text=f"Купить {offer['name']} (💰{offer['price']})", callback_data=f"buy_merch_{idx}")])
            
    if all(o['stock'] <= 0 for o in merch['offers']):
        lines.append("<b>🔴 Все товары распроданы!</b>")
        
    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=kb) if kb else None

async def merchant_update_loop(bot: Bot):
    for _ in range(120): # 10 минут (120 раз по 5 сек)
        await asyncio.sleep(5)
        if bot.id not in ACTIVE_MERCHANTS or not ACTIVE_MERCHANTS[bot.id]['active']: 
            break
        text, markup = build_merchant_ui(bot.id)
        for chat_id, msg_id in ACTIVE_MERCHANTS[bot.id]['messages']:
            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, reply_markup=markup, parse_mode="HTML")
            except TelegramBadRequest: pass
            except TelegramRetryAfter: pass

    if bot.id in ACTIVE_MERCHANTS and ACTIVE_MERCHANTS[bot.id]['active']:
        ACTIVE_MERCHANTS[bot.id]['active'] = False
        text = "<b>🛒 Торговец ушел! Следующий прибудет через час.</b>"
        for chat_id, msg_id in ACTIVE_MERCHANTS[bot.id]['messages']:
            try:
                await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
            except Exception: pass

async def spawn_merchant(bot: Bot):
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT card_id, name, rarity, damage, health, element FROM cards")
    all_cards = cursor.fetchall()
    if len(all_cards) < 3: 
        conn.close()
        return
        
    cursor.execute("SELECT chat_id FROM chats WHERE type IN ('group', 'supergroup')")
    groups = cursor.fetchall()
    conn.close()
    
    offers = []
    selected = random.sample(all_cards, 3)
    for c in selected:
        c_id, name, rarity, dmg, hp, element = c
        conf = MERCHANT_CONFIG.get(rarity, MERCHANT_CONFIG["⬜️Обычная⬜️"])
        price = random.randint(*conf["price"])
        stock = conf["stock"]
        offers.append({
            'id': c_id, 'name': name, 'rarity': rarity, 'dmg': dmg, 'hp': hp, 
            'element': element, 'price': price, 'stock': stock
        })

    ACTIVE_MERCHANTS[bot.id] = {
        'active': True,
        'offers': offers,
        'messages': []
    }

    text, markup = build_merchant_ui(bot.id)
    for (chat_id,) in groups:
        try:
            msg = await bot.send_message(chat_id, text, reply_markup=markup, parse_mode="HTML")
            ACTIVE_MERCHANTS[bot.id]['messages'].append((chat_id, msg.message_id))
        except Exception: pass

    asyncio.create_task(merchant_update_loop(bot))

async def merchant_spawner(bot: Bot):
    while True:
        await asyncio.sleep(3600) # Каждый 1 час
        await spawn_merchant(bot)

async def cmd_gomerchant(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    
    merch = ACTIVE_MERCHANTS.get(bot.id)
    if merch and merch.get('active'):
        return await smart_reply(message, "❌ Мерчант уже активен! Сначала закрой его командой /closemerchant")
        
    await spawn_merchant(bot)
    await smart_reply(message, "✅ Торговец успешно призван во все чаты!")

async def cmd_closemerchant(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    
    merch = ACTIVE_MERCHANTS.get(bot.id)
    if not merch or not merch.get('active'):
        return await smart_reply(message, "❌ Мерчант сейчас не активен.")
        
    ACTIVE_MERCHANTS[bot.id]['active'] = False
    text = "<b>🛒 Торговец ушел! Следующий прибудет через час.</b>"
    for chat_id, msg_id in ACTIVE_MERCHANTS[bot.id]['messages']:
        try:
            await bot.edit_message_text(text, chat_id=chat_id, message_id=msg_id, parse_mode="HTML")
        except Exception: pass
        
    await smart_reply(message, "✅ Торговец успешно закрыт и удален из чатов!")

async def process_buy_merch(callback: CallbackQuery, bot: Bot):
    idx = int(callback.data.split("_")[2])
    merch = ACTIVE_MERCHANTS.get(bot.id)
    
    if not merch or not merch['active']:
        return await callback.answer("Торговец уже ушел!", show_alert=True)
        
    offer = merch['offers'][idx]
    if offer['stock'] <= 0:
        return await callback.answer("Этот товар распродан!", show_alert=True)
        
    user_id = callback.from_user.id
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    res = cursor.fetchone()
    balance = res[0] if res else 0
    
    if balance < offer['price']:
        conn.close()
        return await callback.answer("Недостаточно монет!", show_alert=True)
        
    offer['stock'] -= 1
    new_balance = balance - offer['price']
    
    cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
    cursor.execute('''INSERT INTO inventory (user_id, card_id, amount, is_equipped) 
                      VALUES (?, ?, 1, 0) 
                      ON CONFLICT(user_id, card_id) 
                      DO UPDATE SET amount = inventory.amount + 1''', (user_id, offer['id']))
    conn.commit()
    conn.close()
    
    await callback.answer(f"Ты купил {offer['name']} за {offer['price']} монет!", show_alert=True)

# === СОСТОЯНИЯ FSM И КЛАВИАТУРЫ ===
class AddCardState(StatesGroup):
    waiting_for_photo = State()
    waiting_for_name = State()
    waiting_for_weight = State()
    waiting_for_rarity = State()
    waiting_for_element = State()
    waiting_for_damage = State()
    waiting_for_health = State()

def get_admin_kb():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Добавить карту"), KeyboardButton(text="Удалить карту")]], resize_keyboard=True)

def get_inline_rarities_kb():
    kb, row = [], []
    for r in RARITY_FRAME_COLORS.keys():
        row.append(InlineKeyboardButton(text=r, callback_data=f"rarity_{r}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)
    return InlineKeyboardMarkup(inline_keyboard=kb)

def get_inline_elements_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥", callback_data="element_🔥"), InlineKeyboardButton(text="💧", callback_data="element_💧"), InlineKeyboardButton(text="🌪", callback_data="element_🌪")],
        [InlineKeyboardButton(text="🪵", callback_data="element_🪵"), InlineKeyboardButton(text="🪖", callback_data="element_🪖")]
    ])

def get_cards_delete_kb(cards_list):
    keyboard = []
    for card_id, name, rarity in cards_list:
        keyboard.append([InlineKeyboardButton(text=f"{rarity} {name}", callback_data=f"delcard_{card_id}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ==========================================================
# === ЛОГИКА КОМАНД БОТА ===
# ==========================================================
async def cmd_addbot(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        return await smart_reply(message, "🛠 Создание своего бота\n\nИспользование: /addbot [токен_бота]\n\n1. Перейди в @BotFather и создай нового бота.\n2. Скопируй токен.\n3. Отправь эту команду сюда вместе с токеном.")
    token = args[1].strip()
    try: new_bot_id = int(token.split(':')[0])
    except ValueError: return await smart_reply(message, "❌ Неверный формат токена.")
    if new_bot_id in RUNNING_BOTS: return await smart_reply(message, "❌ Этот бот уже запущен!")
    test_bot = Bot(token=token)
    try: me = await test_bot.get_me()
    except Exception: return await smart_reply(message, "❌ Ошибка авторизации. Токен недействителен.")
    finally: await test_bot.session.close()
    
    conn = get_db_connection(bot.id)
    try:
        conn.execute("INSERT INTO child_bots (bot_id, token, owner_id) VALUES (?, ?, ?)", (new_bot_id, token, message.from_user.id))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        return await smart_reply(message, "❌ Этот бот уже зарегистрирован в базе!")
    conn.close()
    asyncio.create_task(run_bot(token, message.from_user.id, is_startup=False))
    await smart_reply(message, f"✅ Бот @{me.username} успешно создан и запущен!\nТы назначен его Главным Администратором.")

async def cmd_globalmessage(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await smart_reply(message, "Использование: /globalmessage [сообщение]")
    await smart_reply(message, "⏳ Начинаю глобальную рассылку по всем чатам и личным сообщениям...")
    success, failed = await broadcast(bot, f"📢 Глобальное уведомление:\n\n{args[1]}")
    await smart_reply(message, f"✅ Рассылка успешно завершена!\nУспешно отправлено: {success}\nОшибок отправки: {failed}")

async def cmd_addadmin(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await smart_reply(message, "Использование: /addadmin [id или @username]")
    target_id = get_user_id_by_input(args[1], bot.id)
    if not target_id: return await smart_reply(message, "Пользователь не найден в базе данных этого бота.")
    conn = get_db_connection(bot.id)
    conn.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (target_id,))
    conn.commit()
    conn.close()
    await smart_reply(message, f"✅ Пользователь {args[1]} назначен администратором!")

async def cmd_deladmin(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2: return await smart_reply(message, "Использование: /deladmin [id или @username]")
    target_id = get_user_id_by_input(args[1], bot.id)
    if not target_id: return await smart_reply(message, "Пользователь не найден.")
    if bot.id == MAIN_BOT_ID and target_id == MAIN_ADMIN_ID: return await smart_reply(message, "Нельзя удалить главного создателя!")
    conn = get_db_connection(bot.id)
    conn.execute("DELETE FROM admins WHERE user_id = ?", (target_id,))
    conn.commit()
    conn.close()
    await smart_reply(message, f"✅ Пользователь {args[1]} удален из администраторов.")

async def cmd_events(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    cmd = message.text.split()[0].lower()
    args = message.text.split()[1:]
    if len(args) < 2: return await smart_reply(message, f"Использование: {cmd} [множитель] [время в минутах]")
    try: multiplier, minutes = float(args[0]), int(args[1])
    except ValueError: return await smart_reply(message, "Множитель и время должны быть числами!")
    event_map = {"/luckevent": ("luck", "🍀 Активирован эвент УДАЧИ!\nШанс на выпадение редких карт увеличен в"), "/cooldownevent": ("cooldown", "⏳ Активирован эвент СКОРОСТИ!\nВремя перезарядки карт уменьшено в")}
    if cmd not in event_map: return
    ev_type, ev_text = event_map[cmd]
    set_event(ev_type, multiplier, minutes, bot.id)
    await smart_reply(message, f"✅ Эвент {ev_type} успешно запущен. Начинаю рассылку...")
    asyncio.create_task(broadcast(bot, f"{ev_text} {multiplier}x на {minutes} минут!"))

async def cmd_events_space(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    parts = message.text.split()
    if len(parts) < 4: return await smart_reply(message, "Ошибка формата. Пример: /cooldown event 2 60")
    cmd = parts[0]
    try: multiplier, minutes = float(parts[2]), int(parts[3])
    except ValueError: return await smart_reply(message, "Множитель и время должны быть числами!")
    if cmd == "/cooldown":
        set_event("cooldown", multiplier, minutes, bot.id)
        text = f"⏳ Активирован эвент СКОРОСТИ!\nВремя перезарядки карт уменьшено в {multiplier}x на {minutes} минут!"
        await smart_reply(message, "✅ Эвент запущен. Начинаю рассылку...")
        asyncio.create_task(broadcast(bot, text))

# === СОЗДАНИЕ КАРТЫ (FSM) ===
async def start_add_card(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    await message.answer("<b>📸 Отправь фото карты:</b>", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True), parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_photo)

async def cancel_action(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if is_admin(message.from_user.id, bot.id): await message.answer("<b>Действие отменено.</b>", reply_markup=get_admin_kb(), parse_mode="HTML")
    else: await message.answer("<b>Действие отменено.</b>", parse_mode="HTML")

async def process_photo(message: Message, state: FSMContext):
    await state.update_data(photo_id=message.photo[-1].file_id)
    await message.answer("<b>📝 Введи название:</b>", parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_name)

async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("<b>⚖️ Введи шанс выпадения (вес):</b>", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True), parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_weight)

async def process_weight(message: Message, state: FSMContext):
    try: weight = float(message.text.replace(",", "."))
    except ValueError: return await message.answer("<b>Вес должен быть числом! Попробуй еще раз:</b>", parse_mode="HTML")
    await state.update_data(weight=weight)
    await message.answer("<b>💎 Выбери редкость:</b>", reply_markup=get_inline_rarities_kb(), parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_rarity)

async def process_rarity(callback: CallbackQuery, state: FSMContext):
    rarity = callback.data.split("rarity_")[1]
    if rarity not in RARITY_FRAME_COLORS: return await callback.answer("Ошибка! Неизвестная редкость.", show_alert=True)
    await state.update_data(rarity=rarity)
    try: await callback.message.edit_text(f"<b>💎 Выбрана редкость: {rarity}</b>", parse_mode="HTML")
    except TelegramBadRequest: pass
    text = "⚡️ Выбери элемент карты:\n\nТаблица преимуществ:\n💧 &gt; 🔥, 🪖\n🔥 &gt; 🪵, 🪖\n🌪 &gt; 💧, 🪖\n🪵 &gt; 🪖\n🪖 &lt; Проигрывает всем"
    await callback.message.answer(f"<b>{text}</b>", reply_markup=get_inline_elements_kb(), parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_element)
    await callback.answer()

async def process_element(callback: CallbackQuery, state: FSMContext):
    element = callback.data.split("element_")[1]
    await state.update_data(element=element)
    try: await callback.message.edit_text(f"<b>⚡️ Выбран элемент: {element}</b>", parse_mode="HTML")
    except TelegramBadRequest: pass
    await callback.message.answer("<b>⚔️ Введи урон:</b>", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True), parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_damage)
    await callback.answer()

async def process_damage(message: Message, state: FSMContext):
    if not message.text.isdigit(): return await message.answer("<b>Урон должен быть целым числом! Попробуй еще раз:</b>", parse_mode="HTML")
    await state.update_data(damage=int(message.text))
    await message.answer("<b>❤️ Введи здоровье:</b>", parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_health)

async def process_health(message: Message, state: FSMContext, bot: Bot):
    if not message.text.isdigit(): return await message.answer("<b>Здоровье должно быть целым числом! Попробуй еще раз:</b>", parse_mode="HTML")
    health = int(message.text)
    data = await state.get_data()
    
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute('INSERT INTO cards (photo_id, name, weight, rarity, damage, health, element) VALUES (?, ?, ?, ?, ?, ?, ?)', 
                   (data['photo_id'], data['name'], data['weight'], data['rarity'], data['damage'], health, data['element']))
    conn.commit()
    cursor.execute("SELECT user_id FROM admins")
    admins = cursor.fetchall()
    conn.close()
    
    await message.answer(f"<b>✅ Карта «{data['name']} ({data['element']})» добавлена!\n⚖️ Вес: {data['weight']}\n💎 Редкость: {data['rarity']}\n⚔️ Урон: {data['damage']}\n❤️ Здоровье: {health}</b>", reply_markup=get_admin_kb(), parse_mode="HTML")
    await state.clear()

    admin_mention = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    notify_text = f"<b>🔔 Создана новая карта!\nАвтор: {admin_mention}\n\n🃏 {data['name']} ({data['element']})\n⚖️ Вес: {data['weight']}\n💎 Редкость: {data['rarity']}\n⚔️ Урон: {data['damage']}\n❤️ Здоровье: {health}</b>"

    for (adm_id,) in admins:
        if adm_id != message.from_user.id:
            try: await bot.send_photo(adm_id, photo=data['photo_id'], caption=notify_text, parse_mode="HTML")
            except Exception: pass

async def invalid_fsm_input(message: Message):
    await message.answer("<b>⚠️ Неверный формат данных!\nПожалуйста, отправь то, что требует бот на этом шаге (текст, число, фото или нажми на кнопку). Или нажми «Отмена».</b>", parse_mode="HTML")

async def start_delete_card(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(message.from_user.id, bot.id): return
    conn = get_db_connection(bot.id)
    cards = conn.execute("SELECT card_id, name, rarity FROM cards").fetchall()
    conn.close()
    if not cards: return await message.answer("<b>В базе пока нет добавленных карт.</b>", parse_mode="HTML")
    await message.answer("<b>Нажми на карту, чтобы удалить её навсегда:</b>", reply_markup=get_cards_delete_kb(cards), parse_mode="HTML")

async def process_delete_card(callback: CallbackQuery, bot: Bot, state: FSMContext):
    await state.clear()
    if not is_admin(callback.from_user.id, bot.id): return await callback.answer("У вас нет прав!", show_alert=True)
    card_id = int(callback.data.split("_")[1])
    conn = get_db_connection(bot.id)
    conn.execute("DELETE FROM cards WHERE card_id = ?", (card_id,))
    conn.execute("DELETE FROM inventory WHERE card_id = ?", (card_id,))
    conn.commit()
    conn.close()
    await callback.answer("Карта удалена!", show_alert=True)
    new_keyboard = [[btn for btn in row if btn.callback_data != callback.data] for row in callback.message.reply_markup.inline_keyboard]
    new_keyboard = [row for row in new_keyboard if row]
    if new_keyboard:
        try: await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=new_keyboard))
        except TelegramBadRequest: pass
    else:
        await callback.message.edit_text("<b>✅ Все карты из этого списка были удалены.</b>", parse_mode="HTML")

async def cmd_start(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if is_admin(message.from_user.id, bot.id) and message.chat.type == "private": await smart_reply(message, "Добро пожаловать в панель администратора!\nВведите /help для просмотра всех команд.", reply_markup=get_admin_kb())
    else: await smart_reply(message, "Привет! Я карточный бот.\nИспользуй команду /getcard, чтобы испытать удачу и выбить карту!\nСписок всех команд: /help")

async def cmd_help(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user_text = "📜 Список доступных команд:\n\n🎮 Основные:\n• /start — Перезапустить бота\n• /help — Показать это меню\n• /getcard — Выбить случайную карту\n• /inventory — Твои собранные карты\n• /equip — Управление экипировкой\n• /duel — Вызвать игрока на бой (в группе)\n• /pvpsearch — Найти случайного противника (в ЛС)\n• /top — Топ игроков по кубкам\n• /index — Посмотреть индекс всех карт\n• /profile — Посмотреть свой профиль\n"
    admin_text = "\n👑 Для администраторов:\n• /gomerchant — Запустить торговца\n• /closemerchant — Закрыть торговца\n• /addadmin [id/@] — Выдать админку\n• /deladmin [id/@] — Забрать админку\n• /globalmessage [текст] — Рассылка\n• /luckevent [множ] [мин] — Эвент УДАЧИ\n• /cooldownevent [множ] [мин] — Эвент СКОРОСТИ\n\n(Кнопки «Добавить карту» и «Удалить карту» доступны в панели /start)"
    if is_admin(message.from_user.id, bot.id): await smart_reply(message, user_text + admin_text)
    else: await smart_reply(message, user_text)

async def cmd_top(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    conn = get_db_connection(bot.id)
    top_users = conn.execute("SELECT username, user_id, trophies FROM users WHERE trophies > 0 ORDER BY trophies DESC LIMIT 20").fetchall()
    conn.close()
    if not top_users: return await smart_reply(message, "🏆 В топе пока никого нет. Вызови кого-нибудь на дуэль и стань первым!")
    lines = ["🏆 Топ 20 игроков по трофеям:\n"]
    for idx, (username, uid, trophies) in enumerate(top_users, 1): lines.append(f"{idx}. {'@'+username if username else 'ID: '+str(uid)} — {trophies} 🏆")
    await smart_reply(message, "\n".join(lines))

async def cmd_profile(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    conn = get_db_connection(bot.id)
    res = conn.execute("SELECT trophies, balance FROM users WHERE user_id = ?", (user_id,)).fetchone()
    trophies, balance = (res[0], res[1]) if res else (0, 0)
    unlocked_cards = conn.execute("SELECT COUNT(card_id) FROM inventory WHERE user_id = ? AND amount > 0", (user_id,)).fetchone()[0]
    total_cards = conn.execute("SELECT COUNT(card_id) FROM cards").fetchone()[0]
    conn.close()
    mention = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    await smart_reply(message, f"👤 Профиль игрока {mention}\n\n💳 Твой ID: {user_id}\n💰 Монеты: {balance}\n🏆 Трофеи: {trophies}\n🎒 Открыто карт: {unlocked_cards}/{total_cards}\n\n💡 Участвуй в битвах, чтобы заработать монеты для Мерчанта!")

async def cmd_getcard(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    user_id = message.from_user.id
    current_time = int(time.time())
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT last_getcard FROM users WHERE user_id = ?", (user_id,))
    user_data = cursor.fetchone()
    
    active_cooldown = COOLDOWN_SECONDS
    cd_mult = get_active_event("cooldown", bot.id)
    if cd_mult and cd_mult > 0: active_cooldown = int(COOLDOWN_SECONDS / cd_mult)
        
    time_passed = current_time - (user_data[0] if user_data else 0)
    if time_passed < active_cooldown:
        remaining = active_cooldown - time_passed
        await smart_reply(message, f"⏳ Карты перезаряжаются.\nПодожди еще {remaining // 60} мин {remaining % 60} сек.")
        conn.close()
        return
        
    cursor.execute("SELECT card_id, photo_id, name, weight, rarity, damage, health, element FROM cards")
    cards = cursor.fetchall()
    if not cards:
        await smart_reply(message, "Бот пока пуст. Администратор еще не добавил карты!")
        conn.close()
        return
        
    luck_mult = get_active_event("luck", bot.id)
    weights = [(c[3] * luck_mult if luck_mult and c[4] != "⬜️Обычная⬜️" else c[3]) for c in cards]
    chosen = random.choices(cards, weights=weights, k=1)[0]
    
    cursor.execute("SELECT amount FROM inventory WHERE user_id = ? AND card_id = ?", (user_id, chosen[0]))
    is_new = not cursor.fetchone()
    display_name = f"{chosen[2]} ({chosen[7]})" + (" 🔥NEW🔥" if is_new else "")
    
    cursor.execute("UPDATE users SET last_getcard = ? WHERE user_id = ?", (current_time, user_id))
    cursor.execute("INSERT INTO inventory (user_id, card_id, amount, is_equipped) VALUES (?, ?, 1, 0) ON CONFLICT(user_id, card_id) DO UPDATE SET amount = inventory.amount + 1", (user_id, chosen[0]))
    conn.commit()
    conn.close()
    
    mention = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    caption = f"🎉 {mention}, тебе выпала карта!\n\n🃏 {display_name}\n💎 Редкость • {chosen[4]}\n⚔️ Урон • {chosen[5]}\n❤️ Здоровье • {chosen[6]}"
    
    try:
        photo_bytes = BytesIO()
        file = await bot.get_file(chosen[1])
        await bot.download_file(file.file_path, photo_bytes)
        photo_bytes.seek(0)
        framed = await apply_frame(photo_bytes, chosen[4])
        await smart_reply(message, text=caption, photo=framed)
    except Exception: await smart_reply(message, text=caption, photo=chosen[1])

# --- ИНДЕКС ---
async def get_index_page(bot_id: int, user_id: int, page: int = 1):
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT card_id, name, weight, rarity, damage, health, element FROM cards ORDER BY weight DESC")
    cards = cursor.fetchall()
    if not cards:
        conn.close()
        return "<b>📭 В игре пока нет ни одной добавленной карты.</b>", None
        
    total_weight = sum(c[2] for c in cards)
    unlocked_ids = {row[0] for row in cursor.execute("SELECT card_id FROM inventory WHERE user_id = ? AND amount > 0", (user_id,)).fetchall()}
    exists_dict = {row[0]: row[1] for row in cursor.execute("SELECT card_id, SUM(amount) FROM inventory GROUP BY card_id").fetchall()}
    conn.close()
    
    PER_PAGE = 7
    total_pages = (len(cards) + PER_PAGE - 1) // PER_PAGE
    page = max(1, min(page, total_pages))
    
    lines = [f"📖 Индекс всех карт (Страница {page}/{total_pages})\n"]
    for idx, card in enumerate(cards[(page-1)*PER_PAGE : page*PER_PAGE], start=(page-1)*PER_PAGE + 1):
        chance_pct = (card[2] / total_weight) * 100 if total_weight > 0 else 0
        display_name = f"{card[1]} ({card[6]})" if card[0] in unlocked_ids else "???"
        lines.append(f"{idx}. {display_name}\n💎 {card[3]} | 🎲 {chance_pct:.2f}%\n⚔️ Урон • {card[4]} | ❤️ Здоровье • {card[5]}\n💫 Существует • {exists_dict.get(card[0], 0)} 💫\n━━━━━━━━━━━━━━━━━━")
        
    kb, nav = [], []
    if page > 1: nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"index_page_{page-1}_{user_id}"))
    if page < total_pages: nav.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"index_page_{page+1}_{user_id}"))
    if nav: kb.append(nav)
    return "<b>" + "\n".join(lines) + "</b>", InlineKeyboardMarkup(inline_keyboard=kb) if kb else None

async def cmd_index(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    text, markup = await get_index_page(bot.id, message.from_user.id, 1)
    await smart_reply(message, text.replace('<b>','').replace('</b>',''), reply_markup=markup)

async def process_index_page(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    page, owner_id = int(parts[2]), int(parts[3])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твой индекс!", show_alert=True)
    text, markup = await get_index_page(bot.id, owner_id, page)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer()

# --- ИНВЕНТАРЬ ---
async def get_inventory_page(bot_id: int, user_id: int, page: int = 1):
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    cursor.execute("SELECT c.card_id, c.name, c.rarity, c.damage, c.health, i.amount, i.is_equipped, c.element FROM inventory i JOIN cards c ON i.card_id = c.card_id WHERE i.user_id = ? AND i.amount > 0 ORDER BY c.weight DESC", (user_id,))
    cards = cursor.fetchall()
    conn.close()

    if not cards: return "<b>📭 Твой инвентарь пуст. Используй /getcard чтобы выбить первую карту!</b>", None
    PER_PAGE = 7
    total_pages = (len(cards) + PER_PAGE - 1) // PER_PAGE
    page = max(1, min(page, total_pages))

    lines = [f"🎒 Твой инвентарь (Страница {page}/{total_pages})\n"]
    for idx, card in enumerate(cards[(page-1)*PER_PAGE : page*PER_PAGE], start=(page-1)*PER_PAGE + 1):
        lines.append(f"{idx}. {card[1]} ({card[7]}){' ✅(Надето)' if card[6] else ''}\n💎 {card[2]} | 📦 {card[5]}\n⚔️ {card[3]} | ❤️ {card[4]}\n━━━━━━━━━━━━━━━━━━")

    kb, nav = [], []
    if page > 1: nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"inv_page_{page-1}_{user_id}"))
    if page < total_pages: nav.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"inv_page_{page+1}_{user_id}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="🛡 Экипировка", callback_data=f"open_equip_{user_id}")])
    return "<b>" + "\n".join(lines) + "</b>", InlineKeyboardMarkup(inline_keyboard=kb)

async def cmd_inventory(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    text, markup = await get_inventory_page(bot.id, message.from_user.id, 1)
    await smart_reply(message, text.replace('<b>','').replace('</b>',''), reply_markup=markup)

async def process_inv_page(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    page, owner_id = int(parts[2]), int(parts[3])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твой инвентарь!", show_alert=True)
    text, markup = await get_inventory_page(bot.id, callback.from_user.id, page)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except TelegramBadRequest: pass
    await callback.answer()

# --- ЭКИПИРОВКА ---
async def get_equip_menu(bot_id: int, user_id: int):
    conn = get_db_connection(bot_id)
    equipped = conn.execute("SELECT c.name, c.rarity, c.damage, c.health, c.element FROM inventory i JOIN cards c ON i.card_id = c.card_id WHERE i.user_id = ? AND i.is_equipped = 1", (user_id,)).fetchall()
    conn.close()

    lines = ["🛡 Твоя экипировка (макс. 3 карты):\n"]
    if not equipped:
        lines.append("Ничего не надето.\n")
        total_dmg, total_hp = 0, 0
    else:
        for idx, c in enumerate(equipped, 1): lines.append(f"{idx}. {c[0]} ({c[4]})\n⚔️ {c[2]} | ❤️ {c[3]} | 💎 {c[1].split('⬜️')[0].strip('🟩🟦🟪🟨🟥🔵🟣')}")
        lines.append("")
        total_dmg = sum(c[2] for c in equipped)
        total_hp = sum(c[3] for c in equipped)

    lines.append(f"📊 Общие характеристики:\n⚔️ Урон: {total_dmg}\n❤️ Здоровье: {total_hp}")
    kb = [
        [InlineKeyboardButton(text="🗂 Экипировать карты", callback_data=f"equip_select_1_{user_id}")],
        [InlineKeyboardButton(text="🌟 Экипировать лучшие", callback_data=f"equip_auto_{user_id}")],
        [InlineKeyboardButton(text="❌ Снять все", callback_data=f"equip_clear_{user_id}")],
        [InlineKeyboardButton(text="🔙 В инвентарь", callback_data=f"inv_page_1_{user_id}")]
    ]
    return "<b>" + "\n".join(lines) + "</b>", InlineKeyboardMarkup(inline_keyboard=kb)

async def cmd_equip(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    text, markup = await get_equip_menu(bot.id, message.from_user.id)
    await smart_reply(message, text.replace('<b>','').replace('</b>',''), reply_markup=markup)

async def process_open_equip(callback: CallbackQuery, bot: Bot):
    owner_id = int(callback.data.split("_")[2])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твоя экипировка!", show_alert=True)
    text, markup = await get_equip_menu(bot.id, callback.from_user.id)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer()

async def get_equip_select_page(bot_id: int, user_id: int, page: int = 1):
    conn = get_db_connection(bot_id)
    cards = conn.execute("SELECT c.card_id, c.name, i.is_equipped, c.damage, c.health, c.element FROM inventory i JOIN cards c ON i.card_id = c.card_id WHERE i.user_id = ? AND i.amount > 0 ORDER BY (c.damage + c.health) DESC", (user_id,)).fetchall()
    conn.close()

    if not cards: return "<b>📭 У тебя нет карт для экипировки.</b>", InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data=f"open_equip_{user_id}")]])

    PER_PAGE = 8
    total_pages = (len(cards) + PER_PAGE - 1) // PER_PAGE
    page = max(1, min(page, total_pages))

    text = f"<b>🗂 Выбери карту (Страница {page}/{total_pages}):\n(Можно надеть максимум 3 разные карты)</b>"
    kb = []
    for c_id, c_name, is_equipped, dmg, hp, el in cards[(page-1)*PER_PAGE : page*PER_PAGE]:
        kb.append([InlineKeyboardButton(text=f"{'✅ ' if is_equipped else ''}{c_name}({el}) ⚔️{dmg} ❤️{hp}", callback_data=f"eqcard_{c_id}_{user_id}")])

    nav = []
    if page > 1: nav.append(InlineKeyboardButton(text="◀️", callback_data=f"equip_select_{page-1}_{user_id}"))
    if page < total_pages: nav.append(InlineKeyboardButton(text="▶️", callback_data=f"equip_select_{page+1}_{user_id}"))
    if nav: kb.append(nav)
    kb.append([InlineKeyboardButton(text="🔙 Вернуться", callback_data=f"open_equip_{user_id}")])
    return text, InlineKeyboardMarkup(inline_keyboard=kb)

async def process_equip_select(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    page, owner_id = int(parts[2]), int(parts[3])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твоя экипировка!", show_alert=True)
    text, markup = await get_equip_select_page(bot.id, callback.from_user.id, page)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer()

async def process_equip_card(callback: CallbackQuery, bot: Bot):
    parts = callback.data.split("_")
    card_id, owner_id = int(parts[1]), int(parts[2])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твоя экипировка!", show_alert=True)
    
    user_id = callback.from_user.id
    conn = get_db_connection(bot.id)
    res = conn.execute("SELECT is_equipped FROM inventory WHERE user_id = ? AND card_id = ?", (user_id, card_id)).fetchone()
    
    if not res:
        conn.close()
        return await callback.answer("Ошибка: карта не найдена.")

    if res[0]:
        conn.execute("UPDATE inventory SET is_equipped = 0 WHERE user_id = ? AND card_id = ?", (user_id, card_id))
    else:
        if conn.execute("SELECT COUNT(*) FROM inventory WHERE user_id = ? AND is_equipped = 1", (user_id,)).fetchone()[0] >= 3:
            conn.close()
            return await callback.answer("❌ Максимум 3 карты! Сначала снимите другую.", show_alert=True)
        conn.execute("UPDATE inventory SET is_equipped = 1 WHERE user_id = ? AND card_id = ?", (user_id, card_id))
    conn.commit()
    conn.close()
    
    text, markup = await get_equip_menu(bot.id, user_id)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer("Экипировка обновлена!")

async def process_equip_auto(callback: CallbackQuery, bot: Bot):
    owner_id = int(callback.data.split("_")[2])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твоя экипировка!", show_alert=True)
    user_id = callback.from_user.id
    conn = get_db_connection(bot.id)
    conn.execute("UPDATE inventory SET is_equipped = 0 WHERE user_id = ?", (user_id,))
    best_cards = conn.execute("SELECT i.card_id FROM inventory i JOIN cards c ON i.card_id = c.card_id WHERE i.user_id = ? AND i.amount > 0 ORDER BY (c.damage + c.health) DESC LIMIT 3", (user_id,)).fetchall()
    for (c_id,) in best_cards: conn.execute("UPDATE inventory SET is_equipped = 1 WHERE user_id = ? AND card_id = ?", (user_id, c_id))
    conn.commit()
    conn.close()
    
    text, markup = await get_equip_menu(bot.id, user_id)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer("Лучшие карты экипированы!")

async def process_equip_clear(callback: CallbackQuery, bot: Bot):
    owner_id = int(callback.data.split("_")[2])
    if callback.from_user.id != owner_id: return await callback.answer("Это не твоя экипировка!", show_alert=True)
    user_id = callback.from_user.id
    conn = get_db_connection(bot.id)
    conn.execute("UPDATE inventory SET is_equipped = 0 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    text, markup = await get_equip_menu(bot.id, user_id)
    try: await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except: pass
    await callback.answer("Все карты сняты!")

# === МАТЧМЕЙКИНГ В ЛС ===
async def search_update_loop(bot: Bot, user_id: int, cat: int):
    try:
        queue = MATCHMAKING[bot.id][cat]
        while user_id in queue:
            data = queue[user_id]
            elapsed = int(time.time() - data['start_time'])
            
            text = f"<b>🔍 Поиск противника...\n⏳ Время • {elapsed} сек\n👥 Игроки в подборе • {len(queue)}</b>"
            kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_search")]])
            try: await data['msg'].edit_text(text, reply_markup=kb, parse_mode="HTML")
            except TelegramBadRequest: pass
            except TelegramRetryAfter: pass
            await asyncio.sleep(3)
    except asyncio.CancelledError: pass

async def cmd_pvpsearch(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if message.chat.type != "private": return await message.answer("<b>⚔️ Эта команда работает только в личных сообщениях с ботом!</b>", parse_mode="HTML")
    user_id = message.from_user.id

    conn = get_db_connection(bot.id)
    has_cards = conn.execute("SELECT 1 FROM inventory WHERE user_id = ? AND is_equipped = 1", (user_id,)).fetchone()
    res = conn.execute("SELECT trophies FROM users WHERE user_id = ?", (user_id,)).fetchone()
    trophies = res[0] if res else 0
    conn.close()

    if not has_cards: return await message.answer("<b>У тебя нет экипированных карт! Используй /equip</b>", parse_mode="HTML")

    if bot.id not in MATCHMAKING: MATCHMAKING[bot.id] = {1:{}, 2:{}, 3:{}, 4:{}, 5:{}, 6:{}}
    if any(user_id in q for q in MATCHMAKING[bot.id].values()): return await message.answer("<b>Ты уже находишься в поиске!</b>", parse_mode="HTML")

    cat = get_mm_category(trophies)
    queue = MATCHMAKING[bot.id][cat]

    if len(queue) > 0:
        opponent_id, opp_data = list(queue.items())[0]
        del queue[opponent_id]
        if opponent_id in SEARCH_TASKS:
            SEARCH_TASKS[opponent_id].cancel()
            del SEARCH_TASKS[opponent_id]

        try: await opp_data['msg'].edit_text(f"<b>⚔️ Противник найден!\nТвой соперник: {message.from_user.first_name}\n\n<i>Бой начнется через 3 секунды...</i></b>", parse_mode="HTML")
        except: pass
        await message.answer(f"<b>⚔️ Противник найден!\nТвой соперник: {opp_data['user'].first_name}\n\n<i>Бой начнется через 3 секунды...</i></b>", parse_mode="HTML")
        
        await asyncio.sleep(3)
        duel_data = {'p1_id': opponent_id, 'p2_id': message.from_user.id, 'p1_name': opp_data['user'].first_name, 'p2_name': message.from_user.first_name}
        asyncio.create_task(run_battle(bot, opponent_id, duel_data, is_private=True, chat2=message.from_user.id))
        return

    msg = await message.answer("<b>🔍 Запуск радара...</b>", parse_mode="HTML")
    queue[user_id] = {'user': message.from_user, 'msg': msg, 'start_time': time.time()}
    SEARCH_TASKS[user_id] = asyncio.create_task(search_update_loop(bot, user_id, cat))

async def process_cancel_search(callback: CallbackQuery, bot: Bot):
    user_id = callback.from_user.id
    if bot.id in MATCHMAKING:
        for cat, queue in MATCHMAKING[bot.id].items():
            if user_id in queue:
                del queue[user_id]
                if user_id in SEARCH_TASKS:
                    SEARCH_TASKS[user_id].cancel()
                    del SEARCH_TASKS[user_id]
                try: await callback.message.edit_text("<b>❌ Поиск противника отменен.</b>", parse_mode="HTML")
                except: pass
                return await callback.answer()
    await callback.answer("Ты не в поиске!", show_alert=True)

# === ДУЭЛИ В ГРУППЕ ===
async def cmd_duel(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    if message.chat.type == "private": return await message.answer("<b>⚔️ Дуэли доступны только в группах!</b>", parse_mode="HTML")
    if not message.reply_to_message: return await message.answer("<b>Чтобы вызвать игрока на дуэль, ответь на его сообщение командой /duel</b>", parse_mode="HTML")
        
    target = message.reply_to_message.from_user
    if target.id == message.from_user.id or target.is_bot: return await message.answer("<b>Нельзя вызвать на дуэль самого себя или бота!</b>", parse_mode="HTML")
        
    conn = get_db_connection(bot.id)
    p1_has_cards = conn.execute("SELECT 1 FROM inventory WHERE user_id = ? AND is_equipped = 1", (message.from_user.id,)).fetchone()
    p2_has_cards = conn.execute("SELECT 1 FROM inventory WHERE user_id = ? AND is_equipped = 1", (target.id,)).fetchone()
    conn.close()
    
    if not p1_has_cards: return await message.answer("<b>У тебя нет экипированных карт для дуэли! Используй /equip</b>", parse_mode="HTML")
    if not p2_has_cards: return await message.answer(f"<b>У игрока {target.first_name} нет экипированных карт!</b>", parse_mode="HTML")
        
    duel_id = f"{message.chat.id}_{message.message_id}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Принять", callback_data=f"duel_acc_{duel_id}"), InlineKeyboardButton(text="❌ Отказаться", callback_data=f"duel_dec_{duel_id}")]])
    
    mention_target = target.mention_html() if not target.username else f"@{target.username}"
    mention_p1 = message.from_user.mention_html() if not message.from_user.username else f"@{message.from_user.username}"
    
    msg = await message.answer(f"<b>⚔️ {mention_target}, игрок {mention_p1} вызывает тебя на дуэль!\nУ тебя есть 1 минута на принятие решения.</b>", reply_markup=kb, parse_mode="HTML")
    
    if bot.id not in PENDING_DUELS: PENDING_DUELS[bot.id] = {}
        
    async def cancel_duel():
        await asyncio.sleep(60)
        if duel_id in PENDING_DUELS.get(bot.id, {}):
            del PENDING_DUELS[bot.id][duel_id]
            try: await msg.edit_text(f"<b>⏳ Время вышло! Дуэль отменена.</b>", parse_mode="HTML")
            except: pass
            
    PENDING_DUELS[bot.id][duel_id] = {'p1_id': message.from_user.id, 'p2_id': target.id, 'p1_name': message.from_user.first_name, 'p2_name': target.first_name, 'msg': msg, 'task': asyncio.create_task(cancel_duel())}

async def process_duel_acc(callback: CallbackQuery, bot: Bot):
    duel_id = callback.data.split("duel_acc_")[1]
    duels = PENDING_DUELS.get(bot.id, {})
    if duel_id not in duels: return await callback.answer("Время вызова истекло или дуэль уже завершена!", show_alert=True)
    duel_data = duels[duel_id]
    if callback.from_user.id != duel_data['p2_id']: return await callback.answer("Этот вызов адресован не тебе!", show_alert=True)
        
    duel_data['task'].cancel()
    del duels[duel_id]
    try: await callback.message.edit_text("<b>⚔️ Вызов принят! Подготовка к бою...</b>", parse_mode="HTML")
    except: pass
    await callback.answer("Бой начинается!")
    asyncio.create_task(run_battle(bot, callback.message.chat.id, duel_data, is_private=False))

async def process_duel_dec(callback: CallbackQuery, bot: Bot):
    duel_id = callback.data.split("duel_dec_")[1]
    duels = PENDING_DUELS.get(bot.id, {})
    if duel_id not in duels: return await callback.answer("Время вызова истекло или дуэль уже завершена!", show_alert=True)
    duel_data = duels[duel_id]
    if callback.from_user.id != duel_data['p2_id']: return await callback.answer("Этот вызов адресован не тебе!", show_alert=True)
        
    duel_data['task'].cancel()
    del duels[duel_id]
    try: await callback.message.edit_text(f"<b>❌ Игрок {duel_data['p2_name']} отказался от дуэли с {duel_data['p1_name']}.</b>", parse_mode="HTML")
    except: pass
    await callback.answer("Вы отказались от дуэли.")

# === УНИВЕРСАЛЬНЫЙ БОЕВОЙ ДВИЖОК ===
async def run_battle(bot: Bot, chat_id: int, duel_data: dict, is_private=False, chat2=None):
    p1_id, p2_id = duel_data['p1_id'], duel_data['p2_id']
    p1_name, p2_name = duel_data['p1_name'], duel_data['p2_name']
    
    conn = get_db_connection(bot.id)
    def get_team(uid):
        return [{'id': r[0], 'name': r[1], 'dmg': r[2], 'hp': r[3], 'element': r[4], 'took_dmg': False} for r in conn.execute("SELECT c.card_id, c.name, c.damage, c.health, c.element FROM inventory i JOIN cards c ON i.card_id = c.card_id WHERE i.user_id = ? AND i.is_equipped = 1", (uid,)).fetchall()]
    team1, team2 = get_team(p1_id), get_team(p2_id)
    conn.close()
    
    if not team1 or not team2:
        err = "<b>❌ Ошибка: У одного из игроков пропали экипированные карты.</b>"
        try: await bot.send_message(chat_id, err, parse_mode="HTML")
        except: pass
        if is_private and chat2:
            try: await bot.send_message(chat2, err, parse_mode="HTML")
            except: pass
        return
        
    def render_status(t1, t2):
        lines = [f"🥊 Дуэль: {p1_name} VS {p2_name}\n\n👤 Команда {p1_name}:"]
        for c in t1: lines.append(f"• {c['name']} ({c['element']}) {'💥' if c['took_dmg'] else ''} (⚔️{c['dmg']} | ❤️{c['hp']})")
        lines.append("\n---\n👤 Команда {p2_name}:")
        for c in t2: lines.append(f"• {c['name']} ({c['element']}) {'💥' if c['took_dmg'] else ''} (⚔️{c['dmg']} | ❤️{c['hp']})")
        return "<b>" + "\n".join(lines) + "</b>"
        
    log_text = "<b>⚔️ Бой начинается!</b>"
    msgs = []
    
    try:
        s_m = await bot.send_message(chat_id, render_status(team1, team2), parse_mode="HTML")
        l_m = await bot.send_message(chat_id, log_text, parse_mode="HTML")
        msgs.append((s_m, l_m))
    except: pass

    if is_private and chat2:
        try:
            s_m2 = await bot.send_message(chat2, render_status(team1, team2), parse_mode="HTML")
            l_m2 = await bot.send_message(chat2, log_text, parse_mode="HTML")
            msgs.append((s_m2, l_m2))
        except: pass
    
    turn = 0
    while team1 and team2:
        await asyncio.sleep(5)
        turn += 1
        for c in team1 + team2: c['took_dmg'] = False
        
        atk_team, def_team, atk_p = (team1, team2, p1_name) if turn % 2 != 0 else (team2, team1, p2_name)
        attacker, defender = random.choice(atk_team), random.choice(def_team)
        
        mult = 1.2 if defender['element'] in ELEMENT_ADVANTAGES.get(attacker['element'], []) else (0.7 if attacker['element'] in ELEMENT_ADVANTAGES.get(defender['element'], []) else 1.0)
        dmg = max(1, int(attacker['dmg'] * mult))
        defender['hp'] -= dmg
        defender['took_dmg'] = True
        
        log_line = f"<b>📜 Ход {turn} ({atk_p}):\n{defender['name']} ({defender['element']}) -{dmg}❤️ от {attacker['name']} ({attacker['element']})"
        if defender['hp'] <= 0:
            log_line += f"\n💀 Карта {defender['name']} уничтожена!"
            def_team.remove(defender)
        log_line += "</b>"
            
        new_status = render_status(team1, team2)
        for s_m, l_m in msgs:
            try: await s_m.edit_text(new_status, parse_mode="HTML")
            except: pass
            try: await l_m.edit_text(log_line, parse_mode="HTML")
            except: pass
            
    win_id, lose_id, win_name, lose_name = (p1_id, p2_id, p1_name, p2_name) if team1 else (p2_id, p1_id, p2_name, p1_name)
    
    conn = get_db_connection(bot.id)
    w_res = conn.execute("SELECT trophies, balance FROM users WHERE user_id = ?", (win_id,)).fetchone()
    l_res = conn.execute("SELECT trophies, balance FROM users WHERE user_id = ?", (lose_id,)).fetchone()
    
    w_tr, w_bal = (w_res[0], w_res[1]) if w_res else (0, 0)
    l_tr, l_bal = (l_res[0], l_res[1]) if l_res else (0, 0)
    
    win_diff, loss_diff, coins_won = get_win_trophies(w_tr), get_loss_trophies(l_tr), get_win_coins(w_tr)
    
    conn.execute("UPDATE users SET trophies=?, balance=? WHERE user_id=?", (w_tr + win_diff, w_bal + coins_won, win_id))
    conn.execute("UPDATE users SET trophies=? WHERE user_id=?", (max(0, l_tr - loss_diff), lose_id))
    conn.commit()
    conn.close()

    final_log = f"<b>🏆 Бой окончен!\nПобедитель: {win_name} (+{win_diff} 🏆, +{coins_won} 💰)\nПроигравший: {lose_name} (-{loss_diff} 🏆)</b>"
    await asyncio.sleep(2)
    try: await bot.send_message(chat_id, final_log, parse_mode="HTML")
    except: pass
    if is_private and chat2:
        try: await bot.send_message(chat2, final_log, parse_mode="HTML")
        except: pass

async def catch_all_unknown(message: Message, bot: Bot, state: FSMContext):
    await state.clear()
    await message.answer("<b>🤔 Неизвестная команда или сообщение.\nВозвращаю в главное меню...</b>", parse_mode="HTML")
    await cmd_start(message, bot, state)

# === ЗАПУСК ===
async def run_bot(token: str, admin_id: int, is_startup: bool = False):
    try: bot_id = int(token.split(':')[0])
    except Exception: return
    init_db(bot_id, admin_id)
    dp = Dispatcher(storage=MemoryStorage())
    
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_profile, Command("profile"))
    dp.message.register(cmd_index, Command("index"))
    dp.message.register(cmd_inventory, Command("inventory"))
    dp.message.register(cmd_equip, Command("equip"))
    dp.message.register(cmd_duel, Command("duel"))
    dp.message.register(cmd_pvpsearch, Command("pvpsearch"))
    dp.message.register(cmd_top, Command("top"))
    dp.message.register(cmd_getcard, Command("getcard"))
    dp.message.register(cmd_addbot, Command("addbot"))
    dp.message.register(cmd_globalmessage, Command("globalmessage"))
    dp.message.register(cmd_addadmin, Command("addadmin"))
    dp.message.register(cmd_deladmin, Command("deladmin"))
    dp.message.register(cmd_events, Command("luckevent", "cooldownevent"))
    dp.message.register(cmd_events_space, F.text.startswith("/cooldown event "))
    dp.message.register(cmd_gomerchant, Command("gomerchant"))
    dp.message.register(cmd_closemerchant, Command("closemerchant"))
    
    dp.message.register(start_add_card, F.text == "Добавить карту", StateFilter(None), F.chat.type == "private")
    dp.message.register(cancel_action, F.text == "Отмена", F.chat.type == "private")
    dp.message.register(process_photo, AddCardState.waiting_for_photo, F.photo, F.chat.type == "private")
    dp.message.register(process_name, AddCardState.waiting_for_name, F.text, F.chat.type == "private")
    dp.message.register(process_weight, AddCardState.waiting_for_weight, F.text, F.chat.type == "private")
    dp.callback_query.register(process_rarity, AddCardState.waiting_for_rarity, F.data.startswith("rarity_"))
    dp.callback_query.register(process_element, AddCardState.waiting_for_element, F.data.startswith("element_"))
    dp.message.register(process_damage, AddCardState.waiting_for_damage, F.text, F.chat.type == "private")
    dp.message.register(process_health, AddCardState.waiting_for_health, F.text, F.chat.type == "private")
    dp.message.register(invalid_fsm_input, StateFilter(AddCardState.waiting_for_photo, AddCardState.waiting_for_name, AddCardState.waiting_for_weight, AddCardState.waiting_for_rarity, AddCardState.waiting_for_element, AddCardState.waiting_for_damage, AddCardState.waiting_for_health), F.chat.type == "private")
    
    dp.message.register(start_delete_card, F.text == "Удалить карту", StateFilter(None), F.chat.type == "private")
    dp.callback_query.register(process_delete_card, F.data.startswith("delcard_"))
    
    dp.callback_query.register(process_index_page, F.data.startswith("index_page_"))
    dp.callback_query.register(process_inv_page, F.data.startswith("inv_page_"))
    dp.callback_query.register(process_open_equip, F.data.startswith("open_equip_"))
    dp.callback_query.register(process_equip_select, F.data.startswith("equip_select_"))
    dp.callback_query.register(process_equip_card, F.data.startswith("eqcard_"))
    dp.callback_query.register(process_equip_auto, F.data.startswith("equip_auto_"))
    dp.callback_query.register(process_equip_clear, F.data.startswith("equip_clear_"))
    
    dp.callback_query.register(process_duel_acc, F.data.startswith("duel_acc_"))
    dp.callback_query.register(process_duel_dec, F.data.startswith("duel_dec_"))
    dp.callback_query.register(process_cancel_search, F.data == "cancel_search")
    dp.callback_query.register(process_buy_merch, F.data.startswith("buy_merch_"))
    
    dp.message.register(catch_all_unknown, StateFilter(None), F.chat.type == "private")
    dp.update.middleware(TrackerMiddleware())
    bot_instance = Bot(token=token)
    RUNNING_BOTS[bot_id] = bot_instance
    
    try:
        commands = [
            BotCommand(command="start", description="Перезапустить бота"),
            BotCommand(command="help", description="Список команд"),
            BotCommand(command="getcard", description="Выбить карту"),
            BotCommand(command="inventory", description="Твой инвентарь"),
            BotCommand(command="equip", description="Экипировка"),
            BotCommand(command="pvpsearch", description="Поиск боя (ЛС)"),
            BotCommand(command="duel", description="Вызов на бой (группа)"),
            BotCommand(command="top", description="Топ игроков"),
            BotCommand(command="index", description="Индекс карт"),
            BotCommand(command="profile", description="Твой профиль"),
            BotCommand(command="addbot", description="Создать своего бота")
        ]
        await bot_instance.set_my_commands(commands)
        await bot_instance.delete_webhook(drop_pending_updates=True)
        logging.info(f"Бот {bot_id} успешно запущен!")
        if is_startup: await broadcast(bot_instance, UPDATE_LOG_TEXT)
        asyncio.create_task(merchant_spawner(bot_instance))
        await dp.start_polling(bot_instance, allowed_updates=["message", "channel_post", "callback_query", "my_chat_member", "chat_member"])
    except Exception as e: logging.error(f"Ошибка при работе бота {bot_id}: {e}")
    finally:
        if bot_id in RUNNING_BOTS: del RUNNING_BOTS[bot_id]
        await bot_instance.session.close()

async def main():
    asyncio.create_task(run_bot(TOKEN, MAIN_ADMIN_ID, is_startup=True))
    init_db(MAIN_BOT_ID, MAIN_ADMIN_ID)
    conn = get_db_connection(MAIN_BOT_ID)
    child_bots = conn.execute("SELECT token, owner_id FROM child_bots").fetchall()
    conn.close()
    for c_token, c_owner in child_bots: asyncio.create_task(run_bot(c_token, c_owner, is_startup=True))
    asyncio.create_task(ad_broadcaster())
    print("Мульти-бот система запущена и работает!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
