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
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from PIL import Image, ImageOps, ImageDraw

# === КОНФИГУРАЦИЯ ===
TOKEN = "8563258546:AAFWcvQgLNHhIjKofPTceR_9IgchY1uSwMc"
try:
    MAIN_BOT_ID = int(TOKEN.split(":")[0])
except ValueError:
    print("Ошибка: Неверный формат токена основного бота.")
    sys.exit(1)

MAIN_ADMIN_ID = 1018561747
MAIN_CHANNEL = "@L1meYT"
COOLDOWN_SECONDS = 600  # 10 минут
UPDATE_LOG_TEXT = (
    "🚀 <b>Бот успешно запущен!</b>\n\n"
    "Добро пожаловать в новый проект. Следите за новостями!"
)

# Словарь цветов рамок для редкостей
RARITY_FRAME_COLORS = {
    "⬜️Обычная⬜️": "#808080",      # Серый
    "🟩Необычная🟩": "#008000",    # Зеленый
    "🟦Редкая🟦": "#0000FF",       # Синий
    "🟪Эпическая🟪": "#800080",    # Фиолетовый
    "🟨Легендарная🟨": "#FFD700",  # Золотой
    "🟥Мифическая🟥": "#FF0000",    # Красный
    "🔵Божественная🔵": "#87CEEB",  # Небесно-голубой
    "🟣Эксклюзивная🟣": "#9400D3"  # Темно-фиолетовый (Эксклюзивный)
}

logging.basicConfig(level=logging.INFO)

# Словарь для хранения всех запущенных инстансов ботов
RUNNING_BOTS = {}

# === НАСТРОЙКА ПУТЕЙ И ПАПОК ДЛЯ СОХРАНЕНИЯ ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'bot_data')

def get_db_path(bot_id: int) -> str:
    if bot_id == MAIN_BOT_ID:
        return os.path.join(DATA_DIR, 'cards_bot.db')
    return os.path.join(DATA_DIR, f'child_{bot_id}.db')

# === БАЗА ДАННЫХ ===
def init_db(bot_id: int, admin_id: int):
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    db_path = get_db_path(bot_id)
    conn = sqlite3.connect(db_path, timeout=15.0)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY, username TEXT,
            balance INTEGER DEFAULT 0, last_getcard INTEGER DEFAULT 0)''')
            
    cursor.execute('''CREATE TABLE IF NOT EXISTS cards (
            card_id INTEGER PRIMARY KEY AUTOINCREMENT, photo_id TEXT NOT NULL,
            name TEXT NOT NULL, weight REAL DEFAULT 1, rarity TEXT, reward INTEGER DEFAULT 0)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS inventory (
            user_id INTEGER, card_id INTEGER, amount INTEGER DEFAULT 0,
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

    # Миграции
    cursor.execute("PRAGMA table_info(users)")
    user_cols = [col[1] for col in cursor.fetchall()]
    if 'username' not in user_cols:
        cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")

    cursor.execute("PRAGMA table_info(cards)")
    card_cols = [col[1] for col in cursor.fetchall()]
    if 'rarity' not in card_cols:
        cursor.execute("ALTER TABLE cards ADD COLUMN rarity TEXT DEFAULT 'common'")
    if 'reward' not in card_cols:
        cursor.execute("ALTER TABLE cards ADD COLUMN reward INTEGER DEFAULT 0")
        
    cursor.execute("SELECT card_id, weight FROM cards WHERE reward = 0")
    old_cards = cursor.fetchall()
    for cid, cweight in old_cards:
        _, min_c, max_c = get_rarity_and_coins(cweight)
        fixed_reward = random.randint(min_c, max_c)
        cursor.execute("UPDATE cards SET reward = ? WHERE card_id = ?", (fixed_reward, cid))

    # Обновляем старые Галактические на Эксклюзивные
    cursor.execute("UPDATE cards SET rarity = '🟣Эксклюзивная🟣' WHERE rarity = '🌌Галактическая🌌'")

    conn.commit()
    conn.close()

def get_db_connection(bot_id: int):
    return sqlite3.connect(get_db_path(bot_id), timeout=15.0)

# === БЕЗОПАСНЫЙ MIDDLEWARE ДЛЯ ОТСЛЕЖИВАНИЯ ВСЕГО ===
class TrackerMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        try:
            msg = getattr(event, 'message', None)
            channel_post = getattr(event, 'channel_post', None)
            my_chat_member = getattr(event, 'my_chat_member', None)
            call = getattr(event, 'callback_query', None)

            chat = None
            user = None

            if msg:
                chat = msg.chat
                user = msg.from_user
            elif channel_post:
                chat = channel_post.chat
            elif my_chat_member:
                chat = my_chat_member.chat
            elif call:
                chat = call.message.chat if call.message else None
                user = call.from_user

            bot: Bot = data.get('bot')

            if chat and bot:
                conn = get_db_connection(bot.id)
                cursor = conn.cursor()
                
                cursor.execute("INSERT OR IGNORE INTO chats (chat_id, type) VALUES (?, ?)", (chat.id, chat.type))
                
                if user and not user.is_bot:
                    cursor.execute("INSERT OR IGNORE INTO users (user_id, balance, last_getcard) VALUES (?, 0, 0)", (user.id,))
                    if user.username:
                        cursor.execute("UPDATE users SET username = ? WHERE user_id = ?", (user.username, user.id))
                
                conn.commit()
                conn.close()
        except Exception as e:
            logging.error(f"TrackerMiddleware Error: {e}")
            
        return await handler(event, data)

# === УТИЛИТЫ ===
def get_rarity_and_coins(weight: float):
    if weight > 70: return "⬜️Обычная⬜️", 1, 10
    elif weight > 50: return "🟩Необычная🟩", 5, 25
    elif weight > 25: return "🟦Редкая🟦", 20, 50
    elif weight > 10: return "🟪Эпическая🟪", 30, 80
    elif weight > 1: return "🟨Легендарная🟨", 80, 120
    elif weight > 0.1: return "🟥Мифическая🟥", 100, 200
    elif weight > 0.01: return "🔵Божественная🔵", 350, 600
    else: return "🟣Эксклюзивная🟣", 800, 1200

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

async def broadcast(bot: Bot, text: str, reply_markup=None):
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
        reply_markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="сливы карточек тут", url="https://t.me/L1meYT")]
        ])

    success, failed = 0, 0
    
    if bot_id == MAIN_BOT_ID:
        all_targets.add(MAIN_CHANNEL)
        try:
            await bot.send_message(MAIN_CHANNEL, text, parse_mode="HTML")
            success += 1
        except Exception as e:
            logging.error(f"Не удалось отправить лог в {MAIN_CHANNEL}. Ошибка: {e}")
            failed += 1

    for target_id in all_targets:
        if bot_id == MAIN_BOT_ID and target_id in channel_ids or target_id == MAIN_CHANNEL:
            continue
            
        try:
            await bot.send_message(target_id, text, parse_mode="HTML", reply_markup=reply_markup)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
            
    return success, failed

async def ad_broadcaster():
    while True:
        await asyncio.sleep(10800)
        ad_text = (
            "🤖 <b>Понравился этот бот?</b>\n\n"
            "Ты можешь абсолютно бесплатно создать точно такого же бота для своей группы или чата!\n"
            "Переходи в наш официальный канал, там можно создать своего бота и узнать все подробности:"
        )
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Создать своего бота 🚀", url="https://t.me/L1meYT")]
        ])
        for b_id, b_instance in list(RUNNING_BOTS.items()):
            if b_id != MAIN_BOT_ID:
                try:
                    await broadcast(b_instance, ad_text, reply_markup=markup)
                except Exception as e:
                    logging.error(f"Ошибка рекламной рассылки дочернего бота {b_id}: {e}")

# === СОСТОЯНИЯ FSM ===
class AddCardState(StatesGroup):
    waiting_for_photo = State()
    waiting_for_name = State()
    waiting_for_weight = State()

# === КЛАВИАТУРЫ ===
def get_admin_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Добавить карту"), KeyboardButton(text="Удалить карту")]
        ],
        resize_keyboard=True
    )

def get_cards_delete_kb(cards_list):
    keyboard = []
    for card_id, name, rarity in cards_list:
        display_text = f"{rarity} {name}"
        keyboard.append([InlineKeyboardButton(text=display_text, callback_data=f"delcard_{card_id}")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ==========================================================
# === ЛОГИКА КОМАНД ===
# ==========================================================

async def cmd_addbot(message: Message, bot: Bot):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await smart_reply(message, 
            "🛠 <b>Создание своего бота</b>\n\n"
            "Использование: <code>/addbot [токен_бота]</code>\n\n"
            "1. Перейди в @BotFather и создай нового бота.\n"
            "2. Скопируй токен (длинная строка с цифрами и буквами).\n"
            "3. Отправь эту команду сюда вместе с токеном."
        )
        return
        
    token = args[1].strip()
    try:
        new_bot_id = int(token.split(':')[0])
    except ValueError:
        await smart_reply(message, "❌ Неверный формат токена. Скопируй токен из @BotFather полностью.")
        return
        
    if new_bot_id in RUNNING_BOTS:
        await smart_reply(message, "❌ Этот бот уже запущен в нашей системе!")
        return
        
    test_bot = Bot(token=token)
    try:
        me = await test_bot.get_me()
    except Exception:
        await smart_reply(message, "❌ Ошибка авторизации. Токен недействителен или удален.")
        return
    finally:
        await test_bot.session.close()
        
    main_conn = get_db_connection(MAIN_BOT_ID)
    main_cursor = main_conn.cursor()
    try:
        main_cursor.execute("INSERT INTO child_bots (bot_id, token, owner_id) VALUES (?, ?, ?)", 
                            (new_bot_id, token, message.from_user.id))
        main_conn.commit()
    except sqlite3.IntegrityError:
        await smart_reply(message, "❌ Этот бот уже зарегистрирован в базе!")
        main_conn.close()
        return
    main_conn.close()
    
    asyncio.create_task(run_bot(token, message.from_user.id, is_startup=False))
    await smart_reply(message, 
        f"✅ <b>Бот @{me.username} успешно создан и запущен!</b>\n\n"
        f"Ты назначен его Главным Администратором.\n"
        f"Все новые обновления функционала будут появляться на нем автоматически!"
    )

async def cmd_globalmessage(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await smart_reply(message, "Использование: /globalmessage [сообщение]")
        return
    
    broadcast_text = f"📢 <b>Глобальное уведомление:</b>\n\n{args[1]}"
    await smart_reply(message, "⏳ Начинаю глобальную рассылку по всем чатам и личным сообщениям...")
    
    success, failed = await broadcast(bot, broadcast_text)
    await smart_reply(message, f"✅ Рассылка успешно завершена!\nУспешно отправлено: <b>{success}</b>\nОшибок отправки: <b>{failed}</b>")

async def cmd_addadmin(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await smart_reply(message, "Использование: /addadmin [id или @username]")
        return
    target_id = get_user_id_by_input(args[1], bot.id)
    if not target_id:
        await smart_reply(message, "Пользователь не найден в базе данных этого бота.")
        return
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (target_id,))
    conn.commit()
    conn.close()
    await smart_reply(message, f"✅ Пользователь {args[1]} назначен администратором!")

async def cmd_deladmin(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await smart_reply(message, "Использование: /deladmin [id или @username]")
        return
    target_id = get_user_id_by_input(args[1], bot.id)
    if not target_id:
        await smart_reply(message, "Пользователь не найден.")
        return
    if bot.id == MAIN_BOT_ID and target_id == MAIN_ADMIN_ID:
        await smart_reply(message, "Нельзя удалить главного создателя!")
        return
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM admins WHERE user_id = ?", (target_id,))
    conn.commit()
    conn.close()
    await smart_reply(message, f"✅ Пользователь {args[1]} удален из администраторов.")

async def cmd_events(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    cmd = message.text.split()[0].lower()
    args = message.text.split()[1:]
    if len(args) < 2:
        await smart_reply(message, f"Использование: {cmd} [множитель] [время в минутах]")
        return
    try:
        multiplier = float(args[0])
        minutes = int(args[1])
    except ValueError:
        await smart_reply(message, "Множитель и время должны быть числами!")
        return
    event_map = {
        "/luckevent": ("luck", "🍀 <b>Активирован эвент УДАЧИ!</b>\nШанс на выпадение редких карт увеличен в"),
        "/cooldownevent": ("cooldown", "⏳ <b>Активирован эвент СКОРОСТИ!</b>\nВремя перезарядки карт уменьшено в"),
        "/moneyevent": ("money", "💰 <b>Активирован эвент БОГАТСТВА!</b>\nКоличество монет за карты увеличено в")
    }
    ev_type, ev_text = event_map[cmd]
    set_event(ev_type, multiplier, minutes, bot.id)
    await smart_reply(message, f"✅ Эвент {ev_type} успешно запущен. Начинаю рассылку...")
    broadcast_text = f"{ev_text} <b>{multiplier}x</b> на {minutes} минут!"
    asyncio.create_task(broadcast(bot, broadcast_text))

async def cmd_events_space(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    parts = message.text.split()
    if len(parts) < 4:
        await smart_reply(message, "Ошибка формата. Пример: /cooldown event 2 60")
        return
    cmd = parts[0]
    try:
        multiplier = float(parts[2])
        minutes = int(parts[3])
    except ValueError:
        await smart_reply(message, "Множитель и время должны быть числами!")
        return
    if cmd == "/cooldown":
        set_event("cooldown", multiplier, minutes, bot.id)
        text = f"⏳ <b>Активирован эвент СКОРОСТИ!</b>\nВремя перезарядки карт уменьшено в <b>{multiplier}x</b> на {minutes} минут!"
    else:
        set_event("money", multiplier, minutes, bot.id)
        text = f"💰 <b>Активирован эвент БОГАТСТВА!</b>\nКоличество монет за карты увеличено в <b>{multiplier}x</b> на {minutes} минут!"
    await smart_reply(message, f"✅ Эвент запущен. Начинаю рассылку...")
    asyncio.create_task(broadcast(bot, text))

async def start_add_card(message: Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    await message.answer("Отправь фото новой карты:", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Отмена")]], resize_keyboard=True))
    await state.set_state(AddCardState.waiting_for_photo)

async def cancel_action(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if is_admin(message.from_user.id, bot.id):
        await message.answer("Действие отменено.", reply_markup=get_admin_kb())
    else:
        await message.answer("Действие отменено.")

async def process_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await state.update_data(photo_id=photo_id)
    await message.answer("Отлично! Теперь введи название карты:")
    await state.set_state(AddCardState.waiting_for_name)

async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    
    info_text = (
        "Принято. Теперь введи шанс (вес) карты (например: 75, 20.5, 0.5 или 0.005).\n"
        "Бот сам определит её редкость и награду по этой таблице:\n\n"
        "<code>"
        "Вес > 70   | ⬜️Обычная       | 1-10 💰\n"
        "Вес > 50   | 🟩Необычная     | 5-25 💰\n"
        "Вес > 25   | 🟦Редкая        | 20-50 💰\n"
        "Вес > 10   | 🟪Эпическая     | 30-80 💰\n"
        "Вес > 1    | 🟨Легендарная   | 80-120 💰\n"
        "Вес > 0.1  | 🟥Мифическая    | 100-200 💰\n"
        "Вес > 0.01 | 🔵Божественная  | 350-600 💰\n"
        "Вес ≤ 0.01 | 🟣Эксклюзивная  | 800-1200💰\n"
        "</code>"
    )
    await message.answer(info_text, parse_mode="HTML")
    await state.set_state(AddCardState.waiting_for_weight)

async def process_weight(message: Message, state: FSMContext, bot: Bot):
    try:
        weight_str = message.text.replace(",", ".")
        weight = float(weight_str)
    except ValueError:
        await message.answer("Вес должен быть числом! Попробуй еще раз:")
        return
    data = await state.get_data()
    
    rarity, min_coins, max_coins = get_rarity_and_coins(weight)
    fixed_reward = random.randint(min_coins, max_coins)
    
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("INSERT INTO cards (photo_id, name, weight, rarity, reward) VALUES (?, ?, ?, ?, ?)", 
                   (data['photo_id'], data['name'], weight, rarity, fixed_reward))
    conn.commit()
    conn.close()
    
    await message.answer(f"✅ Карта «{data['name']}» успешно создана!\nУстановленный вес: {weight}\nАвтоматическая редкость: {rarity}\nФиксированная награда: {fixed_reward} монет", reply_markup=get_admin_kb())
    await state.clear()

async def start_delete_card(message: Message, bot: Bot):
    if not is_admin(message.from_user.id, bot.id): return
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT card_id, name, rarity FROM cards")
    cards = cursor.fetchall()
    conn.close()
    if not cards:
        await message.answer("В базе пока нет добавленных карт.")
        return
    await message.answer("Нажми на карту, чтобы удалить её навсегда:", reply_markup=get_cards_delete_kb(cards))

async def process_delete_card(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id, bot.id):
        await callback.answer("У вас нет прав!", show_alert=True)
        return
    card_id = int(callback.data.split("_")[1])
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cards WHERE card_id = ?", (card_id,))
    cursor.execute("DELETE FROM inventory WHERE card_id = ?", (card_id,))
    conn.commit()
    conn.close()
    await callback.answer("Карта удалена!", show_alert=True)
    current_keyboard = callback.message.reply_markup.inline_keyboard
    new_keyboard = []
    for row in current_keyboard:
        new_row = [btn for btn in row if btn.callback_data != callback.data]
        if new_row: new_keyboard.append(new_row)
    if new_keyboard:
        try: await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=new_keyboard))
        except TelegramBadRequest: pass
    else:
        await callback.message.edit_text("✅ Все карты из этого списка были удалены.")

async def cmd_start(message: Message, bot: Bot):
    if is_admin(message.from_user.id, bot.id) and message.chat.type == "private":
        await smart_reply(message, "Добро пожаловать в панель администратора!\nВведите /help для просмотра всех команд.", reply_markup=get_admin_kb())
    else:
        await smart_reply(message, "Привет! Я карточный бот.\nИспользуй команду /getcard, чтобы испытать удачу и выбить карту!\nСписок всех команд: /help")

async def cmd_help(message: Message, bot: Bot):
    user_text = (
        "📜 <b>Список доступных команд:</b>\n\n"
        "🎮 <b>Основные:</b>\n"
        "• /start — Перезапустить бота\n"
        "• /help — Показать это меню\n"
        "• /getcard — Выбить случайную карту\n"
        "• /index — Посмотреть индекс всех карт\n"
        "• /profile — Посмотреть свой профиль и баланс\n"
        "• /addbot — Создать своего собственного бота\n"
    )
    admin_text = (
        "\n👑 <b>Для администраторов:</b>\n"
        "• /addadmin [id или @username] — Выдать админку\n"
        "• /deladmin [id или @username] — Забрать админку\n"
        "• /globalmessage [текст] — Глобальная рассылка во все чаты\n"
        "• /luckevent [множитель] [минуты] — Эвент УДАЧИ\n"
        "• /cooldownevent [множитель] [минуты] — Эвент СКОРОСТИ\n"
        "• /moneyevent [множитель] [минуты] — Эвент БОГАТСТВА\n\n"
        "<i>(Кнопки «Добавить карту» и «Удалить карту» доступны в панели /start)</i>"
    )
    if is_admin(message.from_user.id, bot.id):
        await smart_reply(message, user_text + admin_text)
    else:
        await smart_reply(message, user_text)

async def cmd_profile(message: Message, bot: Bot):
    user_id = message.from_user.id
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    user_data = cursor.fetchone()
    
    cursor.execute("SELECT COUNT(card_id) FROM inventory WHERE user_id = ? AND amount > 0", (user_id,))
    unlocked_cards = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(card_id) FROM cards")
    total_cards = cursor.fetchone()[0]
    conn.close()
    
    balance = user_data[0] if user_data else 0
    mention = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    
    text = (
        f"👤 <b>Профиль игрока {mention}</b>\n\n"
        f"💳 Твой ID: <code>{user_id}</code>\n"
        f"💰 Баланс: <b>{balance} монет</b>\n"
        f"🎒 Открыто карт: <b>{unlocked_cards}/{total_cards}</b>\n\n"
        f"<i>💡 Выбивай больше карт командой /getcard, чтобы пополнить счет!</i>"
    )
    await smart_reply(message, text)

async def cmd_getcard(message: Message, bot: Bot):
    user_id = message.from_user.id
    current_time = int(time.time())
    
    conn = get_db_connection(bot.id)
    cursor = conn.cursor()
    cursor.execute("SELECT balance, last_getcard FROM users WHERE user_id = ?", (user_id,))
    user_data = cursor.fetchone()
    if not user_data:
        cursor.execute("INSERT INTO users (user_id, balance, last_getcard) VALUES (?, 0, 0)", (user_id,))
        user_data = (0, 0)
    
    active_cooldown = COOLDOWN_SECONDS
    cd_mult = get_active_event("cooldown", bot.id)
    if cd_mult and cd_mult > 0:
        active_cooldown = int(COOLDOWN_SECONDS / cd_mult)
        
    time_passed = current_time - user_data[1]
    if time_passed < active_cooldown:
        remaining = active_cooldown - time_passed
        minutes = remaining // 60
        seconds = remaining % 60
        await smart_reply(message, f"⏳ Карты перезаряжаются.\nПодожди еще <b>{minutes} мин {seconds} сек</b>.")
        conn.close()
        return
        
    cursor.execute("SELECT card_id, photo_id, name, weight, rarity, reward FROM cards")
    cards = cursor.fetchall()
    if not cards:
        await smart_reply(message, "Бот пока пуст. Администратор еще не добавил карты!")
        conn.close()
        return
        
    luck_mult = get_active_event("luck", bot.id)
    weights = []
    for c in cards:
        base_weight = c[3]
        rarity = c[4]
        if luck_mult and rarity != "⬜️Обычная⬜️":
            weights.append(base_weight * luck_mult)
        else:
            weights.append(base_weight)
            
    chosen_card = random.choices(cards, weights=weights, k=1)[0]
    card_id, card_photo, card_name, card_weight, card_rarity, card_reward = chosen_card
    
    cursor.execute("SELECT amount FROM inventory WHERE user_id = ? AND card_id = ?", (user_id, card_id))
    inv_check = cursor.fetchone()
    is_new = not inv_check or inv_check[0] == 0
    
    if is_new:
        display_name = f"{card_name} 🔥NEW🔥"
    else:
        display_name = card_name
    
    final_reward = card_reward
    money_mult = get_active_event("money", bot.id)
    if money_mult: 
        final_reward = int(final_reward * money_mult)
        
    new_balance = user_data[0] + final_reward
    cursor.execute("UPDATE users SET balance = ?, last_getcard = ? WHERE user_id = ?", (new_balance, current_time, user_id))
    
    cursor.execute('''INSERT INTO inventory (user_id, card_id, amount) 
                      VALUES (?, ?, 1) 
                      ON CONFLICT(user_id, card_id) 
                      DO UPDATE SET amount = inventory.amount + 1''', (user_id, card_id))
    
    conn.commit()
    conn.close()
    
    mention = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    caption_text = f"🎉 {mention}, тебе выпала карта!\n\n🃏 {display_name}\n💎 Редкость • {card_rarity}\n💰 Монеты • {final_reward}"
    
    original_photo_bytes = BytesIO()
    try:
        file = await bot.get_file(card_photo)
        file_path = file.file_path
        await bot.download_file(file_path, original_photo_bytes)
        original_photo_bytes.seek(0)
    except Exception as e:
        logging.error(f"Не удалось скачать фото юнита {card_photo}: {e}")
        await smart_reply(message, text=caption_text, photo=card_photo)
        return

    framed_photo_input = await apply_frame(original_photo_bytes, card_rarity)
    await smart_reply(message, text=caption_text, photo=framed_photo_input)

# --- ЛОГИКА ИНДЕКСА (/index) ---
async def get_index_page(bot_id: int, user_id: int, page: int = 1):
    conn = get_db_connection(bot_id)
    cursor = conn.cursor()
    
    cursor.execute("SELECT card_id, name, weight, rarity, reward FROM cards ORDER BY weight DESC")
    cards = cursor.fetchall()
    
    if not cards:
        conn.close()
        return "📭 В игре пока нет ни одной добавленной карты.", None
        
    total_weight = sum(c[2] for c in cards)
    
    cursor.execute("SELECT card_id FROM inventory WHERE user_id = ? AND amount > 0", (user_id,))
    unlocked_ids = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT card_id, SUM(amount) FROM inventory GROUP BY card_id")
    exists_dict = {row[0]: row[1] for row in cursor.fetchall()}
    conn.close()
    
    ITEMS_PER_PAGE = 7
    total_pages = (len(cards) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    if page < 1: page = 1
    if page > total_pages: page = total_pages
    
    start_idx = (page - 1) * ITEMS_PER_PAGE
    end_idx = start_idx + ITEMS_PER_PAGE
    page_cards = cards[start_idx:end_idx]
    
    lines = [f"📖 <b>Индекс всех карт (Страница {page}/{total_pages})</b>\n"]
    
    for idx, card in enumerate(page_cards, start=start_idx + 1):
        c_id, c_name, c_weight, c_rarity, c_reward = card
        
        chance_pct = (c_weight / total_weight) * 100 if total_weight > 0 else 0
        chance_str = f"{chance_pct:.2f}".rstrip('0').rstrip('.')
        if chance_str == "": chance_str = "0"
        
        display_name = c_name if c_id in unlocked_ids else "???"
        exists_count = exists_dict.get(c_id, 0)
        
        lines.append(f"{idx}. <b>{display_name}</b>")
        lines.append(f"💎 {c_rarity} | 🎲 {chance_str}%")
        lines.append(f"💰 Монеты • {c_reward}")
        lines.append(f"💫Существует: {exists_count}💫")
        lines.append("━━━━━━━━━━━━━━━━━━")
        
    text = "\n".join(lines)
    
    kb = []
    nav_row = []
    if page > 1:
        nav_row.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"index_page_{page-1}"))
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"index_page_{page+1}"))
        
    if nav_row:
        kb.append(nav_row)
        
    markup = InlineKeyboardMarkup(inline_keyboard=kb) if kb else None
    return text, markup

async def cmd_index(message: Message, bot: Bot):
    text, markup = await get_index_page(bot.id, message.from_user.id, page=1)
    await smart_reply(message, text, reply_markup=markup)

async def process_index_page(callback: CallbackQuery, bot: Bot):
    page = int(callback.data.split("_")[2])
    text, markup = await get_index_page(bot.id, callback.from_user.id, page)
    try:
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    except TelegramBadRequest:
        pass
    await callback.answer()

# --- ПЕРЕХВАТ НЕИЗВЕСТНЫХ КОМАНД ---
async def catch_all_unknown(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    await message.answer("🤔 Неизвестная команда или сообщение.\nВозвращаю в главное меню...")
    await cmd_start(message, bot)

# === ФУНКЦИЯ ЗАПУСКА КОНКРЕТНОГО БОТА ===
async def run_bot(token: str, admin_id: int, is_startup: bool = False):
    try:
        bot_id = int(token.split(':')[0])
    except Exception as e:
        logging.error(f"Неверный токен {token}: {e}")
        return

    init_db(bot_id, admin_id)
    dp = Dispatcher(storage=MemoryStorage())
    
    dp.message.register(cmd_start, CommandStart())
    dp.message.register(cmd_help, Command("help"))
    dp.message.register(cmd_profile, Command("profile"))
    dp.message.register(cmd_index, Command("index"))
    dp.message.register(cmd_getcard, Command("getcard"))
    dp.message.register(cmd_addbot, Command("addbot"))
    dp.message.register(cmd_globalmessage, Command("globalmessage"))
    dp.message.register(cmd_addadmin, Command("addadmin"))
    dp.message.register(cmd_deladmin, Command("deladmin"))
    dp.message.register(cmd_events, Command("luckevent", "cooldownevent", "moneyevent"))
    dp.message.register(cmd_events_space, F.text.startswith("/cooldown event ") | F.text.startswith("/money event "))
    
    dp.message.register(start_add_card, F.text == "Добавить карту", F.chat.type == "private")
    dp.message.register(cancel_action, F.text == "Отмена", F.chat.type == "private")
    dp.message.register(process_photo, AddCardState.waiting_for_photo, F.photo, F.chat.type == "private")
    dp.message.register(process_name, AddCardState.waiting_for_name, F.text, F.chat.type == "private")
    dp.message.register(process_weight, AddCardState.waiting_for_weight, F.text, F.chat.type == "private")
    dp.message.register(start_delete_card, F.text == "Удалить карту", F.chat.type == "private")
    
    dp.callback_query.register(process_delete_card, F.data.startswith("delcard_"))
    dp.callback_query.register(process_index_page, F.data.startswith("index_page_"))
    
    dp.message.register(catch_all_unknown, F.chat.type == "private")
    
    dp.update.middleware(TrackerMiddleware())
    
    bot_instance = Bot(token=token)
    RUNNING_BOTS[bot_id] = bot_instance
    
    try:
        commands = [
            BotCommand(command="start", description="Перезапустить бота"),
            BotCommand(command="help", description="Список всех команд"),
            BotCommand(command="getcard", description="Выбить карту"),
            BotCommand(command="index", description="Индекс всех карт"),
            BotCommand(command="profile", description="Профиль и баланс"),
            BotCommand(command="addbot", description="Создать своего бота")
        ]
        await bot_instance.set_my_commands(commands)
        await bot_instance.delete_webhook(drop_pending_updates=True)
        
        logging.info(f"Бот {bot_id} успешно запущен!")
        
        if is_startup:
            await broadcast(bot_instance, UPDATE_LOG_TEXT)
            
        allowed_updates = ["message", "channel_post", "callback_query", "my_chat_member", "chat_member"]
        await dp.start_polling(bot_instance, allowed_updates=allowed_updates)
    except Exception as e:
        logging.error(f"Ошибка при работе бота {bot_id}: {e}")
    finally:
        if bot_id in RUNNING_BOTS:
            del RUNNING_BOTS[bot_id]
        await bot_instance.session.close()

# === ЗАПУСК СИСТЕМЫ ===
async def main():
    asyncio.create_task(run_bot(TOKEN, MAIN_ADMIN_ID, is_startup=True))
    
    init_db(MAIN_BOT_ID, MAIN_ADMIN_ID)
    conn = get_db_connection(MAIN_BOT_ID)
    cursor = conn.cursor()
    cursor.execute("SELECT token, owner_id FROM child_bots")
    child_bots = cursor.fetchall()
    conn.close()
    
    for c_token, c_owner in child_bots:
        asyncio.create_task(run_bot(c_token, c_owner, is_startup=True))
        
    asyncio.create_task(ad_broadcaster())
    
    print("Мульти-бот система запущена и работает!")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
