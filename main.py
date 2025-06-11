import os
import re
import sqlite3
from datetime import datetime, timedelta
import pytz
from aiogram import Bot, Dispatcher, F, Router, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import Message, InlineKeyboardButton, ReplyKeyboardRemove, InputMediaPhoto, InlineKeyboardMarkup
import cv2
import pytesseract
from PIL import Image
from typing import Union
import traceback
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramForbiddenError
import signal
import sys
import asyncio


def load_config():
    config = {}
    with open('config.txt', 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()
                
                if value.startswith('[') and value.endswith(']'):
                    
                    value = [int(x.strip()) for x in value[1:-1].split(',') if x.strip()]
                elif value.isdigit():
                    value = int(value)
                elif value.lower() in ('true', 'false'):
                    value = value.lower() == 'true'
                elif value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                config[key] = value
    return config


config = load_config()


is_shutting_down = False


REPORT_TIME = "22:30"  

async def shutdown(dispatcher: Dispatcher, bot: Bot):
    """Корректное завершение работы бота"""
    global is_shutting_down
    is_shutting_down = True
    
    print("\nПолучен сигнал на завершение работы...")
    
    
    try:
        if 'conn' in globals() and conn:
            conn.close()
            print("Соединение с базой данных закрыто")
    except Exception as e:
        print(f"Ошибка при закрытии базы данных: {e}")
    
    
    try:
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]
        print(f"Отменено {len(tasks)} задач")
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"Ошибка при отмене задач: {e}")
    
    
    try:
        await bot.session.close()
        print("Сессия бота закрыта")
    except Exception as e:
        print(f"Ошибка при закрытии сессии бота: {e}")
    
    print("Бот успешно завершил работу")

def handle_sigint(signum, frame):
    """Обработчик сигнала SIGINT (Ctrl+C)"""
    print("\nПолучен сигнал на завершение работы (Ctrl+C)...")
    sys.exit(0)

async def safe_send_message(chat_id, text, **kwargs):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(1 + attempt)

def init_db():
    conn = sqlite3.connect(config['DB_NAME'])
    cursor = conn.cursor()

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS office_chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    chat_id INTEGER,
                    UNIQUE(user_id, chat_id))''')

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS drops_chats (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER)''')

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS number_topics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    topic_id INTEGER,
                    topic_type TEXT,
                    topic_name TEXT,
                    custom_name TEXT,
                    required_numbers INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1)''')

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS topic_office_links (
                    topic_id INTEGER,
                    office_chat_id INTEGER,
                    PRIMARY KEY (topic_id, office_chat_id))''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS number_requests (
                drops_chat INTEGER PRIMARY KEY,
                required INTEGER DEFAULT 0,
                fulfilled INTEGER DEFAULT 0)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS num_requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                office_chat_id INTEGER,
                drops_chat_id INTEGER,
                request_message_id INTEGER,
                status TEXT DEFAULT 'pending')''')

    
    cursor.execute('DROP TABLE IF EXISTS last_messages')
    cursor.execute('''CREATE TABLE IF NOT EXISTS last_messages (
                    chat_id INTEGER,
                    message_id INTEGER,
                    topic_id INTEGER,
                    PRIMARY KEY (chat_id, topic_id))''')

    
    cursor.execute('DROP TABLE IF EXISTS phone_messages')
    cursor.execute('''CREATE TABLE IF NOT EXISTS phone_messages (
                phone TEXT PRIMARY KEY,
                user_message_id INTEGER,
                confirmation_message_id INTEGER,
                chat_id INTEGER,
                user_id INTEGER,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registration_time TEXT,
                report_message_id INTEGER,
                status TEXT,
                topic_name TEXT,
                topic_id INTEGER,
                slet_time TEXT,
                topic_type TEXT)''')

    
    try:
        cursor.execute('ALTER TABLE phone_messages ADD COLUMN topic_type TEXT')
    except sqlite3.OperationalError:
        pass  

    
    cursor.execute('''CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY)''')

    
    for column in ['registration_time', 'report_message_id', 'status', 'topic_name']:
        try:
            cursor.execute(f'ALTER TABLE phone_messages ADD COLUMN {column} {("TEXT" if column in ["registration_time", "status", "topic_name"] else "INTEGER")}')
        except sqlite3.OperationalError:
            pass

    
    initial_allowed_users = [7699005037, 7699005037]  
    for user_id in initial_allowed_users:
        cursor.execute('INSERT OR IGNORE INTO allowed_users (user_id) VALUES (?)', (user_id,))

    conn.commit()
    return conn

async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ['creator', 'administrator']
    except Exception as e:
        print(f"Ошибка проверки статуса администратора: {e}")
        return False

async def is_allowed_user(user_id: int) -> bool:
    """Проверяет, является ли пользователь разрешенным."""
    cursor.execute('SELECT COUNT(*) FROM allowed_users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()[0] > 0

conn = init_db()
cursor = conn.cursor()

BOT_TOKEN = "7605177463:AAFIt3GqOFfIp4-E0bzhFXtBBOu473brKoE"

router = Router()
bot = Bot(token=config['BOT_TOKEN'])
number_processing_enabled = True
pending_numbers = {}
accepted_numbers = {}

class Form(StatesGroup):
    wait_for_chat_ids = State()

def get_user_data(user_id):
    
    cursor.execute('SELECT chat_id FROM office_chats WHERE user_id = ?', (user_id,))
    office_chats = [row[0] for row in cursor.fetchall()]
    
    
    cursor.execute('SELECT chat_id FROM drops_chats WHERE user_id = ?', (user_id,))
    drops_chat = cursor.fetchone()
    drops_chat = drops_chat[0] if drops_chat else None
    
    return office_chats, drops_chat

def save_user_data(user_id, chat_ids):
    
    drops_chat = chat_ids[-1]
    office_chats = chat_ids[:-1]
    
    
    cursor.execute('DELETE FROM office_chats WHERE user_id = ?', (user_id,))
    cursor.execute('DELETE FROM drops_chats WHERE user_id = ?', (user_id,))
    
    
    for chat_id in office_chats:
        cursor.execute('INSERT INTO office_chats (user_id, chat_id) VALUES (?, ?)',
                      (user_id, chat_id))
    
    
    cursor.execute('INSERT INTO drops_chats (user_id, chat_id) VALUES (?, ?)',
                  (user_id, drops_chat))
    
    conn.commit()

def is_office_chat(chat_id):
    cursor.execute('SELECT COUNT(*) FROM office_chats WHERE chat_id = ?', (chat_id,))
    return cursor.fetchone()[0] > 0

def is_drops_chat(chat_id):
    cursor.execute('SELECT COUNT(*) FROM drops_chats WHERE chat_id = ?', (chat_id,))
    return cursor.fetchone()[0] > 0

def get_drops_chat_for_office(office_chat_id):
    cursor.execute('''SELECT dc.chat_id 
                    FROM drops_chats dc
                    JOIN office_chats oc ON dc.user_id = oc.user_id
                    WHERE oc.chat_id = ?''', (office_chat_id,))
    result = cursor.fetchone()
    return result[0] if result else None

def get_office_chats_for_drops(drops_chat_id):
    cursor.execute('''SELECT oc.chat_id 
                    FROM office_chats oc
                    JOIN drops_chats dc ON oc.user_id = dc.user_id
                    WHERE dc.chat_id = ?''', (drops_chat_id,))
    return [row[0] for row in cursor.fetchall()]

def extract_phone(text: str) -> Union[str, None]:
    phone = re.findall(r'(?:\+7|7|8)?[\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}', text)
    if not phone:
        return None
    
    phone = phone[0].replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
    
    if phone.startswith('+7'):
        return phone
    elif phone.startswith('7'):
        return f'+7{phone[1:]}'
    elif phone.startswith('8'):
        return f'+7{phone[1:]}'
    else:
        return f'+7{phone}' if len(phone) == 10 else None

def get_settings(chat_id):
    cursor.execute('SELECT topic_id FROM number_topics WHERE chat_id = ? AND topic_type = "reports" AND is_active = 1', (chat_id,))
    reports_topic = cursor.fetchone()
    return reports_topic[0] if reports_topic else None

def save_settings(chat_id, reports_topic=None):
    if reports_topic is not None:
        cursor.execute('UPDATE number_topics SET is_active = 0 WHERE chat_id = ? AND topic_type = "reports"', (chat_id,))
        
        cursor.execute('DELETE FROM number_topics WHERE chat_id = ? AND topic_id = ?', (chat_id, reports_topic))
        cursor.execute('INSERT INTO number_topics (chat_id, topic_id, topic_type, custom_name, is_active) VALUES (?, ?, ?, ?, 1)',
                      (chat_id, reports_topic, "reports", "Отчеты", 1))
    conn.commit()

@router.message(Command("resetdb"))
async def cmd_resetdb(message: Message, state: FSMContext):
    print(f"Received /resetdb from user {message.from_user.id} in chat {message.chat.id}")
    try:
        if message.chat.type != 'private':
            await message.answer("❌ Команда доступна только в личных сообщениях.")
            return
        if not await is_allowed_user(message.from_user.id):
            await message.answer("❌ У вас нет прав для выполнения этой команды.")
            return
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Да", callback_data="resetdb_confirm"),
             InlineKeyboardButton(text="Нет", callback_data="resetdb_cancel")]
        ])
        await message.answer("⚠️ Вы уверены, что хотите очистить базу данных и перезапустить бота?", reply_markup=keyboard)
    except Exception as e:
        print(f"Error in /resetdb: {e}")

@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    if message.chat.type == 'private':
        if await is_allowed_user(message.from_user.id):
            await message.answer(
                "👋 Добро пожаловать! Для настройки:\n"
                "1. Узнайте ID чатов через @username_to_id_bot\n"
                "2. Введите ID офисных чатов и ID дроп-чата через запятую\n"
                "Формат: <code>ID_офис1, ID_офис2, ..., ID_дропы</code>\n\n"
                "Пример:\n<code>-100111, -100222, -100333, -100444</code>",
                parse_mode="HTML"
            )
            await state.set_state(Form.wait_for_chat_ids)
        else:
            await message.answer(
                "🔒 Это приватный бот\n\n"
                "❌ Сторонние пользователи не могут использовать данного бота.\n\n",
                parse_mode="HTML"
            )
    elif message.chat.type in ['group', 'supergroup']:
        user_id = message.from_user.id
        member = await bot.get_chat_member(message.chat.id, user_id)
        is_admin = member.status in ['creator', 'administrator']
        is_allowed = await is_allowed_user(user_id)

        if not (is_admin or is_allowed):
            await message.answer("❌ Только администраторы могут использовать эту команду!")
            return

        number_processing_enabled = True
        await message.answer(
            "👋 Welcome to the mediator bot!\n\n"
            "This bot helps manage phone numbers and codes between office and drops groups.\n"
            "To configure the bot, use the /settings command (admins and allowed users only)."
        )

@router.message(Command("stop"))
async def cmd_stop(message: Message):
    global number_processing_enabled
    if not await is_allowed_user(message.from_user.id):
        await message.answer("Только разрешенные пользователи могут использовать эту команду.")
        return
    number_processing_enabled = False
    await message.answer("Прием номеров остановлен. Используйте /start для возобновления.")

@router.message(Command("settings"))
async def cmd_settings(message: Message):
    print(f"Получена команда /settings от пользователя {message.from_user.id} в чате {message.chat.id}")
    
    
    if not await is_admin(message.chat.id, message.from_user.id):
        print(f"Пользователь {message.from_user.id} не является администратором")
        await message.answer("❌ Только администраторы могут использовать эту команду!")
        return

    
    try:
        cursor.execute('SELECT COUNT(*) FROM drops_chats WHERE chat_id = ?', (message.chat.id,))
        is_drops = cursor.fetchone()[0] > 0
        print(f"Чат {message.chat.id} является дроп-чатом: {is_drops}")
    except Exception as e:
        print(f"Ошибка при проверке типа чата: {e}")
        await message.answer("❌ Ошибка при проверке типа чата")
        return
    
    if is_drops:
        try:
            builder = InlineKeyboardBuilder()
            
            
            current_topic_id = message.message_thread_id if message.message_thread_id else None
            
            
            builder.row(
                InlineKeyboardButton(text="1/8", callback_data=f"topic_menu_1_8_{current_topic_id}"),
                InlineKeyboardButton(text="1/16", callback_data=f"topic_menu_1_16_{current_topic_id}")
            )
            builder.row(
                InlineKeyboardButton(text="7/1", callback_data=f"topic_menu_7_1_{current_topic_id}"),
                InlineKeyboardButton(text="20-25", callback_data=f"topic_menu_20_25_{current_topic_id}")
            )
            builder.row(
                InlineKeyboardButton(text="📊 Отчеты", callback_data=f"set_reports_{current_topic_id}")
            )
            
            await message.answer(
                "⚙️ <b>Настройки тем</b>\n\n"
                "Выберите тип темы для текущего чата.\n"
                "<i>Важно: Темы должны быть предварительно созданы администратором чата.</i>",
                parse_mode="HTML",
                reply_markup=builder.as_markup()
            )
            print("Настройки успешно отправлены")
        except Exception as e:
            print(f"Ошибка при отправке настроек: {e}")
            await message.answer("❌ Ошибка при отправке настроек")
    else:
        print(f"Чат {message.chat.id} не является дроп-чатом")
        await message.answer("❌ Эта команда доступна только в чатах дропов!")

@router.callback_query(F.data.startswith("set_drops_"))
async def set_drops_topic(callback: types.CallbackQuery):
    try:
        topic_id = int(callback.data.split("_")[2])
        
        cursor.execute('DELETE FROM number_topics WHERE chat_id = ? AND topic_id = ?', (callback.message.chat.id, topic_id))
        
        cursor.execute(
            'INSERT INTO number_topics (chat_id, topic_id, topic_name, is_active) '
            'VALUES (?, ?, ?, 1)',
            (callback.message.chat.id, topic_id, "drops")
        )
        conn.commit()
        await callback.answer("✅ Тема приемки успешно установлена!")
        await callback.message.delete()
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")

@router.callback_query(F.data.startswith("set_reports_"))
async def set_reports_topic(callback: types.CallbackQuery):
    try:
        topic_id = int(callback.data.split("_")[2])
        
        cursor.execute('DELETE FROM number_topics WHERE chat_id = ? AND topic_id = ?', (callback.message.chat.id, topic_id))
        
        cursor.execute(
            'INSERT INTO number_topics (chat_id, topic_id, topic_type, topic_name, is_active) '
            'VALUES (?, ?, ?, ?, 1)',
            (callback.message.chat.id, topic_id, "reports", "Отчеты")
        )
        conn.commit()
        await callback.answer("✅ Тема отчетов успешно установлена!")
        await callback.message.delete()
    except Exception as e:
        print(f"Ошибка в set_reports_topic: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

async def forward_number_to_office(phone, original_message, drops_chat_id):
    try:
        
        cursor.execute('''SELECT request_id, office_chat_id, request_message_id 
                        FROM num_requests 
                        WHERE status = 'pending' 
                        AND drops_chat_id = ? 
                        LIMIT 1''', (drops_chat_id,))  
        
        request = cursor.fetchone()
        
        if not request:
            await original_message.reply("⚠️ Нет активных запросов!")
            return
            
        request_id, office_chat_id, request_message_id = request
        
        
        msg = await bot.send_message(
            chat_id=office_chat_id,
            text=f"📱 Новый номер: <code>{phone}</code>\n<i>Отправьте фото с кодом в ответ</i>",
            parse_mode="HTML",
            reply_to_message_id=request_message_id  
        )
        
        
        cursor.execute('''UPDATE num_requests 
                        SET status = 'fulfilled' 
                        WHERE request_id = ?''', (request_id,))
        conn.commit()  
        
        
        cursor.execute('''INSERT INTO phone_messages VALUES 
                       (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (phone, original_message.message_id, original_message.chat.id,
                        original_message.from_user.id, original_message.from_user.username,
                        original_message.from_user.first_name, original_message.from_user.last_name,
                        datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S'),
                        None, None, None, None, None))
        conn.commit()
        
    except Exception as e:
        print(f"Error forwarding number: {e}")

async def safe_handle_error(error: Exception, context: dict = None):
    """Безопасная обработка ошибок с очисткой проблемных данных"""
    try:
        print(f"Ошибка: {str(error)}")
        if context:
            print(f"Контекст: {context}")
            
        
        if isinstance(error, TelegramBadRequest):
            error_text = str(error).lower()
            
            
            if "message to reply not found" in error_text or "message can't be deleted" in error_text:
                if context and 'message_id' in context:
                    
                    cursor.execute('''DELETE FROM num_requests 
                                   WHERE request_message_id = ?''', 
                                   (context['message_id'],))
                    conn.commit()
                    print(f"Удален проблемный запрос с message_id: {context['message_id']}")
                    
                    
                    if 'drops_chat_id' in context:
                        cursor.execute('''SELECT COUNT(*) FROM num_requests 
                                       WHERE status = 'pending' 
                                       AND drops_chat_id = ?''', 
                                       (context['drops_chat_id'],))
                        new_count = cursor.fetchone()[0]
                        
                        
                        cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ?', 
                                     (context['drops_chat_id'],))
                        last_msg = cursor.fetchone()
                        
                        if last_msg:
                            try:
                                await bot.edit_message_text(
                                    chat_id=context['drops_chat_id'],
                                    message_id=last_msg[0],
                                    text=f"📱 Требуется номеров: {new_count}\n\n⚠️ Требуются номера!",
                                    parse_mode="HTML"
                                )
                            except Exception as e:
                                print(f"Ошибка обновления счетчика: {e}")
                                
            
            elif "message is not modified" in error_text:
                print("Сообщение не требует изменений")
                return True
                
        return False
    except Exception as e:
        print(f"Ошибка в обработчике ошибок: {e}")
        return False

@router.message(Command("n"))
async def handle_numbers_request(message: Message):
    try:
        
        if not is_office_chat(message.chat.id):
            await message.reply("❌ Запрашивать номера можно только из разрешённых офисных чатов! Обратитесь к администратору для добавления этого чата.")
            return
        
        
        drops_chat = get_drops_chat_for_office(message.chat.id)
        
        if not drops_chat:
            await message.reply("❌ Чат дропов не настроен для этого офиса!")
            return

        
        cursor.execute('''SELECT tol.topic_id, nt.topic_type, nt.required_numbers, nt.custom_name 
                         FROM topic_office_links tol 
                         JOIN number_topics nt ON tol.topic_id = nt.topic_id 
                         WHERE tol.office_chat_id = ? AND nt.chat_id = ? AND nt.is_active = 1''',
                       (message.chat.id, drops_chat))
        topics = cursor.fetchall()
        
        if not topics:
            await message.reply("⚠️ Нет активных тем для приемки! Используйте /settings в чате дропов")
            return
            
        
        for topic_id, topic_type, required_numbers, custom_name in topics:
            try:
                
                cursor.execute('''INSERT INTO num_requests 
                                (office_chat_id, drops_chat_id, request_message_id, status)
                                VALUES (?, ?, ?, 'pending')''',
                              (message.chat.id, drops_chat, message.message_id))
                conn.commit()

                
                cursor.execute('''UPDATE number_topics 
                                 SET required_numbers = required_numbers + 1 
                                 WHERE topic_id = ? AND chat_id = ?''', 
                                 (topic_id, drops_chat))
                conn.commit()

                
                cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ? AND topic_id = ?', 
                             (drops_chat, topic_id))
                last_message = cursor.fetchone()
                
                if last_message:
                    try:
                        await bot.delete_message(drops_chat, last_message[0])
                    except Exception as e:
                        if not await safe_handle_error(e, {'message_id': last_message[0], 'drops_chat_id': drops_chat}):
                            print(f"Ошибка удаления сообщения: {e}")

                
                cursor.execute('SELECT required_numbers FROM number_topics WHERE topic_id = ? AND chat_id = ?', 
                             (topic_id, drops_chat))
                updated_required = cursor.fetchone()[0] or 0

                
                new_message = await bot.send_message(
                    chat_id=drops_chat,
                    text=f"📱 Требуется номеров: {updated_required}\n\n⚠️ Требуются номера!",
                    parse_mode="HTML",
                    message_thread_id=topic_id
                )
                
                
                cursor.execute(
                    'INSERT OR REPLACE INTO last_messages (chat_id, message_id, topic_id) VALUES (?, ?, ?)',
                    (drops_chat, new_message.message_id, topic_id)
                )
                conn.commit()

            except Exception as e:
                print(f"Ошибка при обработке темы {topic_id}: {e}")
                continue
        
        await message.reply("✅ Запросы на номера отправлены в группу приемки")
                
    except Exception as e:
        print(f"Critical error in handle_numbers_request: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        await message.reply("❌ Произошла критическая ошибка при обработке запроса")

@router.message(Command("allow"), F.chat.type == "private")
async def cmd_allow(message: Message, state: FSMContext):
    """Добавляет пользователя в список разрешенных."""
    print(f"Received /allow from user {message.from_user.id} in chat {message.chat.id} (type: {message.chat.type})")
    
    
    if not await is_allowed_user(message.from_user.id):
        print(f"Command /allow rejected: user {message.from_user.id} is not allowed")
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    
    current_state = await state.get_state()
    if current_state is not None:
        print(f"Clearing FSM state {current_state} to process /allow")
        await state.clear()

    try:
        
        args = message.text.strip().split()
        if len(args) != 2:
            print(f"Command /allow rejected: invalid format, received '{message.text}'")
            await message.answer("❌ Формат команды: /allow [user_id]")
            return
        user_id = int(args[1])
        cursor.execute('INSERT OR IGNORE INTO allowed_users (user_id) VALUES (?)', (user_id,))
        conn.commit()
        print(f"User {user_id} successfully added to allowed_users")
        await message.answer(f"✅ Пользователь с ID {user_id} добавлен в список разрешенных.")
    except ValueError:
        print(f"Command /allow rejected: user_id is not a number, received '{args[1]}'")
        await message.answer("❌ ID пользователя должен быть числом.")
    except Exception as e:
        print(f"Error in /allow: {e}")
        await message.answer("❌ Ошибка при добавлении пользователя.")

@router.message(Command("disallow"), F.chat.type == "private")
async def cmd_disallow(message: Message, state: FSMContext):
    """Удаляет пользователя из списка разрешенных."""
    print(f"Received /disallow from user {message.from_user.id} in chat {message.chat.id} (type: {message.chat.type})")
    
    
    if not await is_allowed_user(message.from_user.id):
        print(f"Command /disallow rejected: user {message.from_user.id} is not allowed")
        await message.answer("❌ У вас нет прав для выполнения этой команды.")
        return

    
    current_state = await state.get_state()
    if current_state is not None:
        print(f"Clearing FSM state {current_state} to process /disallow")
        await state.clear()

    try:
        
        args = message.text.strip().split()
        if len(args) != 2:
            print(f"Command /disallow rejected: invalid format, received '{message.text}'")
            await message.answer("❌ Формат команды: /disallow [user_id]")
            return
        user_id = int(args[1])
        cursor.execute('DELETE FROM allowed_users WHERE user_id = ?', (user_id,))
        if cursor.rowcount == 0:
            print(f"User {user_id} not found in allowed_users")
            await message.answer(f"❌ Пользователь с ID {user_id} не найден в списке разрешенных.")
        else:
            conn.commit()
            print(f"User {user_id} successfully removed from allowed_users")
            await message.answer(f"✅ Пользователь с ID {user_id} удален из списка разрешенных.")
    except ValueError:
        print(f"Command /disallow rejected: user_id is not a number, received '{args[1]}'")
        await message.answer("❌ ID пользователя должен быть числом.")
    except Exception as e:
        print(f"Error in /disallow: {e}")
        await message.answer("❌ Ошибка при удалении пользователя.")

@router.message(Form.wait_for_chat_ids)
async def process_chat_ids(message: Message, state: FSMContext):
    try:
        
        cleaned_text = message.text.strip().replace(' ', '')
        
        ids = [x.strip() for x in cleaned_text.split(',')]
        
        if len(ids) < 2:
            await message.answer(
                "❌ Неверный формат!\n\n"
                "Нужно ввести минимум 2 ID через запятую (хотя бы один офисный чат и дроп-чат):\n"
                "<code>ID_офис1,ID_офис2,...,ID_дропы</code>\n\n"
                "Пример:\n<code>-100111,-100222,-100444</code>",
                parse_mode="HTML"
            )
            await state.clear()  
            return
            
        try:
            
            ids = [int(x) for x in ids]
        except ValueError:
            await message.answer(
                "❌ Ошибка в формате ID!\n\n"
                "Каждый ID должен быть числом.\n"
                "Пример:\n<code>-100111,-100222,-100444</code>",
                parse_mode="HTML"
            )
            await state.clear()  
            return
            
        try:
            
            save_user_data(message.from_user.id, ids)
            
            
            office_chats = ids[:-1]
            drops_chat = ids[-1]
            
            confirmation_text = "✅ Настройки чатов успешно сохранены!\n\n"
            for i, chat_id in enumerate(office_chats, 1):
                confirmation_text += f"Офис {i}: <code>{chat_id}</code>\n"
            confirmation_text += f"Дропы: <code>{drops_chat}</code>"
            
            await message.answer(
                confirmation_text,
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
            await state.clear()
        except sqlite3.Error as sql_error:
            print(f"SQL Error: {sql_error}")
            await message.answer("❌ Ошибка при сохранении в БД.")
            await state.clear()

    except Exception as e:
        print(f"Error: {traceback.format_exc()}")
        await message.answer("❌ Критическая ошибка.")
        await state.clear()

@router.message(F.text)
async def handle_phone_number(message: Message):
    try:
        
        if not is_drops_chat(message.chat.id):
            return

        
        if message.text.startswith('/'):
            print(f"Ignoring command in drops chat: {message.text}")
            return

        
        cursor.execute('''SELECT topic_id, custom_name, topic_type, required_numbers 
                         FROM number_topics 
                         WHERE chat_id = ? AND topic_id = ? AND topic_type LIKE 'drops_%' AND is_active = 1''', 
                         (message.chat.id, message.message_thread_id))
        current_topic = cursor.fetchone()
                
        if not current_topic:
            return

        topic_id, custom_name, topic_type, required_numbers = current_topic
        
        
        topic_ratio = topic_type.split('_')[2]  
        actual_topic_name = custom_name if custom_name else f"1/{topic_ratio}"
        print(f"Обработка номера для темы: {actual_topic_name} (тип: {topic_type})")

        
        phone = extract_phone(message.text)
        if not phone:
            return

        try:
            
            cursor.execute('''SELECT office_chat_id FROM topic_office_links WHERE topic_id = ?''', (topic_id,))
            linked_offices = [row[0] for row in cursor.fetchall()]
            
            if not linked_offices:
                await message.reply("❌ К этой теме не привязан ни один офис")
                return

            
            cursor.execute('''SELECT r.request_id, r.office_chat_id, r.request_message_id 
                            FROM num_requests r
                            WHERE r.status = 'pending' 
                            AND r.drops_chat_id = ?
                            AND r.office_chat_id IN ({})
                            ORDER BY r.request_id ASC
                            LIMIT 1'''.format(','.join('?' * len(linked_offices))), 
                            [message.chat.id] + linked_offices)
            request = cursor.fetchone()
            
            if not request:
                
                cursor.execute('''SELECT COUNT(*) FROM num_requests 
                                WHERE status = 'pending' 
                                AND drops_chat_id = ?
                                AND office_chat_id IN ({})'''.format(','.join('?' * len(linked_offices))),
                                [message.chat.id] + linked_offices)
                if cursor.fetchone()[0] == 0:
                    await message.reply("❌ Нет активных запросов для этой темы")
                    return
                else:
                    await message.reply("❌ Этот офис не привязан к данной теме")
                    return
                
            request_id, office_chat_id, request_message_id = request
            
            
            try:
                msg = await bot.send_message(
                    chat_id=office_chat_id,
                    text=f"📱 Новый номер: <code>{phone}</code>\n<i>Отправьте фото с кодом в ответ</i>",
                    parse_mode="HTML",
                    reply_to_message_id=request_message_id
                )
            except TelegramBadRequest as e:
                if not await safe_handle_error(e, {'message_id': request_message_id, 'drops_chat_id': message.chat.id}):
                    
                    msg = await bot.send_message(
                        chat_id=office_chat_id,
                        text=f"📱 Новый номер: <code>{phone}</code>\n<i>Отправьте фото с кодом в ответ</i>",
                        parse_mode="HTML"
                    )
            
            
            confirmation = await message.reply(
                f"✅ Номер <code>{phone}</code> принят!\n\n"
                "⚠️ Оставайтесь в сети до завершения регистрации.\n",
                parse_mode="HTML"
            )
            
            
            cursor.execute('''INSERT OR REPLACE INTO phone_messages 
                           (phone, user_message_id, confirmation_message_id, chat_id, 
                            user_id, username, first_name, last_name, registration_time, 
                            report_message_id, topic_name, topic_id, topic_type)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                           (phone, message.message_id, confirmation.message_id, message.chat.id,
                            message.from_user.id, message.from_user.username,
                            message.from_user.first_name, message.from_user.last_name,
                            datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S'),
                            None,
                            actual_topic_name,
                            topic_id,
                            topic_type))  
            
            
            cursor.execute('''UPDATE num_requests 
                            SET status = 'fulfilled' 
                            WHERE request_id = ?''', (request_id,))
            
            
            cursor.execute('''SELECT required_numbers FROM number_topics 
                            WHERE topic_id = ? AND chat_id = ?''', 
                            (topic_id, message.chat.id))
            current_required = cursor.fetchone()[0] or 0
            
            
            new_required = max(0, current_required - 1)
            cursor.execute('''UPDATE number_topics 
                            SET required_numbers = ? 
                            WHERE topic_id = ? AND chat_id = ?''', 
                            (new_required, topic_id, message.chat.id))
            conn.commit()

            
            cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ? AND topic_id = ?', 
                         (message.chat.id, topic_id))
            last_msg = cursor.fetchone()
            
            if last_msg:
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=last_msg[0],
                        text=f"📱 Требуется номеров: {new_required}\n\n⚠️ Требуются номера!",
                        parse_mode="HTML"
                    )
                except Exception as e:
                    if not await safe_handle_error(e, {'message_id': last_msg[0], 'drops_chat_id': message.chat.id}):
                        print(f"Ошибка обновления счетчика: {e}")

        except Exception as e:
            if not await safe_handle_error(e, {'message_id': message.message_id, 'drops_chat_id': message.chat.id}):
                await message.reply(
                    f"❌ Ошибка обработки: {str(e)}",
                    reply_to_message_id=message.message_id
                )
                print(f"Critical error: {traceback.format_exc()}")
                
    except Exception as e:
        print(f"Critical error in handle_phone_number: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        await message.reply("❌ Произошла критическая ошибка при обработке номера")

@router.message(F.photo)
async def handle_photo_reply(message: Message):
    if not message.reply_to_message:
        return
        
    original_msg = message.reply_to_message
    
    if original_msg.from_user.id != bot.id:
        return
        
    if not original_msg.text or not original_msg.text.startswith("📱 Новый номер:"):
        return
        
    phone_match = re.search(r'\+7\d{10}', original_msg.text)
    if not phone_match:
        return
        
    phone = phone_match.group(0)
    
    
    drops_chat = get_drops_chat_for_office(message.chat.id)
    
    if not drops_chat:
        await message.reply("❌ Ошибка: не найден связанный чат дропов")
        return
        
    try:
        
        cursor.execute('''SELECT topic_id, topic_name FROM phone_messages 
                         WHERE phone = ? AND chat_id = ?''', 
                         (phone, drops_chat))
        phone_info = cursor.fetchone()
        
        if not phone_info:
            print(f"Не найдена информация о номере {phone} в базе данных")
            await message.reply("❌ Ошибка: не найдена информация о номере в базе данных")
            return
            
        topic_id, topic_name = phone_info
        print(f"Отправка фото для номера {phone} в тему {topic_name} (ID: {topic_id})")

        
        cursor.execute('''SELECT topic_id FROM number_topics 
                         WHERE chat_id = ? AND topic_id = ? AND topic_type LIKE 'drops_%' AND is_active = 1''',
                        (drops_chat, topic_id))
        active_topic = cursor.fetchone()
        
        if not active_topic:
            print(f"Тема {topic_id} не активна или не найдена")
            await message.reply("❌ Ошибка: тема не активна или не найдена")
            return

        
        cursor.execute('SELECT user_message_id FROM phone_messages WHERE phone = ? AND chat_id = ?', 
                      (phone, drops_chat))
        user_message = cursor.fetchone()

        sent_msg = await bot.send_photo(
            chat_id=drops_chat,
            photo=message.photo[-1].file_id,
            caption=f"📱 {phone}",
            parse_mode="HTML",
            message_thread_id=topic_id,  
            reply_to_message_id=user_message[0] if user_message else None
        )
        
        await original_msg.edit_text(
            f"📲 Номер: <code>{phone}</code>\n✅ Код отправлен",
            parse_mode="HTML",
            reply_markup=InlineKeyboardBuilder()
                .row(
                    InlineKeyboardButton(text="✅ Встал", callback_data=f"status_ok_{original_msg.message_id}"),
                    InlineKeyboardButton(text="❌ Не встал", callback_data=f"status_fail_{original_msg.message_id}"),
                )
                .row(
                    InlineKeyboardButton(text="🔁 Повтор", callback_data=f"status_repeat_{original_msg.message_id}"),
                )
                .as_markup()
        )
        
    except Exception as e:
        print(f"Ошибка при отправке фото: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        await message.reply(f"❌ Ошибка при отправке фото: {str(e)}")

async def safe_delete_message(chat_id, message_id):
    try:
        await bot.delete_message(chat_id, message_id)
        return True
    except TelegramBadRequest as e:
        if "message to delete not found" in str(e).lower():
            print(f"Сообщение {message_id} уже удалено или недоступно")
        elif "message can't be deleted" in str(e).lower():
            print(f"Невозможно удалить сообщение {message_id}")
        else:
            print(f"Ошибка при удалении сообщения: {e}")
        return False
    except TelegramForbiddenError:
        print(f"Недостаточно прав для удаления сообщения {message_id}")
        return False
    except Exception as e:
        print(f"Неожиданная ошибка при удалении сообщения: {e}")
        return False

async def safe_edit_message(chat_id, message_id, new_text, **kwargs):
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=new_text,
            **kwargs
        )
        return True, None
    except TelegramBadRequest as e:
        if "message to edit not found" in str(e).lower():
            return False, "not_found"
        elif "message is not modified" in str(e).lower():
            return True, None
        else:
            print(f"Ошибка при редактировании сообщения: {e}")
            return False, "other"
    except Exception as e:
        print(f"Неожиданная ошибка при редактировании сообщения: {e}")
        return False, "other"

@router.callback_query(F.data.startswith("status_"))
async def handle_registration_status(callback: types.CallbackQuery):
    try:
        
        if not await is_admin(callback.message.chat.id, callback.from_user.id):
            await callback.answer("❌ Только администраторы могут использовать эти кнопки!", show_alert=True)
            return

        print(f"Processing callback data: {callback.data}")
        _, status, msg_id = callback.data.split("_")
        msg_id = int(msg_id)
        
        current_text = callback.message.text
        print(f"Current message text: {current_text}")
        
        phone_match = re.search(r'\+7\d{10}', current_text)
        if not phone_match:
            print("No phone number found in message text")
            await callback.answer("❌ Номер не найден")
            return
            
        phone = phone_match.group(0)
        print(f"Found phone number: {phone}")
        
        cursor.execute('''SELECT user_id, username, first_name, last_name 
                        FROM phone_messages WHERE phone = ?''', (phone,))
        user_info = cursor.fetchone()
        print(f"User info from database: {user_info}")
        
        if user_info:
            user_id, username, first_name, last_name = user_info
            
            if username:
                user_mention = f"@{username}"
            elif first_name:
                
                safe_name = first_name.encode('ascii', 'ignore').decode()
                if not safe_name:
                    user_mention = f"ID: {user_id}"
                else:
                    user_mention = f"{safe_name}"
            else:
                user_mention = f"ID: {user_id}"
        else:
            user_mention = "Неизвестный пользователь"
            print(f"No user info found for phone {phone}")
        
        if status == "ok":
            print("Processing status_ok")
            moscow_time = datetime.now(pytz.timezone('Europe/Moscow')).strftime('%H:%M')
            
            
            try:
                cursor.execute('''UPDATE phone_messages 
                                SET registration_time = ? 
                                WHERE phone = ?''', 
                                (datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S'), phone))
                conn.commit()
                print("Updated registration time")
            except Exception as e:
                print(f"Error updating registration time: {e}")
                await callback.answer("❌ Ошибка при обновлении времени регистрации")
                return
            
            report_topic = get_settings(callback.message.chat.id)
            print(f"Report topic: {report_topic}")
            
            if report_topic:
                try:
                    await bot.send_message(
                        callback.message.chat.id,
                        f"{phone} {moscow_time}",
                        message_thread_id=report_topic
                    )
                    print("Sent message to report topic")
                except Exception as e:
                    print(f"Error sending message to report topic: {e}")
            
            try:
                
                drops_chat = get_drops_chat_for_office(callback.message.chat.id)
                print(f"Drops chat: {drops_chat}")
                
                if drops_chat:
                    cursor.execute('''SELECT topic_id FROM number_topics 
                                    WHERE chat_id = ? AND topic_type = "reports" AND is_active = 1''', 
                                    (drops_chat,))
                    drops_reports_topic = cursor.fetchone()
                    print(f"Drops reports topic: {drops_reports_topic}")
                    
                    if drops_reports_topic:
                        
                        try:
                            
                            cursor.execute('SELECT topic_name FROM phone_messages WHERE phone = ?', (phone,))
                            topic_name = cursor.fetchone()[0] or "Неизвестный топик"
                            
                            
                            message_text = f"{phone} {moscow_time} {user_mention} | {topic_name}"
                            report_msg = await bot.send_message(
                                drops_chat,
                                message_text,
                                message_thread_id=drops_reports_topic[0]
                            )
                            print("Sent report message")
                            
                            
                            cursor.execute('''UPDATE phone_messages 
                                            SET report_message_id = ? 
                                            WHERE phone = ?''', 
                                            (report_msg.message_id, phone))
                            conn.commit()
                            print("Saved report message ID")
                        except Exception as e:
                            print(f"Error sending/saving report message: {e}")
                            await callback.answer("❌ Ошибка при отправке отчета")
                            return
            except Exception as e:
                print(f"Error processing drops chat: {e}")
                await callback.answer("❌ Ошибка при обработке чата дропов")
                return

            try:
                message_text = f"📲 Номер: {phone}\n✅ Зарегистрирован"
                reply_markup = InlineKeyboardBuilder()
                reply_markup.row(
                    InlineKeyboardButton(text="📱 Запросить номер", callback_data="request_number"),
                    InlineKeyboardButton(text="🔴 Слёт", callback_data=f"slet_{phone}")
                )
                
                success = await safe_edit_message(
                    chat_id=callback.message.chat.id,
                    message_id=callback.message.message_id,
                    new_text=message_text,
                    parse_mode="HTML",
                    reply_markup=reply_markup.as_markup()
                )
                
                if success:
                    print("Updated message with registration status")
                    await callback.answer("✅ Статус обновлен: Зарегистрирован")
                else:
                    await callback.answer("⚠️ Не удалось обновить сообщение")
            except Exception as e:
                print(f"Error updating message with status: {e}")
                await callback.answer("❌ Ошибка при обновлении статуса")
                return
            
        elif status == "fail":
            try:
                message_text = f"📲 Номер: {phone}\n❌ Не зарегистрирован"
                success = await safe_edit_message(
                    chat_id=callback.message.chat.id,
                    message_id=callback.message.message_id,
                    new_text=message_text,
                    parse_mode="HTML"
                )
                if success:
                    await callback.answer("Статус обновлен: Не зарегистрирован")
                else:
                    await callback.answer("⚠️ Не удалось обновить сообщение")
            except Exception as e:
                print(f"Error updating fail status: {e}")
                await callback.answer("❌ Ошибка при обновлении статуса")
                return
            
            
            drops_chat = get_drops_chat_for_office(callback.message.chat.id)
            
            if drops_chat:
                cursor.execute('''SELECT topic_id FROM number_topics 
                                WHERE chat_id = ? AND topic_type LIKE 'drops_%' AND is_active = 1''',
                                (drops_chat,))
                drops_topic = cursor.fetchone()
                
                if drops_topic:
                    cursor.execute('''SELECT COUNT(*) FROM num_requests 
                                    WHERE status = 'pending' 
                                    AND drops_chat_id = ?''', (drops_chat,))
                    required_count = cursor.fetchone()[0] or 0
                    
                    cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ?', (drops_chat,))
                    last_message = cursor.fetchone()
                    
                    if last_message:
                        await safe_delete_message(drops_chat, last_message[0])
                    
                    new_message = await bot.send_message(
                        drops_chat,
                        f"📱 Требуется номеров: {required_count}\n\n⚠️ Требуются номера!",
                        parse_mode="HTML",
                        message_thread_id=drops_topic[0]
                    )
                    
                    cursor.execute('INSERT OR REPLACE INTO last_messages (chat_id, message_id) VALUES (?, ?)',
                                 (drops_chat, new_message.message_id))
                    conn.commit()
            
        elif status == "repeat":
            
            drops_chat = get_drops_chat_for_office(callback.message.chat.id)
            
            if drops_chat:
                cursor.execute('''SELECT topic_id FROM number_topics 
                                WHERE chat_id = ? AND topic_type LIKE 'drops_%' AND is_active = 1''',
                                (drops_chat,))
                drops_topic = cursor.fetchone()
                
                if drops_topic:
                    cursor.execute('SELECT user_message_id FROM phone_messages WHERE phone = ? AND chat_id = ?', 
                                 (phone, drops_chat))
                    user_message = cursor.fetchone()
                    
                    await bot.send_message(
                        drops_chat,
                        f"📱 {phone}\n🔄 Ожидайте повторной отправки кода. Пожалуйста, оставайтесь в сети",
                        parse_mode="HTML",
                        message_thread_id=drops_topic[0],
                        reply_to_message_id=user_message[0] if user_message else None
                    )
                    
                    try:
                        message_text = f"📱 Новый номер: <code>{phone}</code>\n<i>Отправьте фото с кодом в ответ на это сообщение</i>"
                        reply_markup = InlineKeyboardBuilder()
                        reply_markup.row(
                            InlineKeyboardButton(text="✅ Встал", callback_data=f"status_ok_{msg_id}"),
                            InlineKeyboardButton(text="❌ Не встал", callback_data=f"status_fail_{msg_id}")
                        )
                        reply_markup.row(
                            InlineKeyboardButton(text="🔁 Повтор", callback_data=f"status_repeat_{msg_id}")
                        )
                        
                        success = await safe_edit_message(
                            chat_id=callback.message.chat.id,
                            message_id=callback.message.message_id,
                            new_text=message_text,
                            parse_mode="HTML",
                            reply_markup=reply_markup.as_markup()
                        )
                        if success:
                            await callback.answer("Отправлен повторный запрос")
                        else:
                            await callback.answer("⚠️ Не удалось обновить сообщение")
                    except Exception as e:
                        print(f"Error updating repeat status: {e}")
                        await callback.answer("❌ Ошибка при обновлении статуса")
                        return
            
    except Exception as e:
        print(f"Critical error in handle_registration_status: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        await callback.answer(f"❌ Критическая ошибка: {str(e)}")

@router.callback_query(F.data == "request_number")
async def handle_request_number(callback: types.CallbackQuery):
    try:
        
        if not await is_admin(callback.message.chat.id, callback.from_user.id):
            await callback.answer("❌ Только администраторы могут использовать эту кнопку!", show_alert=True)
            return

        
        if not is_office_chat(callback.message.chat.id):
            await callback.answer("❌ Эта команда доступна только в офисном чате!")
            return
        
        
        drops_chat = get_drops_chat_for_office(callback.message.chat.id)
        
        if not drops_chat:
            await callback.answer("❌ Чат дропов не настроен для этого офиса!")
            return

        
        cursor.execute('''SELECT tol.topic_id, nt.topic_type 
                         FROM topic_office_links tol 
                         JOIN number_topics nt ON tol.topic_id = nt.topic_id 
                         WHERE tol.office_chat_id = ? AND nt.chat_id = ? AND nt.is_active = 1''',
                       (callback.message.chat.id, drops_chat))
        topic_info = cursor.fetchone()
        
        if not topic_info:
            await callback.answer("⚠️ Тема для приемки не настроена для этого офиса! Используйте /settings в чате дропов")
            return
            
        topic_id, topic_type = topic_info

        
        cursor.execute('''INSERT INTO num_requests 
                         (office_chat_id, drops_chat_id, request_message_id, status)
                         VALUES (?, ?, ?, 'pending')''',
                       (callback.message.chat.id, drops_chat, callback.message.message_id))
        conn.commit()

        
        cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ?', (drops_chat,))
        last_message = cursor.fetchone()
        
        if last_message:
            await safe_delete_message(drops_chat, last_message[0])

        
        cursor.execute('''SELECT COUNT(*) FROM num_requests 
                        WHERE status = 'pending' AND drops_chat_id = ?''',
                      (drops_chat,))
        pending_count = cursor.fetchone()[0]

        
        new_message = await bot.send_message(
            chat_id=drops_chat,
            text=f"📱 Требуется номеров: {pending_count}\n\n⚠️ Требуются номера!",
            parse_mode="HTML",
            message_thread_id=topic_id
        )
        
        
        cursor.execute(
            'INSERT OR REPLACE INTO last_messages (chat_id, message_id) VALUES (?, ?)',
            (drops_chat, new_message.message_id)
        )
        conn.commit()
        
        await callback.answer("✅ Запрос на номер отправлен в группу приемки")
            
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")
        print(f"Error in handle_request_number: {traceback.format_exc()}")

@router.callback_query(F.data.startswith("slet_"))
async def handle_slet(callback: types.CallbackQuery):
    try:
        
        if not await is_admin(callback.message.chat.id, callback.from_user.id):
            await callback.answer("❌ Только администраторы могут использовать эту кнопку!", show_alert=True)
            return

        phone = callback.data.split("_")[1]
        
        
        cursor.execute('''SELECT registration_time, user_id, username, first_name, last_name, 
                         chat_id, report_message_id, topic_name
                         FROM phone_messages WHERE phone = ?''', (phone,))
        reg_info = cursor.fetchone()
        
        if not reg_info:
            await callback.answer("❌ Информация о регистрации не найдена")
            return
            
        reg_time, user_id, username, first_name, last_name, drops_chat, report_message_id, topic_name = reg_info
        topic_name = topic_name or "Неизвестный топик"
        
        user_mention = f"@{username}" if username else f"[{first_name} {last_name}](tg://user?id={user_id})"
        
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time = datetime.now(moscow_tz)
        slet_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
        reg_datetime = datetime.strptime(reg_time, '%Y-%m-%d %H:%M:%S')
        reg_datetime = moscow_tz.localize(reg_datetime)
        
        
        time_diff = current_time - reg_datetime
        minutes = int(time_diff.total_seconds() // 60)
        seconds = int(time_diff.total_seconds() % 60)
        
        
        reg_time_str = reg_datetime.strftime('%H:%M')
        current_time_str = current_time.strftime('%H:%M')
        
        
        cursor.execute('''UPDATE phone_messages 
                         SET slet_time = ? 
                         WHERE phone = ?''', 
                         (slet_time, phone))
        conn.commit()
        
        
        cursor.execute('''SELECT topic_id FROM number_topics 
                         WHERE chat_id = ? AND topic_type = "reports" AND is_active = 1''', 
                         (drops_chat,))
        drops_reports_topic = cursor.fetchone()
        
        if not drops_reports_topic:
            await callback.answer("❌ Тема для отчетов не настроена!")
            return
            
        new_text = f"{phone} {reg_time_str}-{current_time_str} ({minutes:02d}:{seconds:02d}) {user_mention} | {topic_name}"
        
        if report_message_id:
            success, error = await safe_edit_message(
                chat_id=drops_chat,
                message_id=report_message_id,
                new_text=new_text
            )
        else:
            success, error = False, "not_found"

        if not success and error == "not_found":
            try:
                
                report_msg = await bot.send_message(
                    drops_chat,
                    new_text,
                    message_thread_id=drops_reports_topic[0]
                )
                
                cursor.execute('''UPDATE phone_messages 
                                SET report_message_id = ? 
                                WHERE phone = ?''', 
                                (report_msg.message_id, phone))
                conn.commit()
                success = True
            except Exception as e:
                print(f"Ошибка при создании нового сообщения отчета: {e}")
                success = False

        if success:
            
            button_message_success, _ = await safe_edit_message(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                new_text=f"{callback.message.text}\n🔴 Слетел через {minutes:02d}:{seconds:02d}",
                parse_mode="HTML",
                reply_markup=InlineKeyboardBuilder()
                    .row(InlineKeyboardButton(text="📱 Запросить номер", callback_data="request_number"))
                    .as_markup()
            )
            
            if button_message_success:
                await callback.answer("✅ Отчет о слёте обновлен")
            else:
                await callback.answer("⚠️ Частично обновлено (ошибка с кнопками)")
        else:
            await callback.answer("❌ Не удалось обновить отчет")
            
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")
        print(f"Error in handle_slet: {traceback.format_exc()}")

@router.callback_query(lambda c: c.data == "resetdb_confirm")
async def resetdb_confirm(callback_query: types.CallbackQuery):
    if not await is_allowed_user(callback_query.from_user.id):
        await callback_query.answer("❌ Команда доступна только разрешённым пользователям.")
        return
    try:
        await callback_query.message.edit_text("⏳ Очистка базы данных... Бот будет перезапущен.")
        
        
        conn.close()
        
        
        if os.path.exists(config['DB_NAME']):
            try:
                os.remove(config['DB_NAME'])
            except PermissionError:
                await callback_query.message.edit_text("❌ Ошибка: недостаточно прав для удаления базы данных")
                return
            except OSError as e:
                await callback_query.message.edit_text(f"❌ Ошибка при удалении файла базы данных: {e}")
                return
        
        
        script_path = os.path.abspath(sys.argv[0])
        
        try:
            if sys.platform == 'win32':
                os.execv(sys.executable, [sys.executable] + [script_path] + sys.argv[1:])
            else:
                
                os.execv(sys.executable, [sys.executable] + [script_path] + sys.argv[1:])
        except Exception as e:
            await callback_query.message.edit_text(f"❌ Ошибка при перезапуске бота: {e}")
            return
            
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Ошибка при очистке базы данных: {e}")

@router.callback_query(lambda c: c.data == "resetdb_cancel")
async def resetdb_cancel(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text("❌ Очистка базы данных отменена.")

async def get_topic_name(chat_id: int, topic_id: int) -> str:
    try:
        cursor.execute('SELECT custom_name FROM number_topics WHERE chat_id = ? AND topic_id = ?', (chat_id, topic_id))
        result = cursor.fetchone()
        return result[0] if result and result[0] else f"Топик {topic_id}"
    except Exception as e:
        print(f"Ошибка при получении названия топика: {e}, chat_id={chat_id}, topic_id={topic_id}")
        return f"Топик {topic_id}"

@router.callback_query(F.data.startswith("configure_"))
async def configure_offices(callback: types.CallbackQuery):
    try:
        
        parts = callback.data.split("_")
        
        
        if parts[1] == "20":  
            topic_type = f"{parts[1]}-{parts[2]}"  
            topic_id = int(parts[3])  
            back_callback = f"topic_menu_20_25_{topic_id}"  
        else:  
            topic_type = f"{parts[1]}_{parts[2]}"  
            topic_id = int(parts[3])  
            back_callback = f"topic_menu_{parts[1]}_{parts[2]}_{topic_id}"  
        
        
        cursor.execute('SELECT office_chat_id FROM topic_office_links WHERE topic_id = ?', (topic_id,))
        linked_offices = {row[0] for row in cursor.fetchall()}
        
        
        cursor.execute('SELECT chat_id FROM office_chats')
        office_chats = [row[0] for row in cursor.fetchall()]
        
        if not office_chats:
            await callback.answer("⚠️ Нет доступных офисных чатов!")
            return
            
        builder = InlineKeyboardBuilder()
        for office_id in office_chats:
            text = f"{'✅ ' if office_id in linked_offices else '⬜ '}Офис {office_id}"
            
            if parts[1] == "20":
                toggle_callback = f"toggle_office_20_25_{topic_id}_{office_id}"
            else:
                toggle_callback = f"toggle_office_{parts[1]}_{parts[2]}_{topic_id}_{office_id}"
            builder.row(InlineKeyboardButton(text=text, callback_data=toggle_callback))
        
        
        builder.row(InlineKeyboardButton(text="Назад", callback_data=back_callback))
        
        await callback.message.edit_text(
            "Выберите офисы для этой темы:",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
    except Exception as e:
        print(f"Ошибка в configure_offices: {e}")
        print(f"Callback data: {callback.data}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

@router.callback_query(F.data.startswith("toggle_office_"))
async def toggle_office(callback: types.CallbackQuery):
    try:
        print(f"Callback data в toggle_office: {callback.data}")
        
        parts = callback.data.split("_")
        
        if len(parts) != 6:
            raise ValueError(f"Неверный формат callback_data: {callback.data}")
        
        
        if parts[2] == "20":  
            topic_type = f"{parts[2]}-{parts[3]}"  
            back_callback = f"topic_menu_20_25_{parts[4]}"
        else:  
            topic_type = f"{parts[2]}_{parts[3]}"  
            back_callback = f"topic_menu_{parts[2]}_{parts[3]}_{parts[4]}"
        
        topic_id = int(parts[4])
        office_id = int(parts[5])
        
        print(f"Обработка toggle_office: тип темы = {topic_type}, topic_id = {topic_id}, office_id = {office_id}")
        
        
        cursor.execute('SELECT COUNT(*) FROM topic_office_links WHERE topic_id = ? AND office_chat_id = ?', 
                       (topic_id, office_id))
        if cursor.fetchone()[0] > 0:
            cursor.execute('DELETE FROM topic_office_links WHERE topic_id = ? AND office_chat_id = ?', 
                           (topic_id, office_id))
        else:
            cursor.execute('INSERT INTO topic_office_links (topic_id, office_chat_id) VALUES (?, ?)', 
                           (topic_id, office_id))
        conn.commit()
        
        
        cursor.execute('SELECT office_chat_id FROM topic_office_links WHERE topic_id = ?', (topic_id,))
        linked_offices = {row[0] for row in cursor.fetchall()}
        
        cursor.execute('SELECT chat_id FROM office_chats')
        office_chats = [row[0] for row in cursor.fetchall()]
        
        builder = InlineKeyboardBuilder()
        for office_id in office_chats:
            text = f"{'✅ ' if office_id in linked_offices else '⬜ '}Офис {office_id}"
            
            if "20-25" in topic_type:
                toggle_callback = f"toggle_office_20_25_{topic_id}_{office_id}"
            else:
                parts = topic_type.split("_")
                toggle_callback = f"toggle_office_{parts[0]}_{parts[1]}_{topic_id}_{office_id}"
            builder.row(InlineKeyboardButton(text=text, callback_data=toggle_callback))
        
        print(f"Формирование кнопки 'Назад' с callback_data: {back_callback}")
        builder.row(InlineKeyboardButton(text="Назад", callback_data=back_callback))
        
        await callback.message.edit_text(
            "Выберите офисы для этой темы:",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
    except Exception as e:
        print(f"Ошибка в toggle_office: {e}")
        print(f"Callback data: {callback.data}")
        print(f"Parts: {parts if 'parts' in locals() else 'не определено'}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

@router.callback_query(F.data.startswith("reset_"))
async def reset_topic(callback: types.CallbackQuery):
    try:
        _, topic_type, topic_id = callback.data.split("_")
        topic_id = int(topic_id)
        
        cursor.execute(
            'DELETE FROM number_topics WHERE chat_id = ? AND topic_id = ? AND topic_type = ?',
            (callback.message.chat.id, topic_id, f"drops_{topic_type}")
        )
        cursor.execute(
            'DELETE FROM topic_office_links WHERE topic_id = ?',
            (topic_id,)
        )
        conn.commit()
        
        await callback.answer("✅ Тема сброшена!")
        await callback.message.delete()
    except Exception as e:
        print(f"Ошибка в reset_topic: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

@router.callback_query(lambda c: c.data == "settings")
async def back_to_settings(callback: types.CallbackQuery):
    try:
        builder = InlineKeyboardBuilder()
        
        
        current_topic_id = callback.message.message_thread_id if callback.message.message_thread_id else None
        
        
        builder.row(
            InlineKeyboardButton(text="1/8", callback_data=f"topic_menu_1_8_{current_topic_id}"),
            InlineKeyboardButton(text="1/16", callback_data=f"topic_menu_1_16_{current_topic_id}")
        )
        builder.row(
            InlineKeyboardButton(text="7/1", callback_data=f"topic_menu_7_1_{current_topic_id}"),
            InlineKeyboardButton(text="20-25", callback_data=f"topic_menu_20_25_{current_topic_id}")
        )
        builder.row(
            InlineKeyboardButton(text="📊 Отчеты", callback_data=f"set_reports_{current_topic_id}")
        )
        
        await callback.message.edit_text(
            "⚙️ <b>Настройки тем</b>\n\n"
            "Выберите тип темы для текущего чата.",
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
    except Exception as e:
        print(f"Ошибка в back_to_settings: {e}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

async def send_daily_report():
    try:
        moscow_tz = pytz.timezone(config['TIMEZONE'])
        today = datetime.now(moscow_tz).date()
        report_date = today.strftime('%Y-%m-%d')
        print(f"Формирование отчета за {report_date}")
        all_reports = {'1/8': [], '1/16': [], '7/1': [], '20-25': []}
        cursor.execute('SELECT chat_id FROM drops_chats')
        drops_chats = [row[0] for row in cursor.fetchall()]
        if not drops_chats:
            print("Нет настроенных дроп-чатов")
            return
        print(f"Найдено дроп-чатов: {len(drops_chats)}")
        if any(reports for reports in all_reports.values()):
            total_report = f"📊 Общий отчет за {report_date}:\n\n"
            total_numbers = sum(len(numbers) for numbers in all_reports.values())
            total_report += f"📈 Всего номеров: {total_numbers}\n\n"
            for report_type in ['1/8', '1/16', '7/1', '20-25']:
                if all_reports[report_type]:
                    total_report += f"📱 {report_type} ({len(all_reports[report_type])} шт.):\n"
                    total_report += "\n".join(all_reports[report_type]) + "\n\n"
            print(f"Отправка общего отчета пользователю {config['REPORT_USER_ID']}")
            try:
                await bot.send_message(
                    chat_id=config['REPORT_USER_ID'],
                    text=total_report,
                    parse_mode="HTML"
                )
                print("Общий отчет успешно отправлен")
            except Exception as e:
                print(f"Ошибка при отправке общего отчета: {e}")
                print(f"Traceback: {traceback.format_exc()}")
    except Exception as e:
        print(f"Критическая ошибка в send_daily_report: {e}")
        print(f"Traceback: {traceback.format_exc()}")

async def schedule_daily_report():
    while True:
        try:
            now = datetime.now(pytz.timezone(config['TIMEZONE']))
            target_time = datetime.strptime(config['REPORT_TIME'], "%H:%M").time()
            target_datetime = now.replace(
                hour=target_time.hour,
                minute=target_time.minute,
                second=0,
                microsecond=0
            )
            if now.time() > target_time:
                target_datetime += timedelta(days=1)
            delay = (target_datetime - now).total_seconds()
            await asyncio.sleep(delay)
            await send_daily_report()
        except Exception as e:
            print(f"Ошибка в планировщике отчета: {e}")
            await asyncio.sleep(60)

async def main():
    
    signal.signal(signal.SIGINT, handle_sigint)
    
    dp = Dispatcher()
    dp.include_router(router)
    
    
    asyncio.create_task(schedule_daily_report())
    
    try:
        print("Бот запущен. Для завершения нажмите Ctrl+C")
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Ошибка в главном цикле: {e}")
    finally:
        await shutdown(dp, bot)

@router.callback_query(F.data.startswith("topic_menu_"))
async def topic_menu(callback: types.CallbackQuery):
    try:
        
        parts = callback.data.split("_")
        
        
        if len(parts) < 5:
            print(f"Неверный формат callback_data: {callback.data}")
            await callback.answer("❌ Ошибка: неверный формат данных")
            return
            
        
        if parts[2] == "20":  
            topic_type = f"drops_{parts[2]}-{parts[3]}"  
            try:
                topic_id = int(parts[4]) if parts[4] != 'None' else None
            except (ValueError, TypeError):
                print(f"Неверный topic_id в callback_data: {parts[4]}")
                await callback.answer("❌ Ошибка: неверный ID темы")
                return
            display_type = "20-25"  
        else:  
            topic_type = f"drops_{parts[2]}_{parts[3]}"  
            try:
                topic_id = int(parts[4]) if parts[4] != 'None' else None
            except (ValueError, TypeError):
                print(f"Неверный topic_id в callback_data: {parts[4]}")
                await callback.answer("❌ Ошибка: неверный ID темы")
                return
            display_type = f"{parts[2]}/{parts[3]}"  
        
        
        if topic_id is None:
            topic_id = callback.message.message_thread_id if callback.message.message_thread_id else 0
            print(f"Используем thread_id в качестве topic_id: {topic_id}")
        
        
        cursor.execute('''SELECT custom_name, topic_type FROM number_topics 
                         WHERE chat_id = ? AND topic_id = ?''', 
                      (callback.message.chat.id, topic_id))
        result = cursor.fetchone()
        
        if result:
            custom_name, db_topic_type = result
            display_name = custom_name if custom_name else display_type
        else:
            display_name = display_type
        
        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(text="Выбрать", callback_data=f"select_{parts[2]}_{parts[3]}_{topic_id}")
        )
        builder.row(
            InlineKeyboardButton(text="Назад", callback_data="settings"),
            InlineKeyboardButton(text="Настроить офисы", callback_data=f"configure_{parts[2]}_{parts[3]}_{topic_id}")
        )
        await callback.message.edit_text(
            f"Настройки для темы '{display_name}':",
            parse_mode="HTML",
            reply_markup=builder.as_markup()
        )
        await callback.answer()
    except Exception as e:
        print(f"Ошибка в topic_menu: {e}")
        print(f"Callback data: {callback.data}")
        print(f"Parts: {parts}")
        await callback.answer(f"❌ Ошибка: {str(e)}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПолучен сигнал завершения работы")
    except Exception as e:
        print(f"Критическая ошибка: {e}")
    finally:
        
        if 'conn' in globals() and conn:
            conn.close()
            print("Соединение с базой данных закрыто")
        sys.exit(0)