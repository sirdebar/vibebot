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


is_shutting_down = False

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
    if sys.platform == 'win32':
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
    conn = sqlite3.connect('bot_db.sqlite')
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
                    topic_name TEXT,
                    is_active BOOLEAN DEFAULT 1)''')

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

    cursor.execute('''CREATE TABLE IF NOT EXISTS last_messages (
                    chat_id INTEGER PRIMARY KEY,
                    message_id INTEGER)''')

    
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
                status TEXT)''')

    
    for column in ['registration_time', 'report_message_id', 'status']:
        try:
            cursor.execute(f'ALTER TABLE phone_messages ADD COLUMN {column} {("TEXT" if column in ["registration_time", "status"] else "INTEGER")}')
        except sqlite3.OperationalError:
            pass

    conn.commit()
    return conn

conn = init_db()
cursor = conn.cursor()

ALLOWED_USERS = []  

BOT_TOKEN = ""  

REPORT_USER_ID = 0  

router = Router()
bot = Bot(token=BOT_TOKEN)
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
    cursor.execute('SELECT topic_id FROM number_topics WHERE chat_id = ? AND topic_name = "reports" AND is_active = 1', (chat_id,))
    reports_topic = cursor.fetchone()
    return reports_topic[0] if reports_topic else None

def save_settings(chat_id, reports_topic=None):
    if reports_topic is not None:
        cursor.execute('UPDATE number_topics SET is_active = 0 WHERE chat_id = ? AND topic_name = "reports"', (chat_id,))
        cursor.execute('INSERT INTO number_topics (chat_id, topic_id, topic_name, is_active) VALUES (?, ?, ?, 1)',
                      (chat_id, reports_topic, "reports"))
    conn.commit()

def preprocess_image(image_path: str) -> str:
    img = cv2.imread(image_path)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    img_resized = cv2.resize(gray, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
    _, binary = cv2.threshold(img_resized, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    processed_path = f"processed_{image_path}"
    cv2.imwrite(processed_path, binary)
    return processed_path

def recognize_code(image_path: str) -> str:
    try:
        processed_path = preprocess_image(image_path)
        img = Image.open(processed_path)
        custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        text = pytesseract.image_to_string(img, config=custom_config).strip().upper()
        match = re.search(r'([A-Z0-9]{4})[-\s]*([A-Z0-9]{4})', text)
        if match:
            return f"{match.group(1)}-{match.group(2)}"
        return "Код не распознан"
    except Exception as e:
        print(f"Ошибка распознавания: {e}")
        return "Ошибка распознавания"
    finally:
        for f in [image_path, processed_path]:
            if os.path.exists(f):
                os.remove(f)

@router.message(Command("resetdb"))
async def cmd_resetdb(message: Message, state: FSMContext):
    print(f"Received /resetdb from user {message.from_user.id} in chat {message.chat.id}")
    try:
        if message.chat.type != 'private':
            await message.answer("❌ Команда доступна только в личных сообщениях.")
            return
        if message.from_user.id not in ALLOWED_USERS:
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
        if message.from_user.id in ALLOWED_USERS:
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
        is_allowed = user_id in ALLOWED_USERS

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
    if message.from_user.id not in ALLOWED_USERS:
        await message.answer("Только разрешенные пользователи могут использовать эту команду.")
        return
    number_processing_enabled = False
    await message.answer("Прием номеров остановлен. Используйте /start для возобновления.")

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

@router.message(Command("settings"))
async def cmd_settings(message: Message):
    print(f"Получена команда /settings от пользователя {message.from_user.id} в чате {message.chat.id}")
    
    
    user_id = message.from_user.id
    if user_id not in ALLOWED_USERS:
        print(f"Пользователь {user_id} не имеет прав")
        await message.answer("❌ Только разрешенные пользователи могут использовать эту команду!")
        return

    
    cursor.execute('SELECT COUNT(*) FROM office_chats WHERE user_id = ?', (user_id,))
    has_office_chats = cursor.fetchone()[0] > 0
    
    cursor.execute('SELECT COUNT(*) FROM drops_chats WHERE user_id = ?', (user_id,))
    has_drops_chat = cursor.fetchone()[0] > 0
    
    if not (has_office_chats and has_drops_chat):
        await message.answer("❌ Сначала настройте чаты через команду /start в личных сообщениях с ботом!")
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
            
            builder.row(
                InlineKeyboardButton(
                    text="📥 Приемка номеров", 
                    callback_data=f"set_drops_{message.message_thread_id}"
                ),
                InlineKeyboardButton(
                    text="📊 Отчеты", 
                    callback_data=f"set_reports_{message.message_thread_id}"
                )
            )
            await message.answer(
                "⚙️ <b>Настройки тем</b>\n\nВыберите тип темы для текущего чата:",
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
        
        cursor.execute(
            'DELETE FROM number_topics WHERE chat_id = ? AND topic_name = "drops"',
            (callback.message.chat.id,)
        )
        
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
        
        cursor.execute(
            'DELETE FROM number_topics WHERE chat_id = ? AND topic_name = "reports"',
            (callback.message.chat.id,)
        )
        
        cursor.execute(
            'INSERT INTO number_topics (chat_id, topic_id, topic_name, is_active) '
            'VALUES (?, ?, ?, 1)',
            (callback.message.chat.id, topic_id, "reports")
        )
        conn.commit()
        await callback.answer("✅ Тема отчетов успешно установлена!")
        await callback.message.delete()
    except Exception as e:
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
                       (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                       (phone, original_message.message_id, original_message.chat.id,
                        original_message.from_user.id, original_message.from_user.username,
                        original_message.from_user.first_name, original_message.from_user.last_name,
                        datetime.now(pytz.timezone('Europe/Moscow')).strftime('%Y-%m-%d %H:%M:%S')))
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

        
        cursor.execute('''INSERT INTO num_requests 
                    (office_chat_id, drops_chat_id, request_message_id, status)
                    VALUES (?, ?, ?, 'pending')''',
                  (message.chat.id, drops_chat, message.message_id))
        conn.commit()

        
        cursor.execute(
            '''SELECT topic_id FROM number_topics 
            WHERE chat_id = ? AND topic_name = "drops" AND is_active = 1''',
            (drops_chat,)
        )
        drops_topic = cursor.fetchone()
        
        if not drops_topic:
            await message.reply("⚠️ Тема для приемки не настроена! Используйте /settings в чате дропов")
            return

        
        cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ?', (drops_chat,))
        last_message = cursor.fetchone()
        
        if last_message:
            try:
                await bot.delete_message(drops_chat, last_message[0])
            except Exception as e:
                if not await safe_handle_error(e, {'message_id': last_message[0], 'drops_chat_id': drops_chat}):
                    print(f"Ошибка удаления сообщения: {e}")

        
        try:
            
            cursor.execute('''SELECT COUNT(*) FROM num_requests 
                            WHERE status = 'pending' AND drops_chat_id = ?''',
                          (drops_chat,))
            pending_count = cursor.fetchone()[0]

            new_message = await bot.send_message(
                chat_id=drops_chat,
                text=f"📱 Требуется номеров: {pending_count}\n\n⚠️ Требуются номера!",
                parse_mode="HTML",
                message_thread_id=drops_topic[0]
            )
            
            
            cursor.execute(
                'INSERT OR REPLACE INTO last_messages (chat_id, message_id) VALUES (?, ?)',
                (drops_chat, new_message.message_id)
            )
            conn.commit()
            
            await message.reply("✅ Запрос на номер отправлен в группу приемки")
            
        except Exception as e:
            if not await safe_handle_error(e, {'message_id': message.message_id, 'drops_chat_id': drops_chat}):
                await message.reply(f"❌ Ошибка отправки запроса: {str(e)}")
                
    except Exception as e:
        print(f"Critical error in handle_numbers_request: {e}")
        print(f"Traceback: {traceback.format_exc()}")
        await message.reply("❌ Произошла критическая ошибка при обработке запроса")

@router.message(F.text)
async def handle_phone_number(message: Message):
    try:
        
        if not is_drops_chat(message.chat.id):
            return

        
        cursor.execute('''SELECT topic_id FROM number_topics 
                       WHERE chat_id = ? 
                       AND topic_name = "drops" 
                       AND is_active = 1''', (message.chat.id,))
        drops_topic = cursor.fetchone()
        
        if not drops_topic or message.message_thread_id != drops_topic[0]:
            return

        
        phone = extract_phone(message.text)
        if not phone:
            return

        try:
            
            cursor.execute('''SELECT request_id, office_chat_id, request_message_id 
                            FROM num_requests 
                            WHERE status = 'pending' 
                            AND drops_chat_id = ?
                            LIMIT 1''', (message.chat.id,))
            request = cursor.fetchone()
            
            if not request:
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
                            user_id, username, first_name, last_name, registration_time, report_message_id)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)''',
                           (phone, message.message_id, confirmation.message_id, message.chat.id,
                            message.from_user.id, message.from_user.username,
                            message.from_user.first_name, message.from_user.last_name))
            
            
            cursor.execute('''UPDATE num_requests 
                            SET status = 'fulfilled' 
                            WHERE request_id = ?''', (request_id,))
            conn.commit()

            
            cursor.execute('''SELECT COUNT(*) FROM num_requests 
                            WHERE status = 'pending' 
                            AND drops_chat_id = ?''', (message.chat.id,))
            new_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT message_id FROM last_messages WHERE chat_id = ?', (message.chat.id,))
            last_msg = cursor.fetchone()
            
            if last_msg:
                try:
                    await bot.edit_message_text(
                        chat_id=message.chat.id,
                        message_id=last_msg[0],
                        text=f"📱 Требуется номеров: {new_count}\n\n⚠️ Требуются номера!",
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
        cursor.execute('''SELECT topic_id FROM number_topics 
                        WHERE chat_id = ? AND topic_name = "drops" AND is_active = 1''',
                        (drops_chat,))
        drops_topic = cursor.fetchone()
        
        if not drops_topic:
            await message.reply("❌ Ошибка: не настроена тема для приема в чате дропов")
            return

        cursor.execute('SELECT user_message_id FROM phone_messages WHERE phone = ? AND chat_id = ?', 
                      (phone, drops_chat))
        user_message = cursor.fetchone()

        sent_msg = await bot.send_photo(
            chat_id=drops_chat,
            photo=message.photo[-1].file_id,
            caption=f"📱 {phone}",
            parse_mode="HTML",
            message_thread_id=drops_topic[0],
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
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            
            return True
        elif "message to edit not found" in str(e).lower():
            print(f"Сообщение {message_id} для редактирования не найдено")
        else:
            print(f"Ошибка при редактировании сообщения: {e}")
        return False
    except Exception as e:
        print(f"Неожиданная ошибка при редактировании сообщения: {e}")
        return False

@router.callback_query(F.data.startswith("status_"))
async def handle_registration_status(callback: types.CallbackQuery):
    try:
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
                                    WHERE chat_id = ? AND topic_name = "reports" AND is_active = 1''', 
                                    (drops_chat,))
                    drops_reports_topic = cursor.fetchone()
                    print(f"Drops reports topic: {drops_reports_topic}")
                    
                    if drops_reports_topic:
                        
                        try:
                            
                            message_text = f"{phone} {moscow_time} {user_mention}"
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
                                WHERE chat_id = ? AND topic_name = "drops" AND is_active = 1''',
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
                                WHERE chat_id = ? AND topic_name = "drops" AND is_active = 1''',
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
        
        if not is_office_chat(callback.message.chat.id):
            await callback.answer("❌ Эта команда доступна только в офисном чате!")
            return
        
        
        drops_chat = get_drops_chat_for_office(callback.message.chat.id)
        
        if not drops_chat:
            await callback.answer("❌ Чат дропов не настроен для этого офиса!")
            return

        
        cursor.execute('''INSERT INTO num_requests 
                    (office_chat_id, drops_chat_id, request_message_id, status)
                    VALUES (?, ?, ?, 'pending')''',
                  (callback.message.chat.id, drops_chat, callback.message.message_id))
        conn.commit()

        
        cursor.execute(
            '''SELECT topic_id FROM number_topics 
            WHERE chat_id = ? AND topic_name = "drops" AND is_active = 1''',
            (drops_chat,)
        )
        drops_topic = cursor.fetchone()
        
        if not drops_topic:
            await callback.answer("⚠️ Тема для приемки не настроена! Используйте /settings в чате дропов")
            return

        
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
            message_thread_id=drops_topic[0]
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
        phone = callback.data.split("_")[1]
        
        
        cursor.execute('''SELECT registration_time, user_id, username, first_name, last_name, 
                         chat_id, report_message_id 
                         FROM phone_messages WHERE phone = ?''', (phone,))
        reg_info = cursor.fetchone()
        
        if not reg_info:
            await callback.answer("❌ Информация о регистрации не найдена")
            return
            
        reg_time, user_id, username, first_name, last_name, drops_chat, report_message_id = reg_info
        
        if not report_message_id:
            await callback.answer("❌ Не найдено сообщение отчета")
            return
            
        user_mention = f"@{username}" if username else f"[{first_name} {last_name}](tg://user?id={user_id})"
        
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_time = datetime.now(moscow_tz)
        reg_datetime = datetime.strptime(reg_time, '%Y-%m-%d %H:%M:%S')
        reg_datetime = moscow_tz.localize(reg_datetime)
        
        
        time_diff = current_time - reg_datetime
        minutes = int(time_diff.total_seconds() // 60)
        seconds = int(time_diff.total_seconds() % 60)
        
        
        reg_time_str = reg_datetime.strftime('%H:%M')
        current_time_str = current_time.strftime('%H:%M')
        
        
        cursor.execute('''SELECT topic_id FROM number_topics 
                         WHERE chat_id = ? AND topic_name = "reports" AND is_active = 1''', 
                         (drops_chat,))
        drops_reports_topic = cursor.fetchone()
        
        if drops_reports_topic:
            try:
                
                new_text = f"{phone} {reg_time_str}-{current_time_str} ({minutes:02d}:{seconds:02d}) {user_mention}"
                
                
                success = await safe_edit_message(
                    chat_id=drops_chat,
                    message_id=report_message_id,
                    new_text=new_text
                )
                
                if success:
                    
                    button_message_success = await safe_edit_message(
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
                print(f"Error updating message: {str(e)}")
                await callback.answer("❌ Ошибка при обновлении сообщения")
        else:
            await callback.answer("❌ Тема для отчетов не настроена!")
            
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {str(e)}")
        print(f"Error in handle_slet: {traceback.format_exc()}")

@router.callback_query(lambda c: c.data == "resetdb_confirm")
async def resetdb_confirm(callback_query: types.CallbackQuery):
    if callback_query.from_user.id not in ALLOWED_USERS:
        await callback_query.answer("❌ Команда доступна только разрешённым пользователям.")
        return
    try:
        await callback_query.message.edit_text("⏳ Очистка базы данных... Бот будет перезапущен.")
        conn.close()
        os.remove('bot_db.sqlite')
        
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await callback_query.message.edit_text(f"❌ Ошибка при очистке базы данных: {e}")

@router.callback_query(lambda c: c.data == "resetdb_cancel")
async def resetdb_cancel(callback_query: types.CallbackQuery):
    await callback_query.message.edit_text("❌ Очистка базы данных отменена.")

async def send_daily_report():
    """Отправка ежедневного отчета пользователю"""
    try:
        REPORT_USER_ID = 7037364839  
        
        
        moscow_tz = pytz.timezone('Europe/Moscow')
        current_date = datetime.now(moscow_tz).date()
        
        
        cursor.execute('SELECT DISTINCT chat_id FROM drops_chats')
        drops_chats = cursor.fetchall()
        
        
        total_report = f"📊 Сводный отчет за {current_date.strftime('%d.%m.%Y')}:\n\n"
        total_registrations = 0
        
        for (drops_chat_id,) in drops_chats:
            try:
                
                cursor.execute('''SELECT phone, registration_time, username, first_name, last_name, user_id 
                                FROM phone_messages 
                                WHERE chat_id = ? 
                                AND date(registration_time) = ?''',
                                (drops_chat_id, current_date.strftime('%Y-%m-%d')))
                registrations = cursor.fetchall()
                
                if registrations:
                    for phone, reg_time, username, first_name, last_name, user_id in registrations:
                        
                        reg_datetime = datetime.strptime(reg_time, '%Y-%m-%d %H:%M:%S')
                        reg_time_str = reg_datetime.strftime('%H:%M')
                        
                        
                        if username:
                            user_mention = f"@{username}"
                        elif first_name:
                            user_mention = f"{first_name}"
                        else:
                            user_mention = f"ID: {user_id}"
                        
                        total_report += f"📱 {phone} {reg_time_str} {user_mention}\n"
                    
                    total_registrations += len(registrations)
                
            except Exception as e:
                print(f"Ошибка при обработке чата {drops_chat_id}: {e}")
                continue
        
        
        if total_registrations > 0:
            total_report += f"\n📈 Всего регистраций: {total_registrations}"
        else:
            total_report = "📊 Отчет за день:\nРегистраций не было"
        
        
        try:
            await bot.send_message(
                chat_id=REPORT_USER_ID,
                text=total_report,
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка при отправке отчета пользователю {REPORT_USER_ID}: {e}")
                
    except Exception as e:
        print(f"Критическая ошибка в send_daily_report: {e}")
        print(traceback.format_exc())

async def schedule_daily_report():
    """Планировщик ежедневных отчетов"""
    while not is_shutting_down:
        try:
            
            moscow_tz = pytz.timezone('Europe/Moscow')
            now = datetime.now(moscow_tz)
            
            
            target_time = now.replace(hour=22, minute=30, second=0, microsecond=0)
            if now >= target_time:
                target_time = target_time + timedelta(days=1)
            
            
            wait_seconds = (target_time - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            
            
            await send_daily_report()
            
            
            await asyncio.sleep(60)
            
        except Exception as e:
            print(f"Ошибка в планировщике отчетов: {e}")
            
            await asyncio.sleep(300)

async def main():
    
    signal.signal(signal.SIGINT, handle_sigint)
    
    dp = Dispatcher()
    dp.include_router(router)
    
    try:
        print("Бот запущен. Для завершения нажмите Ctrl+C")
        
        asyncio.create_task(schedule_daily_report())
        await dp.start_polling(bot)
    except Exception as e:
        print(f"Ошибка в главном цикле: {e}")
    finally:
        await shutdown(dp, bot)

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