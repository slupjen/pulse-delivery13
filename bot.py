import sys
import os
import asyncio
import logging
import time
import random
import signal
import aiohttp
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.methods import GetChatMember
from aiogram.exceptions import TelegramBadRequest
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web
from dotenv import load_dotenv

# Завантаження змінних середовища
load_dotenv()

# Виправлення кодування для Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.system("chcp 65001 > nul")

# ==================== КОНФІГУРАЦІЯ ====================
API_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
ADMIN_ID = int(os.getenv('ADMIN_ID', 8154128217))
CHANNEL_ID = os.getenv('CHANNEL_ID', "@pulsedelivery")
GEOCODING_API_KEY = os.getenv('GEOCODING_API_KEY')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Налаштування вебхуку для Render.com
WEB_SERVER_HOST = os.getenv('WEB_SERVER_HOST', '0.0.0.0')
WEB_SERVER_PORT = int(os.getenv('PORT', 8000))
WEBHOOK_PATH = os.getenv('WEBHOOK_PATH', '/webhook')
WEBHOOK_SECRET = os.getenv('WEBHOOK_SECRET')
BASE_WEBHOOK_URL = os.getenv('WEBHOOK_URL')

BLACKLIST = []
RATE_LIMIT = 10
RATE_PERIOD = 60
MAX_MESSAGES_PER_MIN = 40

# Глобальна змінна для керування станом бота
BOT_RUNNING = True

# ==================== ЛОГУВАННЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_errors.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==================== ІНІЦІАЛІЗАЦІЯ ====================
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = RedisStorage.from_url(REDIS_URL)  # Використовуємо Redis для зберігання стану
dp = Dispatcher(storage=storage)

# ==================== СТАНИ ФОРМИ ====================
class OrderForm(StatesGroup):
    captcha = State()
    name = State()
    phone = State()
    item = State()
    delivery_type = State()
    pickup_address = State()
    delivery_address_method = State()
    delivery_address = State()
    delivery_location = State()
    delivery_time = State()
    custom_time = State()
    payment = State()
    change_from = State()
    promo_code = State()
    review = State()

# ==================== СИСТЕМА ЗАХИСТУ ====================
class ProtectionMiddleware(BaseMiddleware):
    def __init__(self):
        self.user_activity = {}
        self.message_timestamps = {}

    async def __call__(self, handler, event: types.Message, data):
        global BOT_RUNNING

        if not BOT_RUNNING:
            return

        user_id = event.from_user.id
        now = time.time()

        if user_id == ADMIN_ID:
            return await handler(event, data)

        if user_id in BLACKLIST:
            await event.answer("⛔ Вам заборонено використовувати бота.")
            return

        # Ліміт RATE_LIMIT за RATE_PERIOD секунд
        if user_id in self.user_activity:
            self.user_activity[user_id] = [
                t for t in self.user_activity[user_id]
                if now - t < RATE_PERIOD
            ]
            if len(self.user_activity[user_id]) >= RATE_LIMIT:
                await event.answer(f"❗ Занадто багато запитів. Спробуйте через {RATE_PERIOD} сек.")
                return
        self.user_activity.setdefault(user_id, []).append(now)

        # Ліміт MAX_MESSAGES_PER_MIN за 60 секунд
        if user_id not in self.message_timestamps:
            self.message_timestamps[user_id] = []
        self.message_timestamps[user_id].append(now)
        self.message_timestamps[user_id] = [
            t for t in self.message_timestamps[user_id]
            if now - t < 60
        ]
        if len(self.message_timestamps[user_id]) > MAX_MESSAGES_PER_MIN:
            BLACKLIST.append(user_id)
            logger.warning(f"User {user_id} added to blacklist")
            await event.answer("⛔ Ваш акаунт тимчасово заблоковано за підозрілу активність.")
            return

        return await handler(event, data)

# ==================== КЛАВІАТУРИ ====================
def style_text(text, emoji=None):
    if emoji:
        text = f"{emoji} {text}"
    return text

def new_order_kb():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=style_text("Оформити нове замовлення", "🛍️")))
    return builder.as_markup(resize_keyboard=True)

def phone_request_kb():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=style_text("Надіслати номер телефону", "📱"), request_contact=True))
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

def item_input_kb():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=style_text("Це все", "✅")))
    builder.add(KeyboardButton(text=style_text("Скасувати замовлення", "❌")))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

def delivery_type_kb():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=style_text("Моє відправлення", "📦"), callback_data="sender"))
    builder.add(InlineKeyboardButton(text=style_text("Доставка", "🚚"), callback_data="delivery"))
    builder.adjust(1)
    return builder.as_markup()

def delivery_address_method_kb():
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text=style_text("Ввести адресу вручну", "✍️")))
    builder.add(KeyboardButton(text=style_text("Поділитися геолокацією", "📍"), request_location=True))
    builder.add(KeyboardButton(text=style_text("Скасувати замовлення", "❌")))
    builder.adjust(1)
    return builder.as_markup(resize_keyboard=True, one_time_keyboard=True)

def delivery_time_kb():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=style_text("Якнайшвидше", "⚡"), callback_data="asap"))
    builder.add(InlineKeyboardButton(text=style_text("Вказати свій час", "⏱️"), callback_data="custom_time"))
    builder.adjust(1)
    return builder.as_markup()

def payment_kb():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=style_text("Готівка", "💵"), callback_data="payment_cash"))
    builder.add(InlineKeyboardButton(text=style_text("Переказ на карту", "💳"), callback_data="payment_cashless"))
    builder.adjust(1)
    return builder.as_markup()

def review_kb():
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=style_text("Редагувати замовлення", "✏️"), callback_data="edit_order"))
    builder.add(InlineKeyboardButton(text=style_text("Ввести промокод", "🎟️"), callback_data="enter_promo"))
    builder.add(InlineKeyboardButton(text=style_text("Надіслати замовлення", "📨"), callback_data="send_order"))
    builder.adjust(1)
    return builder.as_markup()

def get_items_edit_kb(items: list, can_finish: bool = True):
    builder = InlineKeyboardBuilder()
    for i, item in enumerate(items, 1):
        item_text = f"{i}: {item[:15]}..." if len(item) > 15 else f"{i}: {item}"
        builder.add(InlineKeyboardButton(text=f"❌ Видалити {item_text}", callback_data=f"remove_item_{i-1}"))
    
    if can_finish:
        builder.add(InlineKeyboardButton(text="✅ Завершити редагування", callback_data="finish_editing"))
    
    builder.add(InlineKeyboardButton(text="➕ Додати ще товар", callback_data="add_more_items"))
    builder.adjust(1)
    return builder.as_markup()

def admin_main_kb():
    global BOT_RUNNING
    builder = InlineKeyboardBuilder()
    
    if BOT_RUNNING:
        builder.add(InlineKeyboardButton(text="⏸️ Призупинити бота", callback_data="admin_pause_bot"))
    else:
        builder.add(InlineKeyboardButton(text="▶️ Запустити бота", callback_data="admin_start_bot"))
    
    builder.add(InlineKeyboardButton(text="📋 Чорний список", callback_data="admin_blacklist"))
    builder.add(InlineKeyboardButton(text="🔄 Статус", callback_data="admin_status"))
    builder.add(InlineKeyboardButton(text="⏹️ Зупинити бота", callback_data="admin_stop_bot"))
    builder.adjust(1, 2, 1)
    return builder.as_markup()

def admin_blacklist_kb(users: list):
    builder = InlineKeyboardBuilder()
    for user_id in users:
        builder.add(InlineKeyboardButton(
            text=f"❌ Видалити {user_id}",
            callback_data=f"unblock_{user_id}"
        ))
    builder.row(
        InlineKeyboardButton(text="👤 Додати користувача", callback_data="admin_add_to_blacklist"),
        InlineKeyboardButton(text="🔄 Оновити", callback_data="admin_blacklist_refresh")
    )
    builder.row(
        InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")
    )
    builder.adjust(2)
    return builder.as_markup()

def admin_accept_kb(order_id: str):
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(
        text="✅ Прийняти замовлення",
        callback_data=f"accept_order_{order_id}"
    ))
    return builder.as_markup()

# ==================== КЛЮЧОВІ ФУНКЦІЇ ====================
def escape_html(text):
    return (text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )

async def generate_captcha():
    a = random.randint(1, 5)
    b = random.randint(1, 5)
    return f"{a} + {b}", a + b

async def check_subscription(user_id: int):
    try:
        member = await bot(GetChatMember(chat_id=CHANNEL_ID, user_id=user_id))
        return member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logger.error(f"Помилка перевірки підписки: {e}")
        return False

async def get_address_from_coords(lat: float, lon: float):
    """Отримати адресу за координатами за допомогою Nominatim API"""
    if not GEOCODING_API_KEY:
        logger.warning("GEOCODING_API_KEY не встановлено")
        return None
        
    try:
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&zoom=18&addressdetails=1"
        headers = {"User-Agent": "Telegram Delivery Bot"}
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                data = await response.json()
                
                if 'address' in data:
                    address = data['address']
                    components = []
                    
                    if 'road' in address:
                        components.append(address['road'])
                    if 'house_number' in address:
                        components.append(address['house_number'])
                    if 'suburb' in address:
                        components.append(address['suburb'])
                    if 'city' in address:
                        components.append(address['city'])
                    
                    return ", ".join(components) if components else None
                
                return None
    except Exception as e:
        logger.error(f"Помилка отримання адреси: {e}")
        return None

# ==================== ОСНОВНІ КОМАНДИ ====================
@dp.message(Command("start", "help"))
async def send_welcome(message: types.Message, state: FSMContext):
    global BOT_RUNNING
    if not BOT_RUNNING:
        await message.answer("⏸️ Бот тимчасово призупинено. Спробуйте пізніше.")
        return
        
    try:
        if not await check_subscription(message.from_user.id):
            builder = InlineKeyboardBuilder()
            builder.add(InlineKeyboardButton(
                text="Підписатися", 
                url=f"https://t.me/{CHANNEL_ID[1:]}" if CHANNEL_ID.startswith("@") else f"https://t.me/{CHANNEL_ID}"
            ))
            builder.add(InlineKeyboardButton(
                text="Я підписався", 
                callback_data="check_subscription"
            ))
            builder.adjust(1)
            
            await message.answer(
                "📢 Підпишіться на наш канал, щоб продовжити:",
                reply_markup=builder.as_markup()
            )
            return
        
        # Якщо підписка є, генеруємо капчу
        captcha_text, answer = await generate_captcha()
        await state.update_data(captcha_answer=answer)
        await message.answer(f"🔒 Введіть результат: {captcha_text} = ?")
        await state.set_state(OrderForm.captcha)
    except Exception as e:
        logger.error(f"Error in send_welcome: {e}")
        await message.answer("❌ Сталася помилка. Спробуйте ще раз.")

@dp.callback_query(F.data == "check_subscription")
async def check_subscription_callback(callback: types.CallbackQuery, state: FSMContext):
    global BOT_RUNNING
    if not BOT_RUNNING:
        await callback.message.answer("⏸️ Бот тимчасово призупинено. Спробуйте пізніше.")
        return
        
    try:
        if await check_subscription(callback.from_user.id):
            await callback.message.delete()
            captcha_text, answer = await generate_captcha()
            await state.update_data(captcha_answer=answer)
            await callback.message.answer(f"🔒 Введіть результат: {captcha_text} = ?")
            await state.set_state(OrderForm.captcha)
        else:
            await callback.answer("❗ Будь ласка, спочатку підпишіться на канал", show_alert=True)
    except Exception as e:
        logger.error(f"Error in check_subscription_callback: {e}")
        await callback.answer("❌ Сталася помилка. Спробуйте ще раз.", show_alert=True)

@dp.message(OrderForm.captcha)
async def check_captcha(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if "captcha_answer" in data:
        if message.text.isdigit() and int(message.text) == data["captcha_answer"]:
            await message.answer("✅ Вітаємо! Тепер ви можете користуватися ботом.")
            await state.clear()
            welcome_text = "Привіт! Давайте оформимо замовлення.\nЯк вас звати?"
            await message.answer(welcome_text, reply_markup=new_order_kb())
            await state.set_state(OrderForm.name)
        else:
            await message.answer("❌ Невірно. Спробуйте ще раз.")
    else:
        await message.answer("❌ Помилка перевірки. Спробуйте /start знову.")
        await state.clear()

@dp.message(F.text.contains("нове замовлення"))
async def new_order(message: types.Message, state: FSMContext):
    global BOT_RUNNING
    if not BOT_RUNNING:
        await message.answer("⏸️ Бот тимчасово призупинено. Спробуйте пізніше.")
        return
        
    await state.clear()
    await message.answer("Як вас звати? (лише літери, 2-30 символів)", 
                        reply_markup=ReplyKeyboardRemove())
    await state.set_state(OrderForm.name)

@dp.message(OrderForm.name)
async def get_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name.replace(" ", "").isalpha() or len(name) < 2 or len(name) > 30:
        await message.answer("❗ Будь ласка, введіть коректне ім'я (лише літери, 2-30 символів)")
        return
    
    await state.update_data(name=escape_html(name))
    request_text = "Ваш номер телефону? Натисніть кнопку або введіть у форматі +380XXXXXXXXX"
    await message.answer(request_text, reply_markup=phone_request_kb())
    await state.set_state(OrderForm.phone)

@dp.message(OrderForm.phone)
async def get_phone(message: types.Message, state: FSMContext):
    phone = None
    
    if message.contact:
        phone = message.contact.phone_number
    elif message.text:
        phone = message.text.strip()
        # Перевірка коректності номера
        if not phone.replace("+", "").isdigit() or len(phone) < 10:
            await message.answer("❗ Будь ласка, введіть коректний номер телефону (наприклад, +380123456789)")
            return
    else:
        await message.answer("❗ Будь ласка, надішліть номер телефону")
        return
    
    await state.update_data(phone=escape_html(phone))
    await state.update_data(item_text="", item_photos=[])
    request_text = "Що потрібно доставити? Надішліть опис, фото або все разом.\nКоли закінчите, натисніть кнопку \"Це все\" внизу."
    await message.answer(request_text, reply_markup=item_input_kb())
    await state.set_state(OrderForm.item)

@dp.message(OrderForm.item, F.content_type.in_({"text", "photo"}))
async def collect_item_data(message: types.Message, state: FSMContext):
    # Обробка кнопок завершення або скасування
    if message.text and ("Це все" in message.text or "Скасувати" in message.text):
        data = await state.get_data()
        item_text = data.get("item_text", "").strip()
        
        if not item_text and not data.get("item_photos"):
            await message.answer("❗ Ви не додали жодного товару. Будь ласка, додайте хоча б один товар.", 
                                reply_markup=item_input_kb())
            return
        
        if "Скасувати" in message.text:
            await state.clear()
            await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
            return
        
        request_text = "Відправляєте Ви чи потрібна доставка?"
        await message.answer(request_text, reply_markup=delivery_type_kb())
        await state.set_state(OrderForm.delivery_type)
        return
    
    # Обробка введення даних
    data = await state.get_data()

    if message.text:
        text = data.get("item_text", "") + escape_html(message.text) + "\n"
        await state.update_data(item_text=text)
    elif message.photo:
        photo = message.photo[-1].file_id
        photos = data.get("item_photos", [])
        if len(photos) < 25:
            photos.append(photo)
            await state.update_data(item_photos=photos)
        else:
            await message.answer("❗ Можна надіслати не більше 25 фото.", reply_markup=item_input_kb())
    
    await message.answer("Товар додано. Продовжуйте додавати товари або натисніть \"Це все\".", 
                        reply_markup=item_input_kb())

@dp.callback_query(F.data.in_({"sender", "delivery"}))
async def get_delivery_type(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    delivery_type = "Відправник" if callback.data == "sender" else "Одержувач"
    await state.update_data(delivery_type=delivery_type)

    if callback.data == "sender":
        builder = ReplyKeyboardBuilder()
        builder.add(KeyboardButton(text="Скасувати замовлення"))
        await callback.message.answer("Введіть адресу відправлення:",
                                    reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(OrderForm.pickup_address)
    else:
        await state.update_data(pickup_address="—")
        request_text = "Як ви хочете вказати адресу доставки?"
        await callback.message.answer(request_text, reply_markup=delivery_address_method_kb())
        await state.set_state(OrderForm.delivery_address_method)
    await callback.answer()

@dp.message(OrderForm.pickup_address)
async def get_pickup(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    if len(message.text) < 5:
        await message.answer("❗ Адреса занадто коротка. Будь ласка, введіть повну адресу")
        return
        
    await state.update_data(pickup_address=escape_html(message.text))
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="Скасувати замовлення"))
    await message.answer("Введіть адресу доставки:",
                        reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(OrderForm.delivery_address)

@dp.message(OrderForm.delivery_address_method)
async def handle_delivery_address_method(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    if message.location:
        location = message.location
        lat = location.latitude
        lon = location.longitude
        
        # Отримуємо адресу за координатами
        address_text = await get_address_from_coords(lat, lon)
        if not address_text:
            address_text = f"Координати: {lat:.6f}, {lon:.6f}"
        
        # Генеруємо посилання для обох карт
        maps_links = f"Google Maps: https://maps.google.com/?q={lat},{lon}\nApple Maps: https://maps.apple.com/?q={lat},{lon}"
        await state.update_data(delivery_location=maps_links)
        await state.update_data(delivery_address=address_text)  # Зберігаємо адресу текстом
        
        await message.answer(
            f"Дякуємо! Ваша геолокація збережена.\nАдреса: {address_text}",
            reply_markup=ReplyKeyboardRemove()
        )
        
        request_text = "Оберіть час доставки:"
        await message.answer(request_text, reply_markup=delivery_time_kb())
        await state.set_state(OrderForm.delivery_time)
    elif message.text and "Ввести адресу вручну" in message.text:
        builder = ReplyKeyboardBuilder()
        builder.add(KeyboardButton(text="Скасувати замовлення"))
        await message.answer("Будь ласка, введіть адресу доставки:",
                            reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(OrderForm.delivery_address)
    else:
        await message.answer("❗ Будь ласка, оберіть спосіб вказання адреси", 
                            reply_markup=delivery_address_method_kb())

@dp.message(OrderForm.delivery_address)
async def get_delivery_address(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    if len(message.text) < 5:
        await message.answer("❗ Адреса занадто коротка. Будь ласка, введіть повну адресу")
        return
        
    await state.update_data(delivery_address=escape_html(message.text))
    await state.update_data(delivery_location="—")
    
    request_text = "Оберіть час доставки:"
    await message.answer(request_text, reply_markup=delivery_time_kb())
    await state.set_state(OrderForm.delivery_time)

@dp.callback_query(F.data == "asap")
async def set_asap_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.update_data(delivery_time="Якнайшвидше ⚡")
    request_text = "Оберіть форму оплати:"
    await callback.message.answer(request_text, reply_markup=payment_kb())
    await state.set_state(OrderForm.payment)
    await callback.answer()

@dp.callback_query(F.data == "custom_time")
async def request_custom_time(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="Скасувати замовлення"))
    await callback.message.answer("Введіть бажаний час доставки (наприклад, 15:00):",
                                reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(OrderForm.custom_time)
    await callback.answer()

@dp.message(OrderForm.custom_time)
async def get_custom_time(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    if len(message.text) < 2:
        await message.answer("❗ Будь ласка, введіть коректний час (наприклад, 15:00)")
        return
        
    await state.update_data(delivery_time=f"⏰ {escape_html(message.text)}")
    request_text = "Оберіть форму оплати:"
    await message.answer(request_text, reply_markup=payment_kb())
    await state.set_state(OrderForm.payment)

@dp.callback_query(F.data.in_({"payment_cash", "payment_cashless"}))
async def get_payment(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    payment = "Готівка 💵" if callback.data == "payment_cash" else "Переказ на карту 💳"
    await state.update_data(payment=payment)

    if payment == "Готівка 💵":
        builder = ReplyKeyboardBuilder()
        builder.add(KeyboardButton(text="Скасувати замовлення"))
        await callback.message.answer("З якої суми потрібна решта? (наприклад, 500 грн)",
                                    reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(OrderForm.change_from)
    else:
        await show_order_review(callback.message, state)
        await state.set_state(OrderForm.review)
    await callback.answer()

@dp.message(OrderForm.change_from)
async def get_change_from(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    if not message.text.replace(" ", "").replace("грн", "").isdigit():
        await message.answer("❗ Будь ласка, введіть суму цифрами (наприклад, 500)")
        return
        
    await state.update_data(change_from=f"💲 {escape_html(message.text)}")
    await show_order_review(message, state)
    await state.set_state(OrderForm.review)

async def show_order_review(message: types.Message, state: FSMContext):
    data = await state.get_data()

    item_text = data.get("item_text", "").strip()
    items = [line.strip() for line in item_text.split('\n') if line.strip()]
    item_photos = data.get("item_photos", [])
    delivery_type = data.get("delivery_type", "—")
    pickup_address = data.get("pickup_address", "—")
    delivery_address = data.get("delivery_address", "—")
    delivery_location = data.get("delivery_location", "—")
    delivery_time = data.get("delivery_time", "—")
    payment = data.get("payment", "—")
    change_from = data.get("change_from", "—")

    if items:
        items_text = "\n".join(f"• {item}" for item in items)
    else:
        items_text = "—"
    
    review_text = (
        f"📋 <b>ПЕРЕВІРТЕ ВАШЕ ЗАМОВЛЕННЯ:</b>\n\n"
        f"👤 Ім'я: {data.get('name', '—')}\n"
        f"📱 Телефон: {data.get('phone', '—')}\n"
        f"📦 Що доставити:\n{items_text}\n"
        f"🚛 Тип: {delivery_type}\n"
        f"🏠 Адреса відправлення: {pickup_address}\n"
        f"📍 Адреса доставки: {delivery_address}\n"
    )
    
    if delivery_location != "—":
        if "\n" in delivery_location:  # Якщо є обидва посилання
            google_link, apple_link = delivery_location.split("\n")
            review_text += f"🗺️ Переглянути на: <a href='{google_link.split(': ')[1]}'>Google Maps</a> | <a href='{apple_link.split(': ')[1]}'>Apple Maps</a>\n"
        elif delivery_location.startswith("http"):  # Для зворотної сумісності
            review_text += f"🗺️ <a href='{delivery_location}'>Подивитися на мапі</a>\n"
    
    review_text += (
        f"⏰ Час доставки: {delivery_time}\n"
        f"💰 Оплата: {payment}\n"
    )
    
    if payment == "Готівка 💵":
        review_text += f"💲 Решта з: {change_from}\n"

    await message.answer(review_text, reply_markup=review_kb(), disable_web_page_preview=True)

    # Відправляємо всі фото окремими повідомленнями
    for photo_id in item_photos:
        await message.answer_photo(photo=photo_id)

@dp.callback_query(F.data == "edit_order")
async def edit_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    data = await state.get_data()
    
    item_text = data.get("item_text", "").strip()
    items = [line.strip() for line in item_text.split('\n') if line.strip()]
    item_photos = data.get("item_photos", [])
    
    if not items and not item_photos:
        await callback.message.answer(
            "Список товарів порожній. Додайте товари:",
            reply_markup=item_input_kb()
        )
        await state.set_state(OrderForm.item)
    else:
        message_text = "📋 Поточний список товарів:\n\n" + "\n".join(
            f"{i+1}. {item}" for i, item in enumerate(items))
        
        if item_photos:
            message_text += f"\n\n📷 Прикріплено фото: {len(item_photos)}"
        
        await callback.message.answer(
            message_text,
            reply_markup=get_items_edit_kb(items)
        )
    
    await callback.answer()

@dp.callback_query(F.data.startswith("remove_item_"))
async def remove_item(callback: types.CallbackQuery, state: FSMContext):
    item_index = int(callback.data.split("_")[-1])
    data = await state.get_data()
    
    item_text = data.get("item_text", "").strip()
    items = [line.strip() for line in item_text.split('\n') if line.strip()]
    item_photos = data.get("item_photos", [])
    
    if 0 <= item_index < len(items):
        items.pop(item_index)
        new_item_text = "\n".join(items)
        await state.update_data(item_text=new_item_text)
        
        if items:
            message_text = "📋 Оновлений список товарів:\n\n" + "\n".join(
                f"{i+1}. {item}" for i, item in enumerate(items))
            
            if item_photos:
                message_text += f"\n\n📷 Прикріплено фото: {len(item_photos)}"
            
            await callback.message.edit_text(
                message_text,
                reply_markup=get_items_edit_kb(items)
            )
        else:
            await callback.message.edit_text(
                "Список товарів порожній. Додайте товари:",
                reply_markup=None
            )
            await callback.message.answer(
                "Додайте товари:",
                reply_markup=item_input_kb()
            )
            await state.set_state(OrderForm.item)
    else:
        await callback.answer("❗ Неправильний індекс товару", show_alert=True)
    
    await callback.answer()

@dp.callback_query(F.data == "add_more_items")
async def add_more_items(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(
        "Додайте товари:",
        reply_markup=item_input_kb()
    )
    await state.set_state(OrderForm.item)
    await callback.answer()

@dp.callback_query(F.data == "finish_editing")
async def finish_editing(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    await show_order_review(callback.message, state)
    await state.set_state(OrderForm.review)
    await callback.answer()

@dp.callback_query(F.data == "enter_promo")
async def enter_promo_code(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    builder = ReplyKeyboardBuilder()
    builder.add(KeyboardButton(text="Скасувати замовлення"))
    await callback.message.answer(
        "Введіть промокод:",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(OrderForm.promo_code)
    await callback.answer()

@dp.message(OrderForm.promo_code)
async def process_promo_code(message: types.Message, state: FSMContext):
    if message.text == "Скасувати замовлення":
        await state.clear()
        await message.answer("Замовлення скасовано.", reply_markup=new_order_kb())
        return
        
    promo_code = message.text.strip()
    await state.update_data(promo_code=promo_code)
    
    # Відправляємо замовлення адміну
    client_id = message.from_user.id
    await state.update_data(user_id=client_id)
    
    await send_order_to_admin(message, state)
    await message.answer(
        f"✅ Дякуємо! Ваше замовлення з промокодом \"{promo_code}\" оформлено. Очікуйте підтвердження.",
        reply_markup=new_order_kb()
    )
    await state.clear()

@dp.callback_query(F.data == "send_order")
async def send_order(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_reply_markup(reply_markup=None)
    
    client_id = callback.from_user.id
    await state.update_data(user_id=client_id)
    
    await send_order_to_admin(callback.message, state)
    await callback.message.answer(
        "Дякуємо! Ваше замовлення оформлено. Очікуйте підтвердження.",
        reply_markup=new_order_kb()
    )
    await state.clear()
    await callback.answer()

async def send_order_to_admin(message: types.Message, state: FSMContext):
    data = await state.get_data()
    
    order_id = str(int(time.time()))[-6:]
    user_id = data.get('user_id')
    
    if not user_id:
        logger.error("Не знайдено user_id у стані")
        await message.answer("❌ Помилка: не вдалося ідентифікувати клієнта")
        return

    item_text = data.get('item_text', '').strip()
    items = [line.strip() for line in item_text.split('\n') if line.strip()]
    item_photos = data.get('item_photos', [])
    
    if items:
        items_text = "\n".join(f"• {item}" for item in items)
    else:
        items_text = "—"

    delivery_type = data.get('delivery_type', '—')
    pickup_address = data.get('pickup_address', '—')
    delivery_address = data.get('delivery_address', '—')  # Текстова адреса
    delivery_location = data.get('delivery_location', '—')
    delivery_time = data.get('delivery_time', '—')
    payment = data.get('payment', '—')
    change_from = data.get('change_from', '—')
    promo_code = data.get('promo_code', None)

    order_message = (
        f"🆕 <b>НОВЕ ЗАМОВЛЕННЯ #{order_id}:</b>\n\n"
        f"👤 Клієнт: {data.get('name', '—')} (ID: {user_id})\n"
        f"📱 Телефон: {data.get('phone', '—')}\n"
    )
    
    if promo_code:
        order_message += f"🎟️ Промокод: {promo_code}\n"
    
    order_message += (
        f"📦 Що доставити:\n{items_text}\n"
        f"🚛 Тип: {delivery_type}\n"
        f"🏠 Адреса відправлення: {pickup_address}\n"
        f"📍 Адреса доставки: {delivery_address}\n"  # Текстова адреса
    )
    
    if delivery_location != "—":
        if "\n" in delivery_location:  # Якщо є обидва посилання
            google_link, apple_link = delivery_location.split("\n")
            order_message += f"🗺️ Переглянути на: <a href='{google_link.split(': ')[1]}'>Google Maps</a> | <a href='{apple_link.split(': ')[1]}'>Apple Maps</a>\n"
        elif delivery_location.startswith("http"):  # Для зворотної сумісності
            order_message += f"🗺️ <a href='{delivery_location}'>Подивитися на мапі</a>\n"
    
    order_message += (
        f"⏰ Час доставки: {delivery_time}\n"
        f"💰 Оплата: {payment}\n"
    )
    
    if payment == "Готівка 💵":
        order_message += f"💲 Решта з: {change_from}\n"

    try:
        msg = await bot.send_message(
            chat_id=ADMIN_ID, 
            text=order_message, 
            reply_markup=admin_accept_kb(order_id),
            disable_web_page_preview=True
        )
        
        # Відправляємо всі фото окремими повідомленнями
        for photo_id in item_photos:
            await bot.send_photo(chat_id=ADMIN_ID, photo=photo_id)
            
    except Exception as e:
        logger.error(f"Не вдалося надіслати замовлення адміну: {e}")

@dp.callback_query(F.data.startswith("accept_order_"))
async def accept_order(callback: types.CallbackQuery):
    order_id = callback.data.split("_")[-1]
    
    await callback.message.edit_text(
        text=callback.message.text + "\n\n✅ <b>ЗАМОВЛЕННЯ ПРИЙНЯТЕ</b>",
        reply_markup=None
    )
    
    client_id = None
    for line in callback.message.text.split('\n'):
        if "ID:" in line:
            try:
                client_id = int(line.split('ID:')[1].strip().split(')')[0])
                break
            except (ValueError, IndexError):
                logger.error("Помилка парсингу ID клієнта")
    
    if client_id:
        try:
            await bot.send_message(
                chat_id=client_id,
                text=f"✅ Ваше замовлення #{order_id} прийнято в обробку!\n\n"
                     f"Очікуйте дзвінка від нашого менеджера для підтвердження деталей.",
                reply_markup=new_order_kb()
            )
        except Exception as e:
            logger.error(f"Не вдалося повідомити клієнта про прийняття замовлення: {e}")
    
    await callback.answer(f"Замовлення #{order_id} прийнято")

# ==================== АДМІН ПАНЕЛЬ ====================
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ У вас немає доступу до цієї команди")
        return
    await message.answer("👨‍💻 <b>Адмін панель</b>", reply_markup=admin_main_kb())

@dp.callback_query(F.data == "admin_status")
async def admin_status(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return
    
    storage = dp.storage
    active_sessions = 0
    
    if isinstance(storage, MemoryStorage):
        data_dict = getattr(storage, '_data', {})
        active_sessions = len(data_dict)
    
    status_text = (
        "📊 <b>Статус бота:</b>\n\n"
        f"🟢 Стан: {'Активний ▶️' if BOT_RUNNING else 'Призупинено ⏸️'}\n"
        f"👥 Користувачів у чорному списку: {len(BLACKLIST)}\n"
        f"📈 Активних сесій: {active_sessions}"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Оновити", callback_data="admin_status")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ])
    
    try:
        await callback.message.edit_text(
            text=status_text,
            reply_markup=keyboard
        )
    except TelegramBadRequest:
        await callback.answer("Статус не змінився")
    await callback.answer()

@dp.callback_query(F.data == "admin_blacklist")
async def admin_show_blacklist(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return
    
    try:
        if not BLACKLIST:
            text = "📋 <b>Чорний список порожній</b>\n\nВи можете додати користувачів вручну"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="👤 Додати користувача", callback_data="admin_add_to_blacklist")],
                [InlineKeyboardButton(text="🔄 Оновити", callback_data="admin_blacklist_refresh")],
                [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
            ])
            await callback.message.edit_text(text=text, reply_markup=keyboard)
        else:
            text = "📋 <b>Чорний список:</b>\n\n" + "\n".join(f"• {user_id}" for user_id in BLACKLIST)
            new_keyboard = admin_blacklist_kb(BLACKLIST)
            
            # Перевіряємо, чи змінився вміст або клавіатура
            current_text = callback.message.text or ""
            current_markup = callback.message.reply_markup
            
            if text != current_text or new_keyboard != current_markup:
                await callback.message.edit_text(
                    text=text,
                    reply_markup=new_keyboard
                )
            else:
                await callback.answer("Список не змінився")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await callback.answer("Список не змінився")
        else:
            logger.error(f"Помилка при оновленні чорного списку: {e}")
            await callback.answer("❌ Сталася помилка")
    except Exception as e:
        logger.error(f"Помилка при оновленні чорного списку: {e}")
        await callback.answer("❌ Сталася помилка")

@dp.callback_query(F.data == "admin_blacklist_refresh")
async def admin_refresh_blacklist(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return
    await admin_show_blacklist(callback)
    await callback.answer("🔄 Список оновлено")

@dp.callback_query(F.data == "admin_add_to_blacklist")
async def admin_add_blacklist(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return
    
    await callback.message.edit_text(
        "Введіть ID користувача, якого потрібно додати до чорного списку (тільки цифри):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("unblock_")
async def unblock_user(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return

    user_id_str = callback.data.split("_")[1]
    if not user_id_str.isdigit():
        await callback.answer("❌ Невірний ідентифікатор користувача")
        return

    user_id = int(user_id_str)
    
    if user_id in BLACKLIST:
        BLACKLIST.remove(user_id)
        await callback.answer(f"✅ Користувача {user_id} видалено з чорного списку")
    else:
        await callback.answer(f"❌ Користувача {user_id} немає у чорному списку")
        return
    
    # Оновлюємо повідомлення зі списком
    if not BLACKLIST:
        text = "📋 <b>Чорний список порожній</b>\n\nВи можете додати користувачів вручну"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Додати користувача", callback_data="admin_add_to_blacklist")],
            [InlineKeyboardButton(text="🔄 Оновити", callback_data="admin_blacklist_refresh")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
        ])
        await callback.message.edit_text(text=text, reply_markup=keyboard)
    else:
        text = "📋 <b>Чорний список:</b>\n\n" + "\n".join(f"• {user_id}" for user_id in BLACKLIST)
        await callback.message.edit_text(
            text=text,
            reply_markup=admin_blacklist_kb(BLACKLIST)
        )

@dp.callback_query(F.data == "admin_pause_bot"))
async def admin_pause(callback: types.CallbackQuery):
    global BOT_RUNNING
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return

    BOT_RUNNING = False
    await callback.message.edit_text("⏸️ Бот призупинено", reply_markup=admin_main_kb())
    await callback.answer("⏸️ Призупинено")

@dp.callback_query(F.data == "admin_start_bot")
async def admin_start(callback: types.CallbackQuery):
    global BOT_RUNNING
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return

    BOT_RUNNING = True
    await callback.message.edit_text("▶️ Бот запущено", reply_markup=admin_main_kb())
    await callback.answer("▶️ Запущено")

@dp.callback_query(F.data == "admin_stop_bot")
async def admin_stop(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return

    await callback.message.edit_text("⏹️ Бот зупиняється...")
    await callback.answer("⏹️ Зупинено")
    await on_shutdown(bot)
    await bot.session.close()
    os._exit(0)

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("⛔ У вас немає доступу")
        return
    await callback.message.edit_text("👨‍💻 <b>Адмін панель</b>", reply_markup=admin_main_kb())
    await callback.answer()

# ==================== ОБРОБКА ПОМИЛОК ====================
async def on_startup(bot: Bot):
    logger.info("Бот успішно запущений")
    await bot.send_message(chat_id=ADMIN_ID, text="🟢 Бот запущений")

async def on_shutdown(bot: Bot):
    logger.info("Бот зупиняється...")
    await bot.send_message(chat_id=ADMIN_ID, text="🔴 Бот зупиняється")
    await bot.session.close()

# ==================== ЗАПУСК БОТА ====================
async def on_startup(bot: Bot):
    logger.info("Бот успішно запущений")
    
    # Встановлюємо вебхук на Render.com
    if BASE_WEBHOOK_URL:
        webhook_url = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True
        )
        logger.info(f"Webhook установлено на {webhook_url}")
    
    await bot.send_message(chat_id=ADMIN_ID, text="🟢 Бот запущений")

async def on_shutdown(bot: Bot):
    logger.info("Бот зупиняється...")
    
    if BASE_WEBHOOK_URL:
        await bot.delete_webhook()
    
    await bot.send_message(chat_id=ADMIN_ID, text="🔴 Бот зупиняється")
    await bot.session.close()

async def handle_shutdown(signal, loop):
    logger.info("Отримано сигнал завершення...")
    await on_shutdown(bot)
    loop.stop()

async def main():
    # Додаємо middleware
    dp.message.middleware(ProtectionMiddleware())
    
    # Реєструємо обробники подій
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    
    # Налаштовуємо сервер для вебхуків
    if BASE_WEBHOOK_URL:
        app = web.Application()
        webhook_requests_handler = SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=WEBHOOK_SECRET,
        )
        webhook_requests_handler.register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)
        
        # Налаштовуємо обробку сигналів для коректного завершення
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(handle_shutdown(sig, loop))
        
        # Запускаємо сервер
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host=WEB_SERVER_HOST, port=WEB_SERVER_PORT)
        await site.start()
        
        logger.info(f"Сервер запущено на {WEB_SERVER_HOST}:{WEB_SERVER_PORT}")
        logger.info(f"Вебхук доступний за адресою: {BASE_WEBHOOK_URL}{WEBHOOK_PATH}")
        
        # Запускаємо бота у вічному циклі
        await asyncio.Event().wait()
    else:
        # Локальний режим з polling (для розробки)
        logger.info("Запуск в режимі polling...")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())  # або запуск твоєї стартової функції
