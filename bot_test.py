import os
import asyncio
import logging
import json
import uuid
import random
import time
import hashlib
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env
load_dotenv()

# ==================== БЕЗОПАСНОСТЬ ====================
BOT_TOKEN = os.getenv('BOT_TOKEN')
if not BOT_TOKEN:
    raise ValueError("❌ Токен не найден! Добавьте BOT_TOKEN в переменные окружения Bothost.")

# Главный администратор (владелец)
OWNER_ID = int(os.getenv('OWNER_ID', 439446887))
CHANNEL_ID = "@test_shop654"
DELIVERY_COST = 300
FREE_DELIVERY_THRESHOLD = 2000
PICKUP_ADDRESS = "Нижний Новгород ул. Профинтерна д.26"
CONTACT_PHONES = "+79506111165 Ирина и +79200783330 Сергей"

# База администраторов (загружается из файла)
admins_db = set()

# Режим покупателя для админов (кто в режиме покупателя)
buyer_mode_users = set()

# Защита от флуда
user_last_message_time = defaultdict(float)
user_message_count = defaultdict(int)
BLOCKED_USERS = set()

# Средние веса для мяса (в кг)
MEAT_AVERAGE_WEIGHTS = {
    "🐓 Цыпленок бройлер": 2.5,
    "🐔 Молодой петушок": 1,
    "👑 Цесарка": 1.4,
    "🐦 Перепелка": 0.2
}

# Средний вес для колбасы (1 палочка)
SAUSAGE_AVERAGE_WEIGHT = 0.4  # 400 грамм

# Благодарственные сообщения
THANK_YOU_MESSAGES = [
    "Спасибо за покупку в нашем хозяйстве! Надеемся, наши продукты принесут вам удовольствие и пользу! 🏡",
    "Благодарим за заказ! Желаем вам приятного аппетита и отличного настроения! 🌿",
    "Спасибо, что выбрали Русский ТАЙ! Ваш заказ очень важен для нас! 🐔🥚",
    "Большое спасибо за доверие! Надеемся на долгое и плодотворное сотрудничество! 🌾",
    "Благодарим за покупку! Ваша поддержка помогает нашему хозяйству развиваться! 🚜",
    "Спасибо за заказ! Рады, что вы оценили качество наших фермерских продуктов! 🥦",
    "Приятного аппетита! Спасибо, что поддерживаете локальных производителей! 🌱",
    "Благодарим за выбор наших натуральных продуктов! Ваше здоровье - наша забота! 💚",
    "Спасибо за покупку! Ждем вас снова в нашем хозяйстве! 🏠",
    "Ваш заказ выполнен! Спасибо, что цените качество и натуральность! 🐓",
    "Благодарим за доверие! Наши продукты выращены с любовью и заботой! ❤️",
    "Спасибо за заказ! Приятного аппетита и хорошего дня! 🌞",
    "Ваша поддержка очень важна для нас! Спасибо за покупку! 🙏",
    "Благодарим за выбор Русского ТАЯ! Надеемся, наши продукты вам понравятся! 🌳",
    "Спасибо за заказ! Мы ценим каждого клиента и стараемся для вас! 🌻",
    "Приятного аппетита! Спасибо, что выбираете натуральные продукты! 🍃",
    "Благодарим за покупку! Ваше здоровье начинается с качественных продуктов! 💪",
    "Спасибо за заказ! Рады быть вашим поставщиком фермерских продуктов! 🐖",
    "Ваш заказ успешно выдан! Спасибо за сотрудничество! 🤝",
    "Благодарим за доверие! Надеемся, вы оцените вкус наших натуральных продуктов! 👨‍🌾"
]

# ==================== ФУНКЦИИ БЕЗОПАСНОСТИ ====================
async def check_rate_limit(user_id: int) -> Tuple[bool, str]:
    now = time.time()
    if user_id in BLOCKED_USERS:
        return False, "🚫 Вы заблокированы за флуд. Обратитесь к администратору."
    last_time = user_last_message_time[user_id]
    time_diff = now - last_time
    if time_diff < 0.5:
        user_message_count[user_id] += 1
        if user_message_count[user_id] > 10:
            BLOCKED_USERS.add(user_id)
            asyncio.create_task(unblock_user_after_delay(user_id, 300))
            return False, "🚫 Вы заблокированы на 5 минут за чрезмерный флуд."
        return False, "⏳ Слишком часто! Пожалуйста, отправляйте сообщения медленнее."
    user_message_count[user_id] = 0
    user_last_message_time[user_id] = now
    return True, ""

async def unblock_user_after_delay(user_id: int, delay: int):
    await asyncio.sleep(delay)
    BLOCKED_USERS.discard(user_id)
    user_message_count[user_id] = 0

def validate_quantity(quantity_str: str, max_quantity: int = 1000) -> Tuple[bool, int, str]:
    try:
        quantity = int(quantity_str.strip())
        if quantity <= 0:
            return False, 0, "❌ Количество должно быть положительным числом!"
        if quantity > max_quantity:
            return False, 0, f"❌ Максимальное количество за раз: {max_quantity} шт."
        if quantity > 999999:
            return False, 0, "❌ Слишком большое количество!"
        return True, quantity, ""
    except ValueError:
        return False, 0, "❌ Введите число, а не текст!"

def validate_price(price_str: str, max_price: int = 100000) -> Tuple[bool, int, str]:
    try:
        price = int(price_str.strip())
        if price <= 0:
            return False, 0, "❌ Цена должна быть положительным числом!"
        if price > max_price:
            return False, 0, f"❌ Максимальная цена: {max_price} руб."
        return True, price, ""
    except ValueError:
        return False, 0, "❌ Введите число!"

def validate_address(address: str) -> Tuple[bool, str, str]:
    address = address.strip()
    if not address:
        return False, "", "❌ Адрес не может быть пустым!"
    if len(address) < 5:
        return False, "", "❌ Адрес слишком короткий (минимум 5 символов)"
    if len(address) > 500:
        return False, "", "❌ Адрес слишком длинный (максимум 500 символов)"
    dangerous_chars = ['<', '>', '&', ';', '|', '`', '$', '(', ')']
    for char in dangerous_chars:
        if char in address:
            return False, "", f"❌ Адрес содержит недопустимый символ: {char}"
    return True, address, ""

def sanitize_log_data(user_id: int) -> str:
    salt = "Russian_Tay_Farm_2026"
    hash_obj = hashlib.md5(f"{user_id}{salt}".encode())
    return f"user_{hash_obj.hexdigest()[:8]}"

def is_admin(user_id: int, check_buyer_mode: bool = True) -> bool:
    """Проверка, является ли пользователь администратором"""
    if check_buyer_mode and user_id in buyer_mode_users:
        return False
    return user_id == OWNER_ID or user_id in admins_db

def is_owner(user_id: int) -> bool:
    """Проверка, является ли пользователь владельцем"""
    return user_id == OWNER_ID

# ==================== СТРУКТУРА КАТЕГОРИЙ ====================
CATEGORIES = {
    "🥚 Яйцо": {
        "name": "🥚 Яйцо",
        "subcategories": ["🐔 Куриное", "🐦 Перепелиное", "👑 Цесариное"],
        "unit": "шт",
        "multiplier": {
            "🐔 Куриное": 10,  # 1 десяток
            "🐦 Перепелиное": 20,  # 2 десятка
            "👑 Цесариное": 10  # 1 десяток
        },
        "exact_price": True
    },
    "🍗 Мясо": {
        "name": "🍗 Мясо",
        "subcategories": {
            "❄️ Охлажденное": ["🐓 Цыпленок бройлер", "🐔 Молодой петушок", "👑 Цесарка", "🐦 Перепелка"],
            "🧊 Замороженное": ["🐓 Цыпленок бройлер", "🐔 Молодой петушок", "👑 Цесарка", "🐦 Перепелка"]
        },
        "unit": "кг",
        "price_per_kg": True,
        "average_weight": MEAT_AVERAGE_WEIGHTS,
        "exact_price": False,
        "freeze_delay_hours": 48  # 2 суток для переноса из охлажденного в замороженное
    },
    "🥫 Полуфабрикаты": {
        "name": "🥫 Полуфабрикаты",
        "subcategories": ["🌭 Колбаса", "🥩 Тушенка"],
        "unit": "шт",
        "price_per_kg": True,
        "average_weight": {
            "🌭 Колбаса": SAUSAGE_AVERAGE_WEIGHT,  # 400 гр за палочку
            "🥩 Тушенка": 0.5  # 500 гр за банку (стандарт)
        },
        "exact_price": False
    }
}

# ==================== ПОДКЛЮЧЕНИЕ К MONGODB ====================
# Отключено для локального тестирования
MONGODB_URI = ''
DB_NAME = 'telegram_bot'

db = None
mongo_client = None
products_db = {}
individual_products_db = {}
orders_db = {}
user_carts = {}
notifications_db = {}
product_views_db = {}
order_return_items_db = {}
manual_add_requests_db = {}
user_stats_db = {}
reviews_db = {}
admins_collection = None
buyer_mode_collection = None
admins_db = set()
buyer_mode_users = set()

def connect_to_mongodb():
    """Отключено для локального тестирования"""
    return False

# ==================== ФУНКЦИИ ДЛЯ MONGODB ====================
def save_data():
    """Сохранение данных (для MongoDB не нужно)"""
    pass

def get_product(product_id: str):
    """Получить товар по ID"""
    if products_db is None:
        return None
    if isinstance(products_db, dict):
        return products_db.get(product_id)
    return products_db.find_one({'id': product_id})

def get_all_products():
    """Получить все товары"""
    if products_db is None:
        return []
    if isinstance(products_db, dict):
        return list(products_db.values())
    return list(products_db.find())

def add_product(product_data: dict):
    """Добавить товар"""
    if products_db is None:
        return False
    if isinstance(products_db, dict):
        products_db[product_data['id']] = product_data
        return True
    try:
        products_db.insert_one(product_data)
        return True
    except:
        return False

def update_product(product_id: str, update_data: dict):
    """Обновить товар"""
    if products_db is None:
        return False
    if isinstance(products_db, dict):
        if product_id in products_db:
            products_db[product_id].update(update_data)
            return True
        return False
    try:
        products_db.update_one({'id': product_id}, {'$set': update_data})
        return True
    except:
        return False

def delete_product(product_id: str):
    """Удалить товар"""
    if products_db is None:
        return False
    if isinstance(products_db, dict):
        if product_id in products_db:
            del products_db[product_id]
            return True
        return False
    try:
        products_db.delete_one({'id': product_id})
        return True
    except:
        return False

def add_individual_product(product_data: dict):
    """Добавить индивидуальную тушку"""
    if individual_products_db is None:
        return False
    if isinstance(individual_products_db, dict):
        individual_products_db[product_data['id']] = product_data
        return True
    try:
        individual_products_db.insert_one(product_data)
        return True
    except:
        return False

def update_individual_product(product_id: str, update_data: dict):
    """Обновить индивидуальную тушку"""
    if individual_products_db is None:
        return False
    if isinstance(individual_products_db, dict):
        if product_id in individual_products_db:
            individual_products_db[product_id].update(update_data)
            return True
        return False
    try:
        individual_products_db.update_one({'id': product_id}, {'$set': update_data})
        return True
    except:
        return False

def add_order(order_data: dict):
    """Добавить заказ"""
    if orders_db is None:
        return False
    if isinstance(orders_db, dict):
        orders_db[order_data['id']] = order_data
        return True
    try:
        orders_db.insert_one(order_data)
        return True
    except:
        return False

def update_order(order_id: str, update_data: dict):
    """Обновить заказ"""
    if orders_db is None:
        return False
    if isinstance(orders_db, dict):
        if order_id in orders_db:
            orders_db[order_id].update(update_data)
            return True
        return False
    try:
        orders_db.update_one({'id': order_id}, {'$set': update_data})
        return True
    except:
        return False

def add_admin(user_id: int):
    """Добавить админа"""
    if admins_collection is None:
        admins_db.add(user_id)
        return True
    if isinstance(admins_collection, dict):
        admins_db.add(user_id)
        return True
    try:
        admins_collection.insert_one({'user_id': user_id})
        admins_db.add(user_id)
        return True
    except:
        return False

def remove_admin(user_id: int):
    """Удалить админа"""
    if admins_collection is None:
        admins_db.discard(user_id)
        return True
    if isinstance(admins_collection, dict):
        admins_db.discard(user_id)
        return True
    try:
        admins_collection.delete_one({'user_id': user_id})
        admins_db.discard(user_id)
        return True
    except:
        return False

def add_buyer_mode_user(user_id: int):
    """Добавить в режим покупателя"""
    if buyer_mode_collection is None:
        buyer_mode_users.add(user_id)
        return True
    if isinstance(buyer_mode_collection, dict):
        buyer_mode_users.add(user_id)
        return True
    try:
        buyer_mode_collection.insert_one({'user_id': user_id})
        buyer_mode_users.add(user_id)
        return True
    except:
        return False

def remove_buyer_mode_user(user_id: int):
    """Удалить из режима покупателя"""
    if buyer_mode_collection is None:
        buyer_mode_users.discard(user_id)
        return True
    if isinstance(buyer_mode_collection, dict):
        buyer_mode_users.discard(user_id)
        return True
    try:
        buyer_mode_collection.delete_one({'user_id': user_id})
        buyer_mode_users.discard(user_id)
        return True
    except:
        return False

def add_review(product_id: str, review_data: dict):
    """Добавить отзыв"""
    if reviews_db is None:
        return False
    if isinstance(reviews_db, dict):
        if product_id not in reviews_db:
            reviews_db[product_id] = []
        reviews_db[product_id].append(review_data)
        return True
    try:
        reviews_db.update_one(
            {'product_id': product_id},
            {'$push': {'reviews': review_data}},
            upsert=True
        )
        return True
    except:
        return False

def get_reviews(product_id: str):
    """Получить отзывы"""
    if reviews_db is None:
        return []
    if isinstance(reviews_db, dict):
        return reviews_db.get(product_id, [])
    review_doc = reviews_db.find_one({'product_id': product_id})
    return review_doc.get('reviews', []) if review_doc else []

# ==================== АВТОПЕРЕНОС ТОВАРОВ ИЗ ОХЛАЖДЕННОГО В ЗАМОРОЖЕННОЕ ====================
async def check_and_freeze_meat():
    """
    Проверка и перенос товаров из 'Охлажденное' в 'Замороженное' через 48 часов.
    Запускается периодически (каждые 30 минут).
    """
    now = datetime.now()
    meat_category = CATEGORIES.get("🍗 Мясо")
    if not meat_category:
        return

    freeze_delay = meat_category.get("freeze_delay_hours", 48)

    for product in get_all_products():
        product_id = product.get('id')
        if product.get('category') != "🍗 Мясо":
            continue
        if product.get('subcategory_type') != "❄️ Охлажденное":
            continue
        if product.get('quantity', 0) <= 0:
            continue

        # Проверяем, когда товар был добавлен в охлажденное
        created_at_str = product.get('created_at')
        if not created_at_str:
            continue

        try:
            created_at = datetime.strptime(created_at_str, "%d.%m.%Y %H:%M")
            hours_diff = (now - created_at).total_seconds() / 3600

            if hours_diff >= freeze_delay:
                # Переносим в замороженное
                update_product(product_id, {
                    'subcategory_type': "🧊 Замороженное",
                    'frozen_at': now.strftime("%d.%m.%Y %H:%M")
                })
                logging.info(f"Товар {product_id} ({product.get('subcategory')}) перенесен из охлажденного в замороженное")
        except Exception as e:
            logging.error(f"Ошибка при проверке заморозки товара {product_id}: {e}")

async def start_freeze_checker():
    """Запускает периодическую проверку заморозки"""
    while True:
        await asyncio.sleep(1800)  # Проверка каждые 30 минут
        await check_and_freeze_meat()

# ==================== НАСТРОЙКА БОТА ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
logging.basicConfig(level=logging.INFO)

# ==================== MIDDLEWARE ДЛЯ ЗАЩИТЫ ОТ ФЛУДА ====================
class AntiFloodMiddleware(BaseMiddleware):
    """Middleware для защиты от флуда - работает с ЛЮБЫМИ хендлерами"""
    def __init__(self):
        super().__init__()

    async def process_message(self, message: types.Message, data: dict):
        user_id = message.from_user.id
        if user_id == OWNER_ID:
            return
        allow, error_message = await check_rate_limit(user_id)
        if not allow:
            await message.answer(error_message)
            raise CancelHandler()

    async def process_callback_query(self, call: types.CallbackQuery, data: dict):
        user_id = call.from_user.id
        if user_id == OWNER_ID:
            return
        allow, error_message = await check_rate_limit(user_id)
        if not allow:
            await call.answer(error_message, show_alert=True)
            raise CancelHandler()

# Регистрируем middleware
dp.middleware.setup(AntiFloodMiddleware())

# ==================== СОСТОЯНИЯ ====================
class AddProduct(StatesGroup):
    category = State()
    subcategory = State()
    price = State()
    quantity = State()
    photo = State()

class EditProduct(StatesGroup):
    product_id = State()
    action = State()
    new_price = State()
    new_quantity = State()
    new_photo = State()

class CheckoutState(StatesGroup):
    delivery_method = State()
    address = State()
    name = State()
    phone = State()

class AdjustQuantityState(StatesGroup):
    product_id = State()
    quantity = State()

class AdjustStockState(StatesGroup):
    product_id = State()
    quantity = State()

class ManualAddToCartState(StatesGroup):
    product_id = State()
    quantity = State()

class PostponeOrderState(StatesGroup):
    order_id = State()
    new_date = State()

class AddAdminState(StatesGroup):
    action = State()
    user_id = State()

class RemoveAdminState(StatesGroup):
    user_id = State()

class ReviewState(StatesGroup):
    product_id = State()
    rating = State()
    text = State()

class UploadExcelState(StatesGroup):
    product_id = State()

class SelectWeightState(StatesGroup):
    product_id = State()
    weight_category = State()

# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard(is_admin=False, is_buyer_mode=False):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    buttons = [
        KeyboardButton("🛍️ Каталог"),
        KeyboardButton("🛒 Корзина"),
        KeyboardButton("📦 Мои заказы"),
        KeyboardButton("ℹ️ О нас")
    ]
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            keyboard.row(buttons[i], buttons[i + 1])
        else:
            keyboard.add(buttons[i])
    keyboard.add(KeyboardButton("🏠 В начало"))
    if is_buyer_mode:
        keyboard.add(KeyboardButton("👑 Выйти из режима покупателя"))
    elif is_admin:
        keyboard.add(KeyboardButton("👑 Панель админа"))
    return keyboard

def get_start_keyboard(is_admin=False):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    keyboard.add(KeyboardButton("🛍️ Начнем выбирать полезный продукт!"))
    if is_admin:
        keyboard.add(KeyboardButton("👑 Панель админа"))
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Добавить товар"),
        KeyboardButton("📦 Пополнить остатки"),
        KeyboardButton("📤 Опубликовать в канал")
    )
    keyboard.add(
        KeyboardButton("📊 Загрузить прайс (Excel)"),
        KeyboardButton("✏️ Управление товарами"),
        KeyboardButton("📊 Статистика"),
        KeyboardButton("📈 Аналитика"),
        KeyboardButton("📋 Активные заказы"),
        KeyboardButton("👥 Клиенты"),
        KeyboardButton("👥 Управление админами"),
        KeyboardButton("👤 Режим покупателя"),
        KeyboardButton("🏠 В начало")
    )
    return keyboard

def get_categories_keyboard(is_admin=False):
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for category_name in CATEGORIES.keys():
        count = count_products_in_category(category_name)
        keyboard.add(KeyboardButton(f"{category_name} ({count})"))
    if is_admin:
        keyboard.add(KeyboardButton("↩️ Назад"), KeyboardButton("👑 Панель админа"))
    else:
        keyboard.add(KeyboardButton("↩️ Назад"), KeyboardButton("🏠 В начало"))
    return keyboard

def count_products_in_category(category_name: str) -> int:
    """Подсчет товаров в категории (с учетом всех подкатегорий)"""
    count = 0
    category = CATEGORIES.get(category_name)
    if not category:
        return 0
    
    subcategories = category.get("subcategories", [])
    
    if isinstance(subcategories, dict):
        # Для мяса: суммируем по всем подкатегориям (охлажденное + замороженное)
        for subcat_name in subcategories.keys():
            count += count_products_in_subcategory(category_name, subcat_name)
    else:
        # Для яиц и полуфабрикатов
        for subcat in subcategories:
            count += count_products_in_subcategory(category_name, subcat)
    
    return count

def count_products_in_subcategory(category_name: str, subcategory_name: str) -> int:
    """Подсчет товаров в подкатегории (с учетом наличия)"""
    count = 0
    category = CATEGORIES.get(category_name)
    if not category:
        return 0
    
    subcategories = category.get("subcategories", [])
    
    # Для мяса с вложенной структурой
    if isinstance(subcategories, dict):
        rubrics = subcategories.get(subcategory_name, [])
        for product in products_db.values():
            if (product.get('category') == category_name and 
                product.get('subcategory_type') == subcategory_name and
                product.get('quantity', 0) > 0):
                count += 1
    else:
        # Для яиц и полуфабрикатов
        for product in products_db.values():
            if (product.get('category') == category_name and 
                product.get('subcategory') == subcategory_name and
                product.get('quantity', 0) > 0):
                count += 1
    
    return count

def count_products_in_rubric(category_name: str, subcategory_name: str, rubric_name: str) -> int:
    """Подсчет товаров в конкретной рубрике внутри подкатегории"""
    count = 0
    for product in products_db.values():
        if (product.get('category') == category_name and 
            product.get('subcategory_type') == subcategory_name and
            product.get('subcategory') == rubric_name and
            product.get('quantity', 0) > 0):
            count += 1
    return count

def get_subcategories_keyboard(category_name: str, is_admin=False):
    category = CATEGORIES.get(category_name)
    if not category:
        return get_categories_keyboard(is_admin)
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    
    subcategories = category.get("subcategories", [])
    
    # Проверяем, является ли subcategories словарем (для мяса с охлажденное/замороженное)
    if isinstance(subcategories, dict):
        # Для мяса показываем подкатегории "Охлажденное" и "Замороженное" со счетчиками
        for subcat_name, rubrics in subcategories.items():
            count = count_products_in_subcategory(category_name, subcat_name)
            keyboard.add(KeyboardButton(f"{subcat_name} ({count})"))
    else:
        # Для яиц и полуфабрикатов показываем обычные подкатегории со счетчиками
        for subcat in subcategories:
            count = count_products_in_subcategory(category_name, subcat)
            keyboard.add(KeyboardButton(f"{subcat} ({count})"))
    
    if is_admin:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("👑 Панель админа"))
    else:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("🏠 В начало"))
    return keyboard

def get_product_keyboard(product_id: str, product_data: dict, is_admin: bool = False):
    keyboard = InlineKeyboardMarkup(row_width=1)
    if is_admin:
        keyboard.add(
            InlineKeyboardButton("📦 Изменить остаток", callback_data=f"adjust_{product_id}"),
            InlineKeyboardButton("✏️ Редактировать товар", callback_data=f"edit_{product_id}"),
            InlineKeyboardButton("🗑️ Удалить товар", callback_data=f"delete_{product_id}"),
            InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
        )
    else:
        if product_data.get('quantity', 0) > 0:
            keyboard.add(
                InlineKeyboardButton("🛒 В корзину", callback_data=f"add_{product_id}"),
                InlineKeyboardButton("📝 Добавить вручную", callback_data=f"manual_add_{product_id}")
            )
            # Кнопки отзывов для товаров с выбором веса (цыпленок бройлер)
            if product_data.get('subcategory') == "🐓 Цыпленок бройлер":
                keyboard.add(
                    InlineKeyboardButton("📋 Выбрать по весу", callback_data=f"select_weight_{product_id}")
                )
        else:
            keyboard.add(
                InlineKeyboardButton("🔔 Уведомить о появлении", callback_data=f"notify_{product_id}")
            )
        # Кнопки отзывов для всех товаров
        keyboard.add(
            InlineKeyboardButton("✍️ Оставить отзыв", callback_data=f"write_review_{product_id}"),
            InlineKeyboardButton("📖 Читать отзывы", callback_data=f"read_reviews_{product_id}")
        )
    return keyboard

def get_cart_keyboard(cart_items):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Оформить заказ", callback_data="checkout"),
        InlineKeyboardButton("🗑️ Очистить корзину", callback_data="clear_cart")
    )
    for item in cart_items:
        product_id = item['id']
        product_name = item['name'][:20] + "..." if len(item['name']) > 20 else item['name']
        keyboard.add(
            InlineKeyboardButton(f"➕ {product_name}", callback_data=f"inc_{product_id}"),
            InlineKeyboardButton(f"➖ {product_name}", callback_data=f"dec_{product_id}"),
            InlineKeyboardButton(f"📝 Изменить количество", callback_data=f"change_{product_id}")
        )
    keyboard.add(
        InlineKeyboardButton("🛍️ К категориям", callback_data="view_categories"),
        InlineKeyboardButton("🏠 В начало", callback_data="go_home")
    )
    return keyboard

def get_delivery_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚗 Самовывоз", callback_data="pickup"),
        InlineKeyboardButton("🚚 Доставка", callback_data="delivery")
    )
    return keyboard

def get_products_for_adjust_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        if key not in products_by_category:
            products_by_category[key] = product
    for product in products_by_category.values():
        product_name = product.get('subcategory', '')
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        current_quantity = product.get('quantity', 0)
        keyboard.add(InlineKeyboardButton(
            f"{product_name} ({current_quantity} {category_info.get('unit', 'шт')})",
            callback_data=f"adjust_{product['id']}"
        ))
    if not products_by_category:
        keyboard.add(InlineKeyboardButton("📭 Нет товаров", callback_data="no_products"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    return keyboard

def get_product_management_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=1)
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        if key not in products_by_category:
            products_by_category[key] = product
    for product in products_by_category.values():
        product_name = product.get('subcategory', '')
        keyboard.add(InlineKeyboardButton(
            f"✏️ {product_name} ({product.get('price', 0)} руб.)",
            callback_data=f"edit_{product['id']}"
        ))
    if not products_by_category:
        keyboard.add(InlineKeyboardButton("📭 Нет товаров для редактирования", callback_data="no_products"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    return keyboard

def get_edit_product_keyboard(product_id: str):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("💰 Изменить цену", callback_data=f"edit_price_{product_id}"),
        InlineKeyboardButton("📦 Изменить остаток", callback_data=f"edit_quantity_{product_id}")
    )
    keyboard.add(
        InlineKeyboardButton("📸 Изменить фото", callback_data=f"edit_photo_{product_id}"),
        InlineKeyboardButton("🗑️ Удалить товар", callback_data=f"delete_confirm_{product_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🔙 Назад к списку", callback_data="back_to_product_management"),
        InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
    )
    return keyboard

def get_active_orders_keyboard():
    keyboard = InlineKeyboardMarkup(row_width=2)
    active = [o for o in orders_db.values() if o.get('status') in ['🆕 Новый', '✅ Подтвержден']]
    if not active:
        keyboard.add(InlineKeyboardButton("📭 Нет активных заказов", callback_data="no_active_orders"))
        keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
        return keyboard
    new_orders = sorted([o for o in active if o.get('status') == '🆕 Новый'],
                        key=lambda x: x.get('created_at', ''), reverse=True)
    confirmed_orders = sorted([o for o in active if o.get('status') == '✅ Подтвержден'],
                              key=lambda x: x.get('created_at', ''), reverse=True)
    all_orders = new_orders + confirmed_orders
    for order in all_orders[:10]:
        order_id = order['id']
        status = order.get('status', '')
        total = order.get('total', 0)
        if status == '🆕 Новый':
            status_icon = "🆕"
        elif status == '✅ Подтвержден':
            status_icon = "✅"
        else:
            status_icon = "📦"
        keyboard.add(InlineKeyboardButton(
            f"{status_icon} Заказ #{order_id} - {total} руб.",
            callback_data=f"manage_order_{order_id}"
        ))
    keyboard.add(InlineKeyboardButton("🔄 Обновить список", callback_data="refresh_active_orders"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    return keyboard

def get_order_confirmation_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить заказ", callback_data=f"confirm_{order_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{order_id}")
    )
    keyboard.add(InlineKeyboardButton("📞 Связаться", callback_data=f"contact_{order_id}"))
    return keyboard

def get_client_orders_keyboard(user_id: str):
    keyboard = InlineKeyboardMarkup(row_width=1)
    client_orders = [order for order in orders_db.values()
                     if order.get('user_id') == user_id]
    client_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    for order in client_orders[:5]:
        status_icon = ""
        if order.get('status') == '✅ Выполнен':
            status_icon = "✅"
        elif order.get('status') == '❌ Отменен':
            status_icon = "❌"
        elif order.get('status') == '⏰ Перенесен':
            status_icon = "⏰"
        else:
            status_icon = "🆕"
        keyboard.add(InlineKeyboardButton(
            f"{status_icon} Заказ #{order['id']} - {order.get('total', 0)} руб.",
            callback_data=f"view_order_{order['id']}"
        ))
    return keyboard

def get_admin_management_keyboard():
    """Клавиатура для управления администраторами"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("➕ Добавить админа", callback_data="add_admin"),
        InlineKeyboardButton("➖ Удалить админа", callback_data="remove_admin")
    )
    keyboard.add(InlineKeyboardButton("📋 Список админов", callback_data="list_admins"))
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="admin_panel"))
    return keyboard

def get_admins_list_keyboard():
    """Клавиатура со списком админов для удаления"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    for admin_id in admins_db:
        keyboard.add(InlineKeyboardButton(
            f"❌ Удалить админа {admin_id}",
            callback_data=f"remove_admin_{admin_id}"
        ))
    keyboard.add(InlineKeyboardButton("🔙 Назад", callback_data="admin_management"))
    return keyboard

def get_review_keyboard(product_id: str):
    """Клавиатура для отзывов о товаре"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✍️ Оставить отзыв", callback_data=f"write_review_{product_id}"),
        InlineKeyboardButton("📖 Читать отзывы", callback_data=f"read_reviews_{product_id}")
    )
    return keyboard

def get_rating_keyboard(product_id: str):
    """Клавиатура для выбора оценки"""
    keyboard = InlineKeyboardMarkup(row_width=3)
    keyboard.row(
        InlineKeyboardButton("⭐️ 1", callback_data=f"rating_1_{product_id}"),
        InlineKeyboardButton("⭐️ 2", callback_data=f"rating_2_{product_id}"),
        InlineKeyboardButton("⭐️ 3", callback_data=f"rating_3_{product_id}")
    )
    keyboard.row(
        InlineKeyboardButton("⭐️ 4", callback_data=f"rating_4_{product_id}"),
        InlineKeyboardButton("⭐️ 5", callback_data=f"rating_5_{product_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_review"))
    return keyboard

def get_weight_category_keyboard(product_id: str):
    """Клавиатура для выбора весовой категории цыпленка"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("🐔 2.0 - 2.5 кг", callback_data=f"weight_2.0_2.5_{product_id}"),
        InlineKeyboardButton("🐔 2.6 - 3.0 кг", callback_data=f"weight_2.6_3.0_{product_id}"),
        InlineKeyboardButton("🐔 3.0+ кг", callback_data=f"weight_3.0+_{product_id}")
    )
    keyboard.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_weight"))
    return keyboard

def get_back_to_reviews_keyboard(product_id: str):
    """Клавиатура возврата к отзывам"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(InlineKeyboardButton("🔙 Назад к отзывам", callback_data=f"read_reviews_{product_id}"))
    return keyboard

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def get_category_info(category_name: str, subcategory_name: str) -> dict:
    category = CATEGORIES.get(category_name)
    if not category:
        return {}
    
    subcategories = category.get("subcategories", [])
    
    # Для мяса с вложенными подкатегориями
    if isinstance(subcategories, dict):
        # Ищем рубрику во всех типах хранения
        average_weight = category.get('average_weight', {})
        info = {
            'unit': category.get('unit', 'шт'),
            'multiplier': 1,
            'price_per_kg': category.get('price_per_kg', False),
            'average_weight': average_weight.get(subcategory_name, 0),
            'exact_price': category.get('exact_price', False)
        }
    else:
        # Для яиц и полуфабрикатов
        info = {
            'unit': category.get('unit', 'шт'),
            'multiplier': category.get('multiplier', {}).get(subcategory_name, 1),
            'price_per_kg': category.get('price_per_kg', False),
            'average_weight': category.get('average_weight', {}).get(subcategory_name, 0),
            'exact_price': category.get('exact_price', False)
        }
    
    return info

def calculate_product_price(product_data: dict, quantity: int = 1) -> Tuple[int, str]:
    price = product_data.get('price', 0)
    category = CATEGORIES.get(product_data.get('category', ''))
    subcategory = product_data.get('subcategory', '')
    if not category:
        return 0, ""
    if category.get('unit') == 'шт' and not category.get('price_per_kg'):
        multiplier = category.get('multiplier', {}).get(subcategory, 1)
        total_price = price * multiplier * quantity
        return total_price, f"({multiplier} шт. × {price} руб./шт.)"
    elif category.get('price_per_kg') and category.get('average_weight'):
        avg_weight = category.get('average_weight', {}).get(subcategory, 0)
        if avg_weight > 0:
            estimated_price = price * avg_weight * quantity
            note = f"*≈{estimated_price:.0f} руб. за {quantity} шт. (средний вес {avg_weight} кг)"
            return estimated_price, note
    elif category.get('price_per_kg'):
        return 0, "*Итоговая цена будет рассчитана при получении"
    return 0, ""

def format_product_info(product_data: dict) -> str:
    info = f"{product_data.get('subcategory', '')}\n"
    category_info = get_category_info(product_data.get('category', ''), product_data.get('subcategory', ''))
    if category_info.get('price_per_kg') and category_info.get('average_weight', 0) > 0:
        avg_weight = category_info.get('average_weight', 0)
        price_per_kg = product_data.get('price', 0)
        estimated_price = price_per_kg * avg_weight
        info += f"💰 Цена: {price_per_kg} руб./кг\n"
        info += f"📦 Средний вес тушки: ~{avg_weight} кг\n"
        info += f"💰 Примерная цена за тушку*: ~{estimated_price:.0f} руб.\n\n"
        info += f"*Расчетная цена. Итоговая стоимость зависит от фактического веса тушки.\n\n"
    elif category_info.get('price_per_kg'):
        info += f"💰 Цена: {product_data.get('price', 0)} руб./кг\n"
        info += f"*Итоговая стоимость будет рассчитана при получении заказа\n\n"
    else:
        multiplier = category_info.get('multiplier', 1)
        price_per_unit = product_data.get('price', 0)
        total_price = price_per_unit * multiplier
        info += f"💰 Цена за упаковку ({multiplier} шт): {total_price} руб.\n"
        info += f"💰 Цена за 1 шт: {price_per_unit} руб.\n\n"
    info += f"📦 Остаток: {product_data.get('quantity', 0)} "
    info += f"{category_info.get('unit', 'шт')}\n"
    if product_data.get('created_at'):
        info += f"\n📅 Добавлено: {product_data.get('created_at')}"
    return info

def format_order_info(order_data: dict) -> str:
    order = order_data
    text = f"Заказ #{order['id']}\n"
    text += f"📅 Создан: {order.get('created_at', 'Не указана')}\n"
    text += f"📦 Статус: {order.get('status', 'Не указан')}\n"
    if order.get('status_updated_at'):
        text += f"🕒 Статус обновлен: {order.get('status_updated_at')}\n"
    if order.get('delivery_method') == 'pickup':
        text += f"🚗 Способ: Самовывоз\n"
        text += f"📍 Адрес: {PICKUP_ADDRESS}\n"
    else:
        text += f"🚚 Способ: Доставка\n"
        text += f"📍 Адрес: {order.get('address', 'Не указан')}\n"
    text += f"👤 Покупатель: @{order.get('username', 'без username')}\n"
    text += f"🆔 ID пользователя: {sanitize_log_data(int(order.get('user_id', 0)))}\n\n"
    text += "Состав заказа:\n"
    for item in order.get('items', []):
        if item.get('price_per_kg'):
            if item.get('average_weight', 0) > 0:
                item_price = item['price'] * item['average_weight'] * item['quantity']
                text += f"• {item['name']} - {item['quantity']} шт.\n"
                text += f"  Примерная стоимость: ~{item_price:.0f} руб.\n"
            else:
                text += f"• {item['name']} - {item['quantity']} кг\n"
        else:
            product_item = products_db.get(item['id'])
            if product_item:
                category_info = get_category_info(product_item.get('category', ''), product_item.get('subcategory', ''))
                multiplier = category_info.get('multiplier', 1)
                item_price = item['price'] * multiplier * item['quantity']
                text += f"• {item['name']} - {item['quantity']} уп.\n"
                text += f"  Сумма: {item_price} руб.\n"
    text += f"\n💰 Итого: {order.get('total', 0)} руб.\n"
    return text

def format_client_stats(user_id: str) -> str:
    if user_id not in user_stats_db:
        return "📭 Статистика по клиенту не найдена."
    stats = user_stats_db[user_id]
    text = f"📊 Статистика клиента\n\n"
    text += f"👤 Username: @{stats.get('username', 'не указан')}\n"
    text += f"🆔 ID: {sanitize_log_data(int(user_id))}\n\n"
    text += f"📈 Общая статистика:\n"
    text += f"• Всего заказов: {stats.get('total_orders', 0)}\n"
    text += f"• Выполнено: {stats.get('completed_orders', 0)}\n"
    text += f"• Отменено: {stats.get('canceled_orders', 0)}\n"
    text += f"• Перенесено: {stats.get('postponed_orders', 0)}\n"
    text += f"• Всего потрачено: {stats.get('total_spent', 0)} руб.\n\n"
    if stats.get('first_order_date'):
        text += f"📅 Первый заказ: {stats.get('first_order_date')}\n"
    if stats.get('last_order_date'):
        text += f"📅 Последний заказ: {stats.get('last_order_date')}\n"
    if stats.get('last_status_change'):
        text += f"🕒 Последнее изменение статуса: {stats.get('last_status_change')}\n"
    return text

def get_random_thank_you_message() -> str:
    return random.choice(THANK_YOU_MESSAGES)

# ==================== ОСНОВНЫЕ КОМАНДЫ ====================
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    user_is_admin = is_admin(message.from_user.id)
    if user_is_admin:
        await message.answer(
            "👑 Панель администратора\n\n"
            "Вы в режиме администратора. Используйте кнопки ниже для управления хозяйством.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer(
            "🏡 Добро пожаловать в Русский ТАЙ!\n\n"
            "Семейная ферма в экологически чистом месте Керженского заповедника.\n\n"
            "Наши продукты - это забота о вашем здоровье и качестве жизни!",
            parse_mode="HTML",
            reply_markup=get_start_keyboard(is_admin=user_is_admin)
        )

@dp.message_handler(text="🛍️ Начнем выбирать полезный продукт!")
async def start_shopping(message: types.Message):
    user_is_admin = is_admin(message.from_user.id)
    is_buyer = message.from_user.id in buyer_mode_users
    await message.answer(
        "🛍️ Отлично! Давайте выберем самые полезные и свежие продукты!\n\n"
        "Выберите действие:",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(is_admin=user_is_admin, is_buyer_mode=is_buyer)
    )

@dp.message_handler(text="🏠 В начало")
async def go_to_home(message: types.Message):
    await cmd_start(message)

@dp.message_handler(text="👑 Панель админа")
async def switch_to_admin_mode(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("❌ У вас нет прав администратора.")
        return
    await message.answer(
        "👑 Переключение в режим администратора\n\n"
        "Теперь вы можете управлять хозяйством.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )

@dp.message_handler(text="👤 Режим покупателя")
async def switch_to_user_mode(message: types.Message):
    if not is_admin(message.from_user.id, check_buyer_mode=False):
        return
    buyer_mode_users.add(message.from_user.id)
    save_data()
    await message.answer(
        "👤 Переключение в режим покупателя\n\n"
        "Теперь вы видите интерфейс как обычный покупатель.",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(is_buyer_mode=True)
    )

@dp.message_handler(text="👑 Выйти из режима покупателя")
async def exit_buyer_mode(message: types.Message):
    user_id = message.from_user.id
    if user_id in buyer_mode_users:
        buyer_mode_users.discard(user_id)
        save_data()
    await message.answer(
        "👑 Вы вышли из режима покупателя.\n\nТеперь вы снова видите панель администратора.",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(is_admin=True)
    )

@dp.message_handler(text="🛍️ Каталог")
async def show_catalog(message: types.Message):
    user_is_admin = is_admin(message.from_user.id)
    await message.answer(
        "📂 Выберите категорию:",
        parse_mode="HTML",
        reply_markup=get_categories_keyboard(is_admin=user_is_admin)
    )

@dp.message_handler(text="↩️ К категориям")
async def back_to_categories(message: types.Message):
    """Возврат к списку категорий из подкатегорий"""
    user_is_admin = is_admin(message.from_user.id)
    await message.answer(
        "📂 Выберите категорию:",
        parse_mode="HTML",
        reply_markup=get_categories_keyboard(is_admin=user_is_admin)
    )

@dp.message_handler(text="↩️ Назад")
async def go_back(message: types.Message):
    """Возврат из категорий в каталог"""
    user_is_admin = is_admin(message.from_user.id)
    await message.answer(
        "📂 Каталог:",
        parse_mode="HTML",
        reply_markup=get_categories_keyboard(is_admin=user_is_admin)
    )

@dp.message_handler(lambda m: m.text in CATEGORIES.keys() or any(m.text.startswith(cat) for cat in CATEGORIES.keys()))
async def show_category(message: types.Message):
    # Извлекаем имя категории (убираем счетчик в скобках)
    category_text = message.text.split(' (')[0]
    category = CATEGORIES.get(category_text)
    if not category:
        return
    user_is_admin = is_admin(message.from_user.id)
    
    subcategories = category.get("subcategories", [])
    
    # Если это мясо с вложенными подкатегориями (охлажденное/замороженное)
    if isinstance(subcategories, dict):
        await message.answer(
            f"📂 {category_text}\n\nВыберите тип хранения:",
            parse_mode="HTML",
            reply_markup=get_subcategories_keyboard(category_text, is_admin=user_is_admin)
        )
    else:
        # Для яиц и полуфабрикатов сразу показываем рубрики
        await message.answer(
            f"📂 {category_text}\n\nВыберите рубрику:",
            parse_mode="HTML",
            reply_markup=get_subcategories_keyboard(category_text, is_admin=user_is_admin)
        )

def get_rubrics_keyboard(category_name: str, subcategory_type: str, is_admin=False):
    """Клавиатура с рубриками внутри подкатегории (для мяса)"""
    category = CATEGORIES.get(category_name)
    if not category:
        return get_subcategories_keyboard(category_name, is_admin)
    
    subcategories = category.get("subcategories", {})
    rubrics = subcategories.get(subcategory_type, [])
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for rubric in rubrics:
        count = count_products_in_rubric(category_name, subcategory_type, rubric)
        keyboard.add(KeyboardButton(f"{rubric} ({count})"))
    
    if is_admin:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("👑 Панель админа"))
    else:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("🏠 В начало"))
    return keyboard

@dp.message_handler(lambda m: m.text in ["❄️ Охлажденное", "🧊 Замороженное"] or m.text.startswith("❄️ Охлажденное") or m.text.startswith("🧊 Замороженное"))
async def show_meat_subcategory(message: types.Message):
    """Обработчик для подкатегорий мяса (охлажденное/замороженное)"""
    # Извлекаем имя подкатегории (убираем счетчик)
    subcategory_type = message.text.split(' (')[0]
    
    user_is_admin = is_admin(message.from_user.id)
    
    await message.answer(
        f"🥩 {subcategory_type}\n\nВыберите рубрику:",
        parse_mode="HTML",
        reply_markup=get_rubrics_keyboard("🍗 Мясо", subcategory_type, is_admin=user_is_admin)
    )

@dp.message_handler(lambda m: any(rubric in m.text for cat in CATEGORIES.values() 
                                   for sub in (cat.get("subcategories", []) if not isinstance(cat.get("subcategories"), dict) else [])
                                   for rubric in (sub if isinstance(sub, list) else [])) or 
                          any(rubric in m.text for rubric in ["🐓 Цыпленок бройлер", "🐔 Молодой петушок", "👑 Цесарка", "🐦 Перепелка", "🐔 Куриное", "🐦 Перепелиное", "👑 Цесариное", "🌭 Колбаса", "🥩 Тушенка"]))
async def show_products(message: types.Message):
    try:
        # Извлекаем имя рубрики (убираем счетчик в скобках)
        rubric_text = message.text.split(' (')[0]
        
        product = None
        for prod in products_db.values():
            if prod.get('subcategory') == rubric_text:
                product = prod
                break
        
        if not product:
            user_is_admin = is_admin(message.from_user.id)
            if user_is_admin:
                await message.answer(
                    f"📭 В рубрике '{rubric_text}' пока нет товаров.\n\n"
                    f"Хотите добавить товар? Используйте кнопку '➕ Добавить товар' в панели админа.",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await message.answer(f"📭 В рубрике '{rubric_text}' пока нет товаров.")
            return
        
        caption = format_product_info(product)
        user_is_admin = is_admin(message.from_user.id)
        if user_is_admin:
            caption = f"👑 РЕЖИМ АДМИНИСТРАТОРА\n\n{caption}"
        if product.get('photo'):
            await message.answer_photo(
                product['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=get_product_keyboard(product['id'], product, is_admin=user_is_admin)
            )
        else:
            await message.answer(
                caption,
                parse_mode="HTML",
                reply_markup=get_product_keyboard(product['id'], product, is_admin=user_is_admin)
            )
        if not user_is_admin:
            increment_product_view(product['id'])
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

# ==================== КОРЗИНА ====================
@dp.message_handler(text="🛒 Корзина")
async def show_cart(message: types.Message):
    user_id = str(message.from_user.id)
    cart = user_carts.get(user_id, [])
    if not cart:
        user_is_admin = is_admin(message.from_user.id)
        is_buyer = message.from_user.id in buyer_mode_users
        await message.answer("🛒 Ваша корзина пуста.",
                             reply_markup=get_main_keyboard(is_admin=user_is_admin, is_buyer_mode=is_buyer))
        return
    total = 0
    text = "🛒 Ваша корзина:\n\n"
    has_inexact_price = False
    has_exact_price_only = True
    for item in cart:
        product = products_db.get(item['id'])
        if product:
            category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
            if not category_info.get('exact_price', True):
                has_inexact_price = True
                has_exact_price_only = False
            if category_info.get('price_per_kg'):
                if category_info.get('average_weight', 0) > 0:
                    estimated_price, note = calculate_product_price(product, item['quantity'])
                    text += f"• {item['name']} - {item['quantity']} шт.\n"
                    text += f"  Примерная стоимость*: ~{estimated_price:.0f} руб.\n"
                    total += estimated_price
                else:
                    text += f"• {item['name']} - {item['quantity']} кг\n"
                    text += f"   Цена будет известна при получении\n"
                    has_exact_price_only = False
            else:
                item_price, note = calculate_product_price(product, item['quantity'])
                text += f"• {item['name']} - {item['quantity']} упак.\n"
                text += f"  Цена: {item_price} руб. {note}\n"
                total += item_price
    if has_inexact_price:
        text += f"\n*Расчетная стоимость. Итоговая цена зависит от фактического веса.\n"
    if has_exact_price_only:
        text += f"\n💰 Итого к оплате: {total:.0f} руб."
    else:
        text += f"\n💰 Примерная сумма: ~{total:.0f} руб.\n"
        text += f"Итоговая стоимость будет рассчитана при получении"
    await message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))

@dp.callback_query_handler(lambda c: c.data.startswith('add_') and c.data != 'add_admin')
async def add_to_cart(call: types.CallbackQuery):
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    if is_admin(call.from_user.id):
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    if product.get('quantity', 0) <= 0:
        await call.answer("❌ Товара нет в наличии", show_alert=True)
        return
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        user_carts[user_id] = []
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    if item_index >= 0:
        user_carts[user_id][item_index]['quantity'] += 1
        current_quantity = user_carts[user_id][item_index]['quantity']
    else:
        user_carts[user_id].append({
            'id': product_id,
            'name': product.get('subcategory', ''),
            'quantity': 1
        })
        current_quantity = 1
    save_data()
    await call.answer(f"✅ {product.get('subcategory', 'Товар')} добавлен в корзину! 📦 В корзине: {current_quantity} шт.", show_alert=False)
    new_keyboard = get_product_keyboard(product_id, product, is_admin=False)
    try:
        if call.message.photo:
            await call.message.edit_caption(
                call.message.caption,
                parse_mode="HTML",
                reply_markup=new_keyboard
            )
        else:
            await call.message.edit_text(
                call.message.text,
                parse_mode="HTML",
                reply_markup=new_keyboard
            )
    except Exception as e:
        print(f"Ошибка при обновлении клавиатуры: {e}")

@dp.callback_query_handler(lambda c: c.data.startswith('manual_add_'))
async def manual_add_to_cart_start(call: types.CallbackQuery, state: FSMContext):
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    if is_admin(call.from_user.id):
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    if product.get('quantity', 0) <= 0:
        await call.answer("❌ Товара нет в наличии", show_alert=True)
        return
    increment_manual_add_request(product_id)
    user_id = str(call.from_user.id)
    current_quantity = 0
    for item in user_carts.get(user_id, []):
        if item['id'] == product_id:
            current_quantity = item['quantity']
            break
    await state.update_data(product_id=product_id)
    await ManualAddToCartState.quantity.set()
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    await call.message.answer(
        f"📝 Ручное добавление в корзину\n\n"
        f"{product.get('subcategory', '')}\n"
        f"📦 Доступно: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        f"📦 Сейчас в корзине: {current_quantity}\n\n"
        f"Введите количество для добавления в корзину:",
        parse_mode="HTML"
    )

@dp.message_handler(state=ManualAddToCartState.quantity)
async def process_manual_add_quantity(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        if not product:
            await message.answer("❌ Товар не найден")
            await state.finish()
            return
        is_valid, quantity, error_msg = validate_quantity(message.text, product.get('quantity', 0))
        if not is_valid:
            await message.answer(error_msg)
            await state.finish()
            return
        user_id = str(message.from_user.id)
        if user_id not in user_carts:
            user_carts[user_id] = []
        item_index = -1
        for i, item in enumerate(user_carts[user_id]):
            if item['id'] == product_id:
                item_index = i
                break
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        if item_index >= 0:
            new_total = user_carts[user_id][item_index]['quantity'] + quantity
            if new_total > product.get('quantity', 0):
                await message.answer(f"❌ Недостаточно товара! Доступно: {product.get('quantity', 0)}")
                await state.finish()
                return
            user_carts[user_id][item_index]['quantity'] = new_total
            current_quantity = new_total
        else:
            user_carts[user_id].append({
                'id': product_id,
                'name': product.get('subcategory', ''),
                'quantity': quantity
            })
            current_quantity = quantity
        save_data()
        await message.answer(
            f"✅ Товар добавлен в корзину!\n\n"
            f"{product.get('subcategory', '')}\n"
            f"📦 Добавлено: {quantity} {category_info.get('unit', 'шт')}\n"
            f"📦 Всего в корзине: {current_quantity}",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=is_admin(message.from_user.id), is_buyer_mode=message.from_user.id in buyer_mode_users)
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('inc_'))
async def increase_quantity(call: types.CallbackQuery):
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    if item_index >= 0:
        if user_carts[user_id][item_index]['quantity'] < product.get('quantity', 0):
            user_carts[user_id][item_index]['quantity'] += 1
            save_data()
            await call.answer(f"➕ {product.get('subcategory', 'Товар')}\n📦 Теперь: {user_carts[user_id][item_index]['quantity']} шт.", show_alert=False)
            await update_cart_message(call, user_id)
        else:
            await call.answer("❌ Недостаточно товара на складе", show_alert=True)
    else:
        await call.answer("❌ Товар не найден в корзине", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('dec_'))
async def decrease_quantity(call: types.CallbackQuery):
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    if item_index >= 0:
        if user_carts[user_id][item_index]['quantity'] > 1:
            user_carts[user_id][item_index]['quantity'] -= 1
            save_data()
            await call.answer(f"➖ {product.get('subcategory', 'Товар')}\n📦 Теперь: {user_carts[user_id][item_index]['quantity']} шт.", show_alert=False)
            await update_cart_message(call, user_id)
        else:
            product_name = user_carts[user_id][item_index]['name']
            del user_carts[user_id][item_index]
            save_data()
            await call.answer(f"🗑️ {product_name}\n❌ Удалено из корзины", show_alert=False)
            if user_carts[user_id]:
                await update_cart_message(call, user_id)
            else:
                await call.message.answer("🛒 Корзина пуста")
    else:
        await call.answer("❌ Товар не найден в корзине", show_alert=True)

async def update_cart_message(call: types.CallbackQuery, user_id: str):
    cart = user_carts.get(user_id, [])
    if not cart:
        await call.message.answer("🛒 Корзина пуста")
        return
    total = 0
    text = "🛒 Ваша корзина:\n\n"
    has_inexact_price = False
    has_exact_price_only = True
    for item in cart:
        product = products_db.get(item['id'])
        if product:
            category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
            if not category_info.get('exact_price', True):
                has_inexact_price = True
                has_exact_price_only = False
            if category_info.get('price_per_kg'):
                if category_info.get('average_weight', 0) > 0:
                    estimated_price, note = calculate_product_price(product, item['quantity'])
                    text += f"• {item['name']} - {item['quantity']} шт.\n"
                    text += f"  Примерная стоимость*: ~{estimated_price:.0f} руб.\n"
                    total += estimated_price
                else:
                    text += f"• {item['name']} - {item['quantity']} кг\n"
                    text += f"   Цена будет известна при получении\n"
                    has_exact_price_only = False
            else:
                item_price, note = calculate_product_price(product, item['quantity'])
                text += f"• {item['name']} - {item['quantity']} упак.\n"
                text += f"  Цена: {item_price} руб. {note}\n"
                total += item_price
    if has_inexact_price:
        text += f"\n*Расчетная стоимость. Итоговая цена зависит от фактического веса.\n"
    if has_exact_price_only:
        text += f"\n💰 Итого к оплате: {total:.0f} руб."
    else:
        text += f"\n💰 Примерная сумма: ~{total:.0f} руб.\n"
        text += f"Итоговая стоимость будет рассчитана при получении"
    try:
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))
    except Exception:
        await call.message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))

@dp.callback_query_handler(lambda c: c.data == "clear_cart")
async def clear_cart_callback(call: types.CallbackQuery):
    user_id = str(call.from_user.id)
    user_carts[user_id] = []
    save_data()
    await call.answer("🗑️ Корзина очищена", show_alert=False)
    await call.message.answer("🛒 Корзина пуста")

# ==================== ОФОРМЛЕНИЕ ЗАКАЗА ====================
@dp.callback_query_handler(lambda c: c.data == "checkout")
async def start_checkout(call: types.CallbackQuery, state: FSMContext):
    user_id = str(call.from_user.id)
    cart = user_carts.get(user_id, [])
    if not cart:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    for item in cart:
        product = products_db.get(item['id'])
        if not product or product.get('quantity', 0) < item['quantity']:
            await call.answer(f"❌ Недостаточно товара: {product.get('subcategory', 'Товар')}", show_alert=True)
            return
    await CheckoutState.delivery_method.set()
    await call.message.answer(
        "🚚 Выберите способ получения заказа:\n\n"
        f"🚗 Самовывоз: {PICKUP_ADDRESS}\n"
        f"🚚 Доставка: {DELIVERY_COST} руб. (бесплатно от {FREE_DELIVERY_THRESHOLD} руб.)\n\n"
        f"После оформления заказа с вами свяжется администратор для подтверждения",
        parse_mode="HTML",
        reply_markup=get_delivery_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data in ["pickup", "delivery"], state=CheckoutState.delivery_method)
async def process_delivery_method(call: types.CallbackQuery, state: FSMContext):
    delivery_method = call.data
    async with state.proxy() as data:
        data['delivery_method'] = delivery_method
    if delivery_method == "delivery":
        await CheckoutState.address.set()
        await call.message.answer("🏠 Введите адрес доставки:")
    else:
        async with state.proxy() as data:
            data['address'] = PICKUP_ADDRESS
        await CheckoutState.name.set()
        await call.message.answer(
            "👤 Введите ваше имя для оформления заказа:\n\n"
            "Это поможет нам быстрее связаться с вами."
        )

@dp.message_handler(state=CheckoutState.address)
async def process_address(message: types.Message, state: FSMContext):
    is_valid, address, error_msg = validate_address(message.text)
    if not is_valid:
        await message.answer(error_msg)
        return
    async with state.proxy() as data:
        data['address'] = address
    await CheckoutState.name.set()
    await message.answer(
        "👤 Введите ваше имя для оформления заказа:\n\n"
        "Это поможет нам быстрее связаться с вами."
    )

@dp.message_handler(state=CheckoutState.name)
async def process_name(message: types.Message, state: FSMContext):
    name = message.text.strip()
    if not name:
        await message.answer("❌ Имя не может быть пустым! Введите ваше имя:")
        return
    if len(name) < 2:
        await message.answer("❌ Имя слишком короткое! Введите полное имя:")
        return
    if len(name) > 50:
        await message.answer("❌ Имя слишком длинное! Введите имя короче:")
        return
    async with state.proxy() as data:
        data['name'] = name
    await CheckoutState.phone.set()
    await message.answer(
        "📞 Введите ваш номер телефона для связи:\n\n"
        "Формат: +7XXXXXXXXX или 8XXXXXXXXXX\n"
        "Например: +79991234567"
    )

@dp.message_handler(state=CheckoutState.phone)
async def process_phone(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    
    # Простая валидация номера
    digits = ''.join(filter(str.isdigit, phone))
    
    if len(digits) < 10:
        await message.answer(
            "❌ Номер телефона слишком короткий!\n\n"
            "Введите номер в формате: +7XXXXXXXXX или 8XXXXXXXXXX\n"
            "Например: +79991234567"
        )
        return
    
    if len(digits) > 12:
        await message.answer(
            "❌ Номер телефона слишком длинный!\n\n"
            "Введите номер в формате: +7XXXXXXXXX или 8XXXXXXXXXX\n"
            "Например: +79991234567"
        )
        return
    
    # Проверка, что номер начинается с 7 или 8 (для России)
    if digits[0] == '8':
        digits = '7' + digits[1:]
    elif digits[0] != '7':
        await message.answer(
            "❌ Номер должен начинаться с +7 или 8 (российский номер)!\n\n"
            "Введите номер в формате: +7XXXXXXXXX или 8XXXXXXXXXX\n"
            "Например: +79991234567"
        )
        return
    
    # Форматируем номер для отображения
    formatted_phone = f"+7 ({digits[1:4]}) {digits[4:7]}-{digits[7:9]}-{digits[9:11]}"
    
    async with state.proxy() as data:
        data['phone'] = formatted_phone
        address = data.get('address', PICKUP_ADDRESS)
    
    await create_order(message, state, address)

async def create_order(message_or_call, state: FSMContext, address: str):
    if isinstance(message_or_call, types.Message):
        user_id = str(message_or_call.from_user.id)
        username = message_or_call.from_user.username
        bot_obj = message_or_call.bot
    else:
        user_id = str(message_or_call.from_user.id)
        username = message_or_call.from_user.username
        bot_obj = message_or_call.bot
    cart = user_carts.get(user_id, [])
    async with state.proxy() as data:
        delivery_method = data.get('delivery_method')
        customer_name = data.get('name', username or 'Не указано')
        customer_phone = data.get('phone', 'Не указан')
        # Получаем информацию о забронированной тушке
        selected_product_id = data.get('selected_product')
        weight_category = data.get('weight_category')
    
    # Получаем информацию о забронированной тушке
    reserved_chicken = None
    if selected_product_id and selected_product_id in individual_products_db:
        reserved_chicken = individual_products_db[selected_product_id]
        # Помечаем как проданную
        reserved_chicken['status'] = 'sold'
        reserved_chicken['sold_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
        reserved_chicken['sold_order'] = None  # Будет установлено ниже
    
    if not cart:
        if isinstance(message_or_call, types.Message):
            await message_or_call.answer("❌ Корзина пуста")
        else:
            await message_or_call.answer("❌ Корзина пуста")
        await state.finish()
        return
    total = 0
    order_items = []
    has_inexact_price = False
    has_exact_price_only = True
    for item in cart:
        product = products_db.get(item['id'])
        if product:
            category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
            if not category_info.get('exact_price', True):
                has_inexact_price = True
                has_exact_price_only = False
            if not category_info.get('price_per_kg'):
                item_price, note = calculate_product_price(product, item['quantity'])
                total += item_price
            elif category_info.get('price_per_kg') and category_info.get('average_weight', 0) > 0:
                item_price, note = calculate_product_price(product, item['quantity'])
                total += item_price
            order_items.append({
                'id': product['id'],
                'name': product.get('subcategory', ''),
                'quantity': item['quantity'],
                'price': product.get('price', 0),
                'price_per_kg': category_info.get('price_per_kg', False),
                'average_weight': category_info.get('average_weight', 0),
                'exact_price': category_info.get('exact_price', True)
            })
            product['quantity'] = product.get('quantity', 0) - item['quantity']
    if delivery_method == "delivery" and total < FREE_DELIVERY_THRESHOLD:
        total += DELIVERY_COST
    order_id = str(uuid.uuid4())[:8]
    orders_db[order_id] = {
        'id': order_id,
        'user_id': user_id,
        'username': username,
        'customer_name': customer_name,
        'customer_phone': customer_phone,
        'items': order_items,
        'total': total,
        'delivery_method': delivery_method,
        'address': address,
        'status': '🆕 Новый',
        'status_history': [{
            'status': '🆕 Новый',
            'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
            'changed_by': 'system'
        }],
        'created_at': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'has_exact_price': has_exact_price_only,
        'reserved_chicken': reserved_chicken
    }
    
    # Обновляем запись о тушке с номером заказа
    if reserved_chicken:
        individual_products_db[selected_product_id]['sold_order'] = order_id
    
    update_user_stats(user_id, orders_db[order_id])
    save_data()
    order_text = f"🎉 НОВЫЙ ЗАКАЗ #{order_id}\n\n"
    for item in order_items:
        if item.get('price_per_kg'):
            if item.get('average_weight', 0) > 0:
                item_price = item['price'] * item['average_weight'] * item['quantity']
                order_text += f"• {item['name']} - {item['quantity']} шт.\n"
                order_text += f"  Примерная стоимость: ~{item_price:.0f} руб.\n"
            else:
                order_text += f"• {item['name']} - {item['quantity']} кг\n"
        else:
            product_item = products_db.get(item['id'])
            if product_item:
                category_info = get_category_info(product_item.get('category', ''), product_item.get('subcategory', ''))
                multiplier = category_info.get('multiplier', 1)
                item_price = item['price'] * multiplier * item['quantity']
                order_text += f"• {item['name']} - {item['quantity']} уп.\n"
                order_text += f"  Сумма: {item_price} руб.\n"
    order_text += f"\n💰 Итого: {total} руб.\n"
    if delivery_method == 'pickup':
        order_text += f"🚗 Способ: Самовывоз\n"
        order_text += f"📍 Адрес: {PICKUP_ADDRESS}\n"
    else:
        order_text += f"🚚 Способ: Доставка\n"
        order_text += f"📍 Адрес: {address}\n"
    if total - DELIVERY_COST < FREE_DELIVERY_THRESHOLD:
        order_text += f"🚚 Доставка: {DELIVERY_COST} руб.\n"
    
    # Добавляем информацию о забронированной тушке
    if reserved_chicken:
        order_text += f"\n🐔 ЗАБРОНИРОВАННАЯ ТУШКА\n"
        order_text += f"⚖️ Вес: {reserved_chicken.get('weight', 0)} кг\n"
        order_text += f"📦 Категория: {weight_category or 'Не указана'}\n"
        order_text += f"💰 Цена за кг: {products_db.get(reserved_chicken.get('product_id'), {}).get('price', 0)} руб.\n"
    
    order_text += f"\n👤 Покупатель: {customer_name}\n"
    order_text += f"📞 Телефон: {customer_phone}\n"
    order_text += f"👤 Username: @{username or 'без username'}\n"
    order_text += f"🆔 ID пользователя: {user_id}\n"
    admin_keyboard = get_order_confirmation_keyboard(order_id)
    await bot_obj.send_message(OWNER_ID, order_text, parse_mode="HTML", reply_markup=admin_keyboard)
    # Отправка всем админам
    for admin_id in admins_db:
        try:
            await bot_obj.send_message(admin_id, order_text, parse_mode="HTML", reply_markup=admin_keyboard)
        except:
            pass
    user_carts[user_id] = []
    save_data()
    await state.finish()
    user_response = f"✅ Заказ #{order_id} оформлен!\n\n"
    if delivery_method == 'pickup':
        user_response += f"🚗 Способ получения: Самовывоз\n"
        user_response += f"📍 Адрес: {PICKUP_ADDRESS}\n"
    else:
        user_response += f"🚚 Способ получения: Доставка\n"
        user_response += f"📍 Адрес доставки: {address}\n"
    if total - DELIVERY_COST < FREE_DELIVERY_THRESHOLD:
        user_response += f"🚚 Стоимость доставки: {DELIVERY_COST} руб.\n"
    user_response += f"👤 Имя: {customer_name}\n"
    user_response += f"📞 Телефон: {customer_phone}\n"
    user_response += f"👤 Username: @{username or 'не указан'}\n\n"
    if has_exact_price_only:
        user_response += f"💰 Сумма к оплате: {total} руб.\n\n"
    else:
        user_response += f"💰 Примерная сумма: ~{total} руб.\n"
        user_response += f"Итоговая стоимость будет рассчитана при получении\n\n"
    user_response += "📞 С вами свяжется администратор для подтверждения заказа\n\n"
    user_response += "Спасибо за заказ! 🛍️"
    user_is_buyer = message_or_call.from_user.id in buyer_mode_users
    if isinstance(message_or_call, types.Message):
        await message_or_call.answer(
            user_response,
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=is_admin(message_or_call.from_user.id), is_buyer_mode=user_is_buyer)
        )
    else:
        await message_or_call.message.answer(
            user_response,
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=is_admin(message_or_call.from_user.id), is_buyer_mode=user_is_buyer)
        )

# ==================== АДМИН ФУНКЦИИ ====================
@dp.message_handler(text="📦 Мои ��������аказы")
async def show_user_orders(message: types.Message):
    user_id = str(message.from_user.id)
    user_orders = [order for order in orders_db.values() if order.get('user_id') == user_id]
    if not user_orders:
        is_buyer = message.from_user.id in buyer_mode_users
        await message.answer(
            "📭 У вас пока нет заказов.\n\n"
            "Совершите покупки в нашем каталоге и оформите заказ! 🛍️",
            reply_markup=get_main_keyboard(is_admin=is_admin(message.from_user.id), is_buyer_mode=is_buyer)
        )
        return
    user_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    orders_text = "📦 Ваши заказы:\n\n"
    for i, order in enumerate(user_orders[:10], 1):
        orders_text += f"Заказ #{order['id']}\n"
        orders_text += f"📅 Дата: {order.get('created_at', 'Не указана')}\n"
        orders_text += f"📦 Статус: {order.get('status', 'Не указан')}\n"
        if order.get('delivery_method') == 'pickup':
            orders_text += f"🚗 Способ: Самовывоз\n"
            orders_text += f"📍 Адрес: {PICKUP_ADDRESS}\n"
        else:
            orders_text += f"🚚 Способ: Доставка\n"
            orders_text += f"📍 Адрес: {order.get('address', 'Не указан')}\n"
        if order.get('total', 0) > 0:
            orders_text += f"💰 Сумма: {order.get('total', 0)} руб.\n"
        orders_text += "─" * 20 + "\n\n"
    if len(user_orders) > 10:
        orders_text += f"\nПоказаны последние 10 из {len(user_orders)} заказов"
    is_buyer = message.from_user.id in buyer_mode_users
    await message.answer(orders_text, parse_mode="HTML", reply_markup=get_main_keyboard(is_admin=is_admin(message.from_user.id), is_buyer_mode=is_buyer))

@dp.message_handler(text="ℹ️ О нас")
async def show_about(message: types.Message):
    about_text = (
        "🏡 Русский ТАЙ - Семейная ферма\n\n"
        "Мы находимся в экологически чистом месте Керженского заповедника.\n\n"
        "Предлагаем свежие и натуральные продукты:\n"
        "• 🥚 Свежие яйца разных видов\n"
        "• 🍗 Качественное мясо птицы\n"
        "• 🥫 Вкусные полуфабрикаты\n\n"
        "📞 Контакты:\n"
        f"• Адрес самовывоза: {PICKUP_ADDRESS}\n"
        f"• Телефон: {CONTACT_PHONES}\n\n"
        "🚚 Доставка:\n"
        f"• Стоимость: {DELIVERY_COST} руб. (бесплатно от {FREE_DELIVERY_THRESHOLD} руб.)\n"
        "• По городу: 1-2 дня\n\n"
        "⏰ Работаем: ежедневно с 9:00 до 21:00\n\n"
        "💬 После оформления заказа с вами свяжется администратор для подтверждения"
    )
    is_buyer = message.from_user.id in buyer_mode_users
    await message.answer(about_text, parse_mode="HTML", reply_markup=get_main_keyboard(is_admin=is_admin(message.from_user.id), is_buyer_mode=is_buyer))

# ==================== АДМИН ФУНКЦИИ (ТОЛЬКО ДЛЯ АДМИНА) ====================
@dp.message_handler(text="📋 Активные заказы")
async def show_active_orders(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    active = [o for o in orders_db.values() if o.get('status') in ['🆕 Новый', '✅ Подтвержден']]
    if not active:
        await message.answer("📭 Нет активных заказов.", reply_markup=get_admin_keyboard())
        return
    text = "📋 Активные заказы\n\n"
    new_count = len([o for o in active if o.get('status') == '🆕 Новый'])
    confirmed_count = len([o for o in active if o.get('status') == '✅ Подтвержден'])
    text += f"🆕 Новые: {new_count}\n"
    text += f"✅ Подтвержденные: {confirmed_count}\n"
    text += f"📊 Всего активных: {len(active)}\n\n"
    text += "Нажмите на заказ для управления:"
    await message.answer(text, parse_mode="HTML", reply_markup=get_active_orders_keyboard())

@dp.message_handler(text="📊 Статистика")
async def show_stats(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    in_stock = sum(1 for p in products_db.values() if p.get('quantity', 0) > 0)
    out_of_stock = sum(1 for p in products_db.values() if p.get('quantity', 0) == 0)
    total_stock_value = 0
    for product in products_db.values():
        if product.get('quantity', 0) > 0:
            category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
            if not category_info.get('price_per_kg'):
                item_price, note = calculate_product_price(product, product.get('quantity', 0))
                total_stock_value += item_price
    total_orders = len(orders_db)
    new_orders = len([o for o in orders_db.values() if o.get('status') == '🆕 Новый'])
    confirmed_orders = len([o for o in orders_db.values() if o.get('status') == '✅ Подтвержден'])
    completed_orders = len([o for o in orders_db.values() if o.get('status') == '✅ Выполнен'])
    canceled_orders = len([o for o in orders_db.values() if o.get('status') in ['❌ Отклонен', '❌ Отменен']])
    postponed_orders = len([o for o in orders_db.values() if o.get('status') == '⏰ Перенесен'])
    total_revenue = sum(o.get('total', 0) for o in orders_db.values() if o.get('status') == '✅ Выполнен')
    stats = (
        f"📊 Статистика хозяйства\n\n"
        f"🛍️ Товары:\n"
        f"• Всего товаров: {len(products_db)}\n"
        f"• В наличии: {in_stock}\n"
        f"• Нет в наличии: {out_of_stock}\n"
        f"• Стоимость остатков: ~{total_stock_value:.0f} руб.\n\n"
        f"📦 Заказы:\n"
        f"• Всего заказов: {total_orders}\n"
        f"• Новых: {new_orders}\n"
        f"• Подтвержденных: {confirmed_orders}\n"
        f"• Выполненных: {completed_orders}\n"
        f"• Отмененных: {canceled_orders}\n"
        f"• Перенесенных: {postponed_orders}\n"
        f"• Общая выручка: {total_revenue} руб.\n\n"
        f"👥 Клиенты:\n"
        f"• Всего клиентов: {len(user_stats_db)}\n"
        f"• Активных корзин: {len([c for c in user_carts.values() if c])}\n"
        f"• Ожидают уведомлений: {sum(len(v) for v in notifications_db.values())}\n"
        f"• Администраторов: {len(admins_db) + 1} (включая владельца)"
    )
    await message.answer(stats, parse_mode="HTML", reply_markup=get_admin_keyboard())

@dp.message_handler(text="📈 Аналитика")
async def show_analytics(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    text = "📈 Аналитика хозяйства\n\n"
    today = date.today()
    week_ago = str(today - timedelta(days=7))
    total_views = 0
    product_views_stats = {}
    for day in product_views_db:
        if day >= week_ago:
            for product_id, views in product_views_db[day].items():
                total_views += views
                if product_id not in product_views_stats:
                    product_views_stats[product_id] = 0
                product_views_stats[product_id] += views
    text += f"👁️ Просмотры товаров за неделю: {total_views}\n\n"
    if product_views_stats:
        text += "Топ 5 просматриваемых товаров (за неделю):\n"
        sorted_products = sorted(product_views_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        for i, (product_id, views) in enumerate(sorted_products, 1):
            product = products_db.get(product_id)
            if product:
                text += f"{i}. {product.get('subcategory', '')}\n"
                text += f"   👁️ Просмотров: {views}\n"
    total_requests = 0
    manual_add_stats = {}
    for day in manual_add_requests_db:
        if day >= week_ago:
            for product_id, requests in manual_add_requests_db[day].items():
                total_requests += requests
                if product_id not in manual_add_stats:
                    manual_add_stats[product_id] = 0
                manual_add_stats[product_id] += requests
    text += f"\n📝 Запросы на ручное добавление за неделю: {total_requests}\n\n"
    if manual_add_stats:
        text += "Топ 5 товаров по запросам (за неделю):\n"
        sorted_manual = sorted(manual_add_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        for i, (product_id, requests) in enumerate(sorted_manual, 1):
            product = products_db.get(product_id)
            if product:
                text += f"{i}. {product.get('subcategory', '')}\n"
                text += f"   📝 Запросов: {requests}\n"
    total_waiting = sum(len(v) for v in notifications_db.values())
    text += f"\n🔔 Ожидают уведомлений: {total_waiting} человек\n\n"
    if notifications_db:
        text += "Товары с подписчиками:\n"
        waiting_stats = []
        for product_id, users in notifications_db.items():
            product = products_db.get(product_id)
            if product and users:
                waiting_stats.append((product, len(users)))
        waiting_stats.sort(key=lambda x: x[1], reverse=True)
        for i, (product, count) in enumerate(waiting_stats[:5], 1):
            text += f"{i}. {product.get('subcategory', '')}\n"
            text += f"   👥 Ожидают: {count} человек\n"
    today_str = str(today)
    if today_str in product_views_db:
        today_views = sum(product_views_db[today_str].values())
        text += f"\n📊 Просмотров сегодня: {today_views}\n"
    if today_str in manual_add_requests_db:
        today_requests = sum(manual_add_requests_db[today_str].values())
        text += f"📝 Запросов на ручное добавление сегодня: {today_requests}\n"
    await message.answer(text, parse_mode="HTML", reply_markup=get_admin_keyboard())

@dp.message_handler(text="➕ Добавить товар")
async def add_product_start(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await AddProduct.category.set()
    await message.answer("📝 Выберите категорию:", reply_markup=get_categories_keyboard(is_admin=True))

@dp.message_handler(state=AddProduct.category)
async def process_category_state(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    
    # Извлекаем чистое название категории (убираем счетчик в скобках)
    category_text = message.text.split(' (')[0]
    
    if category_text not in CATEGORIES:
        if message.text == "👑 Панель админа":
            await state.finish()
            await message.answer("❌ Добавление отменено", reply_markup=get_admin_keyboard())
        elif message.text == "↩️ Назад":
            await state.finish()
            await message.answer("↩️ Возвращаемся...", reply_markup=get_admin_keyboard())
        else:
            await message.answer("❌ Выберите категорию из списка!")
        return
    
    async with state.proxy() as data:
        data['category'] = category_text
    await AddProduct.next()
    await message.answer("📂 Выберите рубрику:", reply_markup=get_subcategories_keyboard(category_text, is_admin=True))

@dp.message_handler(state=AddProduct.subcategory)
async def process_subcategory_state(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    
    if message.text == "👑 Панель админа":
        await state.finish()
        await message.answer("❌ Добавление отменено", reply_markup=get_admin_keyboard())
        return
    
    if message.text == "↩️ К категориям":
        await AddProduct.category.set()
        await message.answer("↩️ Выберите категорию:", reply_markup=get_categories_keyboard(is_admin=True))
        return
    
    async with state.proxy() as data:
        category = data['category']
        category_data = CATEGORIES.get(category)
        
        # Извлекаем чистое название (убираем счетчик)
        subcategory_text = message.text.split(' (')[0]
        
        # Проверяем, есть ли вложенные подкатегории (для мяса)
        subcategories = category_data.get("subcategories", [])
        if isinstance(subcategories, dict):
            # Для мяса: сначала выбираем тип хранения (охлажденное/замороженное)
            if subcategory_text in subcategories:
                data['subcategory_type'] = subcategory_text  # "❄️ Охлажденное" или "🧊 Замороженное"
                # Показываем рубрики внутри этого типа
                await message.answer("🥩 Выберите рубрику:", reply_markup=get_rubrics_keyboard(category, subcategory_text, is_admin=True))
                return
            elif data.get('subcategory_type'):
                # Уже выбран тип хранения, теперь выбираем рубрику
                data['subcategory'] = subcategory_text
            else:
                await message.answer("❌ Сначала выберите тип хранения!")
                return
        else:
            # Для яиц и полуфабрикатов
            data['subcategory'] = subcategory_text
        
        subcategory = data.get('subcategory')
        subcategory_type = data.get('subcategory_type')
        
        # Проверяем существование товара
        existing_products = [p for p in products_db.values()
                             if p.get('category') == category and
                             p.get('subcategory') == subcategory and
                             (not subcategory_type or p.get('subcategory_type') == subcategory_type)]
        if existing_products:
            await message.answer(f"❌ В рубрике '{subcategory}' уже есть товар. Можно добавить только один товар в рубрику.")
            await state.finish()
            await message.answer("↩️ Возвращаемся...", reply_markup=get_admin_keyboard())
            return
    
    await AddProduct.next()
    category_info = get_category_info(category, subcategory)
    if category_info.get('price_per_kg'):
        await message.answer("💰 Введите цену товара за 1 кг (только число):")
    else:
        multiplier = category_info.get('multiplier', 1)
        await message.answer(f"💰 Введите цену товара за 1 шт (упаковка {multiplier} шт, только число):")

@dp.message_handler(state=AddProduct.price)
async def process_price_state(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    is_valid, price, error_msg = validate_price(message.text)
    if not is_valid:
        await message.answer(error_msg)
        return
    async with state.proxy() as data:
        data['price'] = price
    await AddProduct.next()
    await message.answer("📦 Введите количество на складе:")

@dp.message_handler(state=AddProduct.quantity)
async def process_quantity_state(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    is_valid, quantity, error_msg = validate_quantity(message.text, 999999)
    if not is_valid:
        await message.answer(error_msg)
        return
    async with state.proxy() as data:
        data['quantity'] = quantity
    await AddProduct.next()
    await message.answer("📸 Отправьте фото товара:")

@dp.message_handler(content_types=types.ContentType.PHOTO, state=AddProduct.photo)
async def process_photo_state(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    async with state.proxy() as data:
        product_id = str(uuid.uuid4())[:8]
        category = data['category']
        subcategory = data['subcategory']
        subcategory_type = data.get('subcategory_type')  # Только для мяса
        
        product_data = {
            'id': product_id,
            'category': category,
            'subcategory': subcategory,
            'price': data['price'],
            'quantity': data['quantity'],
            'photo': message.photo[-1].file_id,
            'published': False,
            'created_at': datetime.now().strftime("%d.%m.%Y %H:%M")
        }
        
        # Для мяса добавляем тип хранения
        if subcategory_type:
            product_data['subcategory_type'] = subcategory_type
        
        products_db[product_id] = product_data
        save_data()
    await message.answer_photo(
        message.photo[-1].file_id,
        caption=f"✅ Товар добавлен!\n\n" + format_product_info(products_db[product_id]),
        parse_mode="HTML"
    )
    await state.finish()
    await message.answer("✅ Товар сохранен! Теперь можете опубликовать его в канале.", reply_markup=get_admin_keyboard())

@dp.message_handler(text="📊 Загрузить прайс (Excel)")
async def upload_excel_start(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    await UploadExcelState.product_id.set()
    await message.answer(
        "📊 Загрузка прайса из Excel\n\n"
        "📝 Формат файла:\n"
        "• Столбец A: Название (например 'Цыпленок бройлер')\n"
        "• Столбец B: Вес в кг (например 2.5)\n\n"
        "📤 Отправьте Excel файл (.xlsx):",
        parse_mode="HTML"
    )

@dp.message_handler(content_types=types.ContentType.DOCUMENT, state=UploadExcelState.product_id)
async def process_excel_upload(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    
    # Проверяем расширение файла
    file_name = message.document.file_name
    if not file_name.endswith('.xlsx'):
        await message.answer("❌ Пожалуйста, отправьте файл в формате .xlsx")
        return
    
    try:
        # Скачиваем файл
        file_info = await bot.get_file(message.document.file_id)
        file_path = file_info.file_path
        downloaded_file = await bot.download_file(file_path)
        
        # Сохраняем временно
        import io
        try:
            import openpyxl
        except ImportError:
            await message.answer("❌ Модуль openpyxl не установлен. Установите командой: pip install openpyxl")
            await state.finish()
            return
        
        # Читаем Excel
        wb = openpyxl.load_workbook(filename=io.BytesIO(downloaded_file.read()))
        sheet = wb.active
        
        # Считываем данные
        uploaded_count = 0
        error_count = 0
        
        for row in sheet.iter_rows(min_row=2, values_only=True):  # Пропускаем заголовок
            try:
                if len(row) < 2:
                    continue
                
                name = str(row[0]).strip() if row[0] else None
                weight = float(row[1]) if row[1] else None
                
                if not name or not weight:
                    error_count += 1
                    continue
                
                # Находим товар по названию
                product_id = None
                for pid, prod in products_db.items():
                    if prod.get('subcategory') == name and name == "🐓 Цыпленок бройлер":
                        product_id = pid
                        break
                
                if not product_id:
                    error_count += 1
                    continue
                
                # Создаем индивидуальную тушку
                indiv_id = str(uuid.uuid4())[:8]
                individual_products_db[indiv_id] = {
                    'id': indiv_id,
                    'product_id': product_id,
                    'subcategory': name,
                    'weight': weight,
                    'status': 'available',  # available, reserved, sold
                    'reserved_by': None,
                    'added_at': datetime.now().strftime("%d.%m.%Y %H:%M")
                }
                uploaded_count += 1
                
            except Exception as e:
                error_count += 1
                logging.error(f"Ошибка при чтении строки: {e}")
        
        save_data()
        
        await message.answer(
            f"✅ Прайс загружен!\n\n"
            f"📊 Добавлено тушек: {uploaded_count}\n"
            f"❌ Ошибок: {error_count}\n\n"
            f"Теперь клиенты могут выбирать цыпленка по весу.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при загрузке: {e}")
    
    await state.finish()

@dp.message_handler(state=UploadExcelState.product_id)
async def invalid_excel_format(message: types.Message, state: FSMContext):
    await message.answer("❌ Это не Excel файл. Пожалуйста, отправьте файл .xlsx")

@dp.message_handler(text="📦 Пополнить остатки")
async def add_quantity_start(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    if not products_db:
        await message.answer("📭 В хозяйстве пока нет товаров.", reply_markup=get_admin_keyboard())
        return
    text = "📦 Управление остатками товаров\n\n"
    text += "Выберите товар для изменения остатков:\n\n"
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        if key not in products_by_category:
            products_by_category[key] = product
    for i, product in enumerate(products_by_category.values(), 1):
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        text += f"{i}. {product.get('subcategory', '')}\n"
        text += f"   📦 Остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        text += f"   💰 Цена: {product.get('price', 0)} руб."
        if category_info.get('price_per_kg'):
            text += "/кг\n"
        else:
            text += "/шт\n"
        text += f"   🔹 ID: `{product.get('id')}`\n\n"
    text += "Нажмите на кнопку с товаром ниже для изменения остатков:"
    await message.answer(text, parse_mode="HTML", reply_markup=get_products_for_adjust_keyboard())

@dp.callback_query_handler(lambda c: c.data.startswith('adjust_'))
async def adjust_quantity_start(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    await state.update_data(product_id=product_id)
    await AdjustStockState.quantity.set()
    await call.message.answer(
        f"📦 Изменение остатков на складе:\n\n"
        f"{product.get('subcategory', '')}\n"
        f"📦 Текущий остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n\n"
        f"Введите новое количество:\n"
        f"Можно ввести:\n"
        f"• `10` - установить 10 шт\n"
        f"• `+5` - добавить 5 шт\n"
        f"• `-3` - убрать 3 шт\n"
        f"• `0` - обнулить остатки",
        parse_mode="HTML"
    )

@dp.message_handler(state=AdjustStockState.quantity)
async def process_adjust_stock_quantity(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        if not product:
            await message.answer("❌ Товар не найден в базе данных")
            await state.finish()
            return
        quantity_str = message.text.strip()
        old_quantity = product.get('quantity', 0)
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        if quantity_str.startswith('+'):
            delta = int(quantity_str[1:])
            new_quantity = old_quantity + delta
            operation = "добавлено"
        elif quantity_str.startswith('-'):
            delta = int(quantity_str[1:])
            new_quantity = old_quantity - delta
            operation = "вычтено"
        else:
            new_quantity = int(quantity_str)
            operation = "установлено"
        if new_quantity < 0:
            await message.answer("❌ Нельзя установить отрицательное количество!")
            await state.finish()
            return
        if new_quantity > 999999:
            await message.answer("❌ Слишком большое количество!")
            await state.finish()
            return
        product['quantity'] = new_quantity
        save_data()
        if old_quantity == 0 and new_quantity > 0 and product_id in notifications_db:
            await send_notifications(product_id)
        await message.answer(
            f"✅ Остатки на складе обновлены!\n\n"
            f"{product.get('subcategory', '')}\n"
            f"📦 Было: {old_quantity} {category_info.get('unit', 'шт')}\n"
            f"📦 Стало: {new_quantity} {category_info.get('unit', 'шт')}\n"
            f"📊 {operation}: {abs(new_quantity - old_quantity)} {category_info.get('unit', 'шт')}",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    except ValueError:
        await message.answer("❌ Введите корректное число!")
    except Exception as e:
        await message.answer(f"❌ Ошибка при изменении остатков: {str(e)}")
    finally:
        await state.finish()

@dp.message_handler(text="✏️ Управление товарами")
async def manage_products(message: types.Message):
    if not is_admin(message.from_user.id):
        return
    if not products_db:
        await message.answer("📭 В хозяйстве пока нет товаров.", reply_markup=get_admin_keyboard())
        return
    text = "✏️ Управление товарами\n\n"
    text += "Выберите товар для редактирования или удаления:\n\n"
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        if key not in products_by_category:
            products_by_category[key] = product
    for i, product in enumerate(products_by_category.values(), 1):
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        text += f"{i}. {product.get('subcategory', '')}\n"
        text += f"   📦 Остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        text += f"   💰 Цена: {product.get('price', 0)} руб."
        if category_info.get('price_per_kg'):
            text += "/кг\n"
        else:
            text += "/шт\n"
        text += f"   🔹 ID: `{product.get('id')}`\n\n"
    text += "Нажмите на кнопку с товаром ниже для управления:"
    await message.answer(text, parse_mode="HTML", reply_markup=get_product_management_keyboard())

@dp.callback_query_handler(lambda c: c.data.startswith('edit_') and not c.data.startswith('edit_price_') and not c.data.startswith('edit_quantity_') and not c.data.startswith('edit_photo_'))
async def edit_product_start(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    caption = format_product_info(product)
    caption = f"✏️ Редактирование товара\n\n{caption}"
    if product.get('photo'):
        try:
            await call.message.answer_photo(
                product['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=get_edit_product_keyboard(product_id)
            )
        except:
            await call.message.answer(
                caption,
                parse_mode="HTML",
                reply_markup=get_edit_product_keyboard(product_id)
            )
    else:
        await call.message.answer(
            caption,
            parse_mode="HTML",
            reply_markup=get_edit_product_keyboard(product_id)
        )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_price_'))
async def edit_product_price_start(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    await state.update_data(product_id=product_id)
    await EditProduct.new_price.set()
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    if category_info.get('price_per_kg'):
        unit = "за 1 кг"
    else:
        multiplier = category_info.get('multiplier', 1)
        unit = f"за 1 шт (упаковка {multiplier} шт)"
    await call.message.answer(
        f"💰 Изменение цены товара\n\n"
        f"{product.get('subcategory', '')}\n"
        f"📦 Текущая цена: {product.get('price', 0)} руб. {unit}\n\n"
        f"Введите новую цену (только число):",
        parse_mode="HTML"
    )

@dp.message_handler(state=EditProduct.new_price)
async def process_edit_price(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        if not product:
            await message.answer("❌ Товар не найден в базе данных")
            await state.finish()
            return
        is_valid, new_price, error_msg = validate_price(message.text)
        if not is_valid:
            await message.answer(error_msg)
            await state.finish()
            return
        old_price = product.get('price', 0)
        product['price'] = new_price
        save_data()
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        if category_info.get('price_per_kg'):
            unit = "за 1 кг"
        else:
            multiplier = category_info.get('multiplier', 1)
            unit = f"за 1 шт (упаковка {multiplier} шт)"
        await message.answer(
            f"✅ Цена товара изменена!\n\n"
            f"{product.get('subcategory', '')}\n"
            f"💰 Было: {old_price} руб. {unit}\n"
            f"💰 Стало: {new_price} руб. {unit}\n"
            f"📊 Изменение: {new_price - old_price} руб.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при изменении цены: {str(e)}")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_quantity_'))
async def edit_product_quantity_start(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    await call.message.answer(
        f"📦 Изменение остатков товара:\n\n"
        f"{product.get('subcategory', '')}\n"
        f"📦 Текущий остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n\n"
        f"Введите новое количество:\n"
        f"Можно ввести:\n"
        f"• `10` - установить 10 шт\n"
        f"• `+5` - добавить 5 шт\n"
        f"• `-3` - убрать 3 шт\n"
        f"• `0` - обнулить остатки",
        parse_mode="HTML"
    )
    await AdjustStockState.product_id.set()
    await AdjustStockState.quantity.set()
    await dp.current_state().update_data(product_id=product_id)

@dp.callback_query_handler(lambda c: c.data.startswith('edit_photo_'))
async def edit_product_photo_start(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    await state.update_data(product_id=product_id)
    await EditProduct.new_photo.set()
    await call.message.answer(
        f"📸 Изменение фото товара\n\n"
        f"{product.get('subcategory', '')}\n\n"
        f"Отправьте новое фото для товара:",
        parse_mode="HTML"
    )

@dp.message_handler(content_types=types.ContentType.PHOTO, state=EditProduct.new_photo)
async def process_edit_photo(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        if not product:
            await message.answer("❌ Товар не найден в базе данных")
            await state.finish()
            return
        product['photo'] = message.photo[-1].file_id
        save_data()
        await message.answer_photo(
            message.photo[-1].file_id,
            caption=f"✅ Фото товара обновлено!\n\n"
                    f"{product.get('subcategory', '')}\n\n"
                    f"Фото успешно изменено.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при изменении фото: {str(e)}")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('delete_confirm_'))
async def delete_product_confirm(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    in_active_orders = False
    for order in orders_db.values():
        if order.get('status') in ['🆕 Новый', '✅ Подтвержден']:
            for item in order.get('items', []):
                if item.get('id') == product_id:
                    in_active_orders = True
                    break
        if in_active_orders:
            break
    if in_active_orders:
        await call.answer("❌ Нельзя удалить товар, который есть в активных заказах!", show_alert=True)
        return
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_yes_{product_id}"),
        InlineKeyboardButton("❌ Нет, отмена", callback_data=f"delete_no_{product_id}")
    )
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    await call.message.answer(
        f"⚠️ Подтверждение удаления товара\n\n"
        f"{product.get('subcategory', '')}\n"
        f"📦 Остаток: {product.get('quantity', 0)} шт\n"
        f"💰 Цена: {product.get('price', 0)} руб.\n\n"
        f"ВНИМАНИЕ! Это действие невозможно отменить.\n"
        f"Товар будет полностью удален из базы данных.\n\n"
        f"Вы уверены, что хотите удалить этот товар?",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('delete_yes_'))
async def delete_product_yes(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    product_name = product.get('subcategory', 'товар')
    del products_db[product_id]
    if product_id in notifications_db:
        del notifications_db[product_id]
    save_data()
    await call.message.answer(
        f"🗑️ Товар удален!\n\n"
        f"{product_name} успешно удален из базы данных.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    await call.answer(f"✅ Товар '{product_name}' удален", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('delete_no_'))
async def delete_product_no(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    await call.message.answer(
        f"✅ Удаление отменено\n\n"
        f"Товар {product.get('subcategory', '')} не был удален.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    await call.answer("❌ Удаление отменено", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "back_to_product_management")
async def back_to_product_management(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    await manage_products(call.message)

# ==================== УПРАВЛЕНИЕ АДМИНИСТРАТОРАМИ ====================
@dp.message_handler(text="👥 Управление админами")
async def admin_management_menu(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.answer("❌ Только владелец может управлять администраторами!")
        return
    await message.answer(
        "👥 Управление администраторами\n\n"
        f"Владелец: {OWNER_ID}\n"
        f"Администраторов: {len(admins_db)}\n\n"
        "Выберите действие:",
        parse_mode="HTML",
        reply_markup=get_admin_management_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data == "add_admin")
async def add_admin_start(call: types.CallbackQuery, state: FSMContext):
    if not is_owner(call.from_user.id):
        await call.answer("❌ Только владелец может добавлять администраторов!", show_alert=True)
        return
    await AddAdminState.action.set()
    await call.message.answer(
        "➕ Добавление администратора\n\n"
        "Отправьте ID пользователя, которого хотите сделать администратором.\n\n"
        "Чтобы узнать ID пользователя, отправьте команду /id в бота @userinfobot",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 Назад", callback_data="admin_management")
        )
    )

@dp.message_handler(state=AddAdminState.action)
async def process_add_admin(message: types.Message, state: FSMContext):
    if not is_owner(message.from_user.id):
        await message.answer("❌ Только владелец может добавлять администраторов!")
        await state.finish()
        return
    
    # Завершаем все другие состояния перед добавлением ��дмина
    await state.finish()
    
    try:
        new_admin_id = int(message.text.strip())
        if new_admin_id == OWNER_ID:
            await message.answer("❌ Владелец уже является администратором!")
            await state.finish()
            return
        if new_admin_id in admins_db:
            await message.answer(f"❌ Пользователь {new_admin_id} уже является администратором!")
            await state.finish()
            return
        admins_db.add(new_admin_id)
        save_data()
        await message.answer(
            f"✅ Пользов��тель {new_admin_id} добавлен в администраторы!\n\n"
            f"Теперь он может управлять хозяйством.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
        # Уведомление новому админу
        try:
            await bot.send_message(
                new_admin_id,
                f"🎉 Вас добавили в администраторы бота Русский ТАЙ!\n\n"
                f"Теперь вы можете управлять хозяйством.\n"
                f"Используйте кнопку '👑 Панель админа' для доступа.",
                parse_mode="HTML"
            )
        except:
            pass
    except ValueError:
        await message.answer("❌ Введите корректный числовой ID пользователя!")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data == "remove_admin")
async def remove_admin_start(call: types.CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("❌ Только владелец может удалять администраторов!", show_alert=True)
        return
    if not admins_db:
        await call.message.answer(
            "📭 Нет администраторов ����ля удаления.\n\n"
            f"Владелец: {OWNER_ID}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("🔙 Назад", callback_data="admin_management")
            )
        )
        return
    await call.message.answer(
        "➖ Удаление администратора\n\n"
        "Выберите администратора для удаления:",
        parse_mode="HTML",
        reply_markup=get_admins_list_keyboard()
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('remove_admin_'))
async def remove_admin_confirm(call: types.CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("❌ Только владелец может удалять администраторов!", show_alert=True)
        return
    admin_id = int(call.data.split('_')[2])
    if admin_id not in admins_db:
        await call.answer("❌ Этот пользователь не является администратором!", show_alert=True)
        return
    admins_db.discard(admin_id)
    save_data()
    await call.message.answer(
        f"✅ Администратор {admin_id} удален!\n\n"
        f"Теперь он больше не имеет прав администратора.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    await call.answer(f"✅ Администратор {admin_id} удален", show_alert=True)
    # Уведомление удаленному админу
    try:
        await bot.send_message(
            admin_id,
            f"⚠️ Вас удалили из администраторов бота Русский ТАЙ!\n\n"
            f"Теперь вы больше не можете управлять хозяйством.",
            parse_mode="HTML"
        )
    except:
        pass

@dp.callback_query_handler(lambda c: c.data == "list_admins")
async def list_admins(call: types.CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("❌ Только владелец может просматривать список администраторов!", show_alert=True)
        return
    text = "📋 Список администраторов\n\n"
    text += f"👑 Владелец: {OWNER_ID}\n\n"
    if admins_db:
        text += "👥 Администраторы:\n"
        for admin_id in admins_db:
            text += f"• {admin_id}\n"
    else:
        text += "📭 Нет дополнительных администраторов\n"
    text += f"\nВсего: {len(admins_db) + 1} (включая владельца)"
    await call.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 Назад", callback_data="admin_management")
        )
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "admin_management")
async def admin_management_callback(call: types.CallbackQuery):
    if not is_owner(call.from_user.id):
        await call.answer("❌ Только владелец может управлять администраторами!", show_alert=True)
        return
    await call.message.edit_reply_markup(None)
    await admin_management_menu(call.message)

# ==================== ОБРАБОТКА ЗАКАЗОВ (АДМИН) ====================
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_'))
async def confirm_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('confirm_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    order['status'] = '✅ Подтвержден'
    order['status_history'].append({
        'status': '✅ Подтвержден',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    save_data()
    try:
        await bot.send_message(
            order['user_id'],
            f"✅ Ваш заказ #{order_id} подтвержден!\n\n"
            f"Администратор подтвердил ваш заказ.\n"
            f"Скоро с вами свяжутся для уточнения деталей.\n\n"
            f"Спасибо за покупку! 🛍️",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    await call.answer("✅ Заказ подтвержден", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('reject_'))
async def reject_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('reject_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    for item in order.get('items', []):
        product = products_db.get(item['id'])
        if product:
            product['quantity'] = product.get('quantity', 0) + item['quantity']
    order['status'] = '❌ Отклонен'
    order['status_history'].append({
        'status': '❌ Отклонен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    update_user_stats(order['user_id'], order, 'canceled')
    save_data()
    try:
        await bot.send_message(
            order['user_id'],
            f"❌ Ваш заказ #{order_id} отклонен\n\n"
            f"К сожалению, администратор отклонил ваш заказ.\n"
            f"Товары возвращены на склад.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    await call.answer("❌ Заказ отклонен", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('complete_'))
async def complete_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('complete_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    order['status'] = '✅ Выполнен'
    order['status_history'].append({
        'status': '✅ Выполнен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    update_user_stats(order['user_id'], order, 'completed')
    save_data()
    try:
        thank_you_message = get_random_thank_you_message()
        await bot.send_message(
            order['user_id'],
            f"✅ Ваш заказ #{order_id} выполнен!\n\n"
            f"Заказ успешно выдан/доставлен.\n\n"
            f"{thank_you_message}",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке благодарственного сообщения: {e}")
    await call.answer("✅ Заказ отмечен как выполненный", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('cancel_'))
async def cancel_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('cancel_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    for item in order.get('items', []):
        product = products_db.get(item['id'])
        if product:
            product['quantity'] = product.get('quantity', 0) + item['quantity']
    order['status'] = '❌ Отменен'
    order['status_history'].append({
        'status': '❌ Отменен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    update_user_stats(order['user_id'], order, 'canceled')
    save_data()
    try:
        await bot.send_message(
            order['user_id'],
            f"❌ Ваш заказ #{order_id} отменен\n\n"
            f"К сожалению, администратор отменил ваш заказ.\n"
            f"Товары возвращены на склад.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    await call.answer("❌ Заказ отменен", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('postpone_'))
async def postpone_order_start(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('postpone_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    await state.update_data(order_id=order_id)
    await PostponeOrderState.new_date.set()
    await call.message.answer(
        f"⏰ Перенос заказа #{order_id}\n\n"
        f"Введите новую дату и время для заказа (например: 'завтра 18:00' или '25.12.2024 15:00'):",
        parse_mode="HTML"
    )

@dp.message_handler(state=PostponeOrderState.new_date)
async def process_postpone_date(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.finish()
        return
    data = await state.get_data()
    order_id = data.get('order_id')
    order = orders_db.get(order_id)
    if not order:
        await message.answer("❌ Заказ не найден")
        await state.finish()
        return
    new_date = message.text.strip()
    if len(new_date) < 3 or len(new_date) > 100:
        await message.answer("❌ Слишком короткая или длинная дата")
        await state.finish()
        return
    dangerous_chars = ['<', '>', '&', ';', '|', '`', '$', '(', ')']
    for char in dangerous_chars:
        if char in new_date:
            await message.answer(f"❌ Дата содержит недопустимый символ: {char}")
            await state.finish()
            return
    order['status'] = f'⏰ Перенесен ({new_date})'
    order['status_history'].append({
        'status': f'⏰ Перенесен ({new_date})',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{message.from_user.id}",
        'new_date': new_date
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    update_user_stats(order['user_id'], order, 'postponed')
    save_data()
    try:
        await bot.send_message(
            order['user_id'],
            f"⏰ Ваш заказ #{order_id} перенесен\n\n"
            f"Администратор перенес ваш заказ на {new_date}.\n"
            f"С вами свяжутся для уточнения деталей.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    await message.answer(
        f"✅ Заказ #{order_id} перенесен на {new_date}",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('contact_'))
async def contact_client(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('contact_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    username = order.get('username')
    if username:
        await call.message.answer(
            f"💬 Связь с клиентом\n\n"
            f"Заказ #{order_id}\n"
            f"Клиент: @{username}\n\n"
            f"Нажмите на кнопку ниже, чтобы написать сообщение:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton(
                    f"💬 Написать @{username}",
                    url=f"https://t.me/{username}"
                )
            ).add(
                InlineKeyboardButton("🔙 Назад к заказу", callback_data=f"manage_order_{order_id}"),
                InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
            )
        )
    else:
        await call.answer("❌ У клиента нет username", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('manage_order_'))
async def manage_specific_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('manage_order_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    order_text = format_order_info(order)
    keyboard = InlineKeyboardMarkup(row_width=2)
    if order.get('status') == '🆕 Новый':
        keyboard.add(
            InlineKeyboardButton("✅ Подтвердить", callback_data=f"confirm_{order_id}"),
            InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{order_id}")
        )
    elif order.get('status') == '✅ Подтвержден':
        keyboard.add(
            InlineKeyboardButton("✅ Выполнен", callback_data=f"complete_{order_id}"),
            InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{order_id}"),
            InlineKeyboardButton("⏰ Перенести", callback_data=f"postpone_{order_id}")
        )
    keyboard.add(
        InlineKeyboardButton("💬 Связаться", callback_data=f"contact_{order_id}"),
        InlineKeyboardButton("📊 Статистика клиента", callback_data=f"client_stats_{order_id}")
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад к списку", callback_data="back_to_orders_list"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    try:
        await call.message.edit_text(
            f"Управление заказом #{order_id}\n\n{order_text}",
            parse_mode="HTML",
            reply_markup=keyboard
        )
    except Exception:
        await call.message.answer(
            f"Управление заказом #{order_id}\n\n{order_text}",
            parse_mode="HTML",
            reply_markup=keyboard
        )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('client_stats_'))
async def show_client_stats(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('client_stats_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    user_id = order['user_id']
    stats_text = format_client_stats(user_id)
    await call.message.answer(stats_text, parse_mode="HTML", reply_markup=get_admin_keyboard())
    await call.answer("📊 Статистика отправлена", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "back_to_orders_list")
async def back_to_orders_list(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    await show_active_orders(call.message)

@dp.callback_query_handler(lambda c: c.data == "refresh_active_orders")
async def refresh_active_orders(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    await call.answer("🔄 Обновляем список...", show_alert=False)
    await show_active_orders(call.message)

@dp.callback_query_handler(lambda c: c.data.startswith('view_order_'))
async def view_client_order(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    order_id = call.data.replace('view_order_', '')
    order = orders_db.get(order_id)
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    order_text = format_order_info(order)
    await call.message.answer(order_text, parse_mode="HTML", reply_markup=get_admin_keyboard())
    await call.answer("📋 Информация о заказе", show_alert=True)

# ==================== УВЕДОМЛЕНИЯ ====================
def increment_product_view(product_id: str):
    today = str(date.today())
    if today not in product_views_db:
        product_views_db[today] = {}
    if product_id not in product_views_db[today]:
        product_views_db[today][product_id] = 0
    product_views_db[today][product_id] += 1
    save_data()

def increment_manual_add_request(product_id: str):
    today = str(date.today())
    if today not in manual_add_requests_db:
        manual_add_requests_db[today] = {}
    if product_id not in manual_add_requests_db[today]:
        manual_add_requests_db[today][product_id] = 0
    manual_add_requests_db[today][product_id] += 1
    save_data()

def update_user_stats(user_id: str, order_data: dict, status_change: str = None):
    if user_id not in user_stats_db:
        user_stats_db[user_id] = {
            'total_orders': 0,
            'completed_orders': 0,
            'canceled_orders': 0,
            'postponed_orders': 0,
            'total_spent': 0,
            'last_order_date': None,
            'first_order_date': None,
            'username': order_data.get('username', ''),
            'last_status_change': None
        }
    stats = user_stats_db[user_id]
    if status_change:
        stats['last_status_change'] = datetime.now().strftime("%d.%m.%Y %H:%M")
        if status_change == 'completed':
            stats['completed_orders'] = stats.get('completed_orders', 0) + 1
            stats['total_spent'] = stats.get('total_spent', 0) + order_data.get('total', 0)
        elif status_change == 'canceled':
            stats['canceled_orders'] = stats.get('canceled_orders', 0) + 1
        elif status_change == 'postponed':
            stats['postponed_orders'] = stats.get('postponed_orders', 0) + 1
    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    if not stats.get('first_order_date'):
        stats['first_order_date'] = current_date
    stats['last_order_date'] = current_date
    save_data()

@dp.callback_query_handler(lambda c: c.data.startswith('notify_'))
async def notify_product(call: types.CallbackQuery):
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    if is_admin(call.from_user.id):
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    user_id = str(call.from_user.id)
    if product_id not in notifications_db:
        notifications_db[product_id] = []
    if user_id not in notifications_db[product_id]:
        notifications_db[product_id].append(user_id)
        save_data()
        await call.answer("🔔 Подписка оформлена! Вы будете уведомлены о появлении товара", show_alert=True)
    else:
        await call.answer("ℹ️ Вы уже подписаны на уведомление", show_alert=True)

async def send_notifications(product_id: str):
    product = products_db.get(product_id)
    if not product or product_id not in notifications_db:
        return
    for user_id in notifications_db[product_id]:
        try:
            await bot.send_message(
                user_id,
                f"🔔 Товар появился в наличии!\n\n"
                f"{product.get('subcategory', '')}\n"
                f"📦 Остаток: {product.get('quantity', 0)} шт\n\n"
                f"Скорее заказывайте! 🛍️",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")
    notifications_db[product_id] = []
    save_data()

# ==================== АВТОУДАЛЕНИЕ ПЕРСОНАЛЬНЫХ ДАННЫХ (152-ФЗ) ====================
async def auto_delete_old_orders(days: int = 30):
    """Автоматическое удаление выполненных заказов старше N дней"""
    try:
        now = datetime.now()
        deleted_count = 0
        orders_to_delete = []
        for order_id, order in orders_db.items():
            if order.get('status') == '✅ Выполнен':
                date_str = order.get('status_updated_at') or order.get('created_at')
                if date_str:
                    try:
                        order_date = datetime.strptime(date_str, "%d.%m.%Y %H:%M")
                        if (now - order_date).days >= days:
                            orders_to_delete.append(order_id)
                            deleted_count += 1
                            safe_user_id = sanitize_log_data(int(order.get('user_id', 0)))
                            print(f"🗑️ Автоудаление заказа #{order_id} (пользователь: {safe_user_id})")
                    except:
                        pass
        for order_id in orders_to_delete:
            del orders_db[order_id]
        if deleted_count > 0:
            save_data()
            print(f"✅ Автоудаление: {deleted_count} заказов старше {days} дней удалено")
            try:
                await bot.send_message(OWNER_ID,
                    f"🧹 <b>Автоматическое удаление данных</b>\n\n"
                    f"Удалено заказов: {deleted_count}\n"
                    f"Срок хранения: {days} дней\n"
                    f"Персональные данные уничтожены в ��оответствии с 152-ФЗ.",
                    parse_mode="HTML")
            except:
                pass
    except Exception as e:
        print(f"❌ Ошиб��а автоудаления: {e}")

async def schedule_daily_cleanup():
    """Ежедневное автоудаление в 3:00"""
    while True:
        try:
            now = datetime.now()
            next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)
            wait_seconds = (next_run - now).total_seconds()
            print(f"⏰ Следующее автоудаление через {wait_seconds/3600:.1f} часов")
            await asyncio.sleep(wait_seconds)
            await auto_delete_old_orders(days=30)
        except Exception as e:
            print(f"❌ Ошибка в планировщике: {e}")
            await asyncio.sleep(3600)

# ==================== ОБРАБОТКА КНОПОК ИЗ КАНАЛА ====================
@dp.callback_query_handler(lambda c: c.data.startswith('channel_order_'))
async def process_channel_order(call: types.CallbackQuery):
    product_id = call.data.split('_')[-1]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    if is_admin(call.from_user.id):
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        user_carts[user_id] = []
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    if item_index >= 0:
        user_carts[user_id][item_index]['quantity'] += 1
    else:
        user_carts[user_id].append({
            'id': product_id,
            'name': product.get('subcategory', ''),
            'quantity': 1
        })
    save_data()
    await call.answer(f"✅ {product.get('subcategory', 'Товар')} добавлен в корзину!", show_alert=True)
    try:
        bot_info = await call.bot.get_me()
        bot_username = bot_info.username
    except:
        bot_username = "RusskiyTAY_bot"
    try:
        await call.bot.send_message(
            call.from_user.id,
            f"🛒 Товар добавлен в корзину!\n\n"
            f"{product.get('subcategory', '')} "
            f"успешно добавлен в вашу корзину.\n\n"
            f"Перейдите в бота @{bot_username} чтобы оформить заказ.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton(
                    "🛒 Перейти в корзину",
                    url=f"https://t.me/{bot_username}?start=cart"
                )
            )
        )
    except Exception as e:
        print(f"Ошибка при отправке сообщения пользователю: {e}")
    await call.answer("✅ Товар добавлен! Перейдите в бота для оформления заказа.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "no_products")
async def no_products_callback(call: types.CallbackQuery):
    await call.answer("📭 В хозяйстве пока нет товаров", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "no_active_orders")
async def no_active_orders_callback(call: types.CallbackQuery):
    await call.answer("📭 Нет активных заказов", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "go_home")
async def go_home_callback(call: types.CallbackQuery):
    await call.message.edit_reply_markup(None)
    await cmd_start(call.message)

@dp.callback_query_handler(lambda c: c.data == "view_categories")
async def callback_view_categories(call: types.CallbackQuery):
    await call.message.edit_reply_markup(None)
    await show_catalog(call.message)

@dp.callback_query_handler(lambda c: c.data == "go_to_cart")
async def go_to_cart_callback(call: types.CallbackQuery):
    user_id = str(call.from_user.id)
    cart = user_carts.get(user_id, [])
    if not cart:
        await call.answer("🛒 Ваша корзина пуста.", show_alert=True)
        return
    await show_cart(call.message)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "admin_panel")
async def admin_panel_callback(call: types.CallbackQuery):
    if not is_admin(call.from_user.id):
        await call.answer("❌ Недостаточно прав")
        return
    await call.message.edit_reply_markup(None)
    await call.message.answer(
        "👑 Панель администратора\n\n"
        "Вы в режиме администратора. Используйте кнопки ниже для управления хозяйством.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )

# ==================== ОТЗЫВЫ О ТОВАРАХ ====================
@dp.callback_query_handler(lambda c: c.data.startswith('write_review_'))
async def write_review_start(call: types.CallbackQuery, state: FSMContext):
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    await state.update_data(product_id=product_id)
    await ReviewState.rating.set()
    await call.message.answer(
        f"✍️ Оставьте отзыв о товаре\n\n"
        f"📦 {product.get('subcategory', '')}\n\n"
        f"Выберите оценку:",
        parse_mode="HTML",
        reply_markup=get_rating_keyboard(product_id)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('rating_') and c.data != 'cancel_review')
async def process_rating(call: types.CallbackQuery, state: FSMContext):
    parts = call.data.split('_')
    rating = parts[1]
    product_id = parts[3] if len(parts) > 3 else parts[2]
    
    await state.update_data(rating=rating)
    await ReviewState.text.set()
    await call.message.answer(
        f"⭐️ Вы выбрали оценку: {rating}\n\n"
        f"Напишите ваш отзыв (или отправьте /skip чтобы пропустить):",
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_review"))
    )
    await call.answer()

@dp.message_handler(state=ReviewState.text)
async def process_review_text(message: types.Message, state: FSMContext):
    if message.text == "/skip":
        review_text = ""
    else:
        review_text = message.text.strip()
        if len(review_text) > 500:
            await message.answer("❌ Отзыв слишком длинный (максимум 500 символов). Напишите короче:")
            return
    
    data = await state.get_data()
    product_id = data.get('product_id')
    rating = data.get('rating')
    
    if not product_id or not rating:
        await message.answer("❌ Ошибка при сохранении отзыва. Попробуйте снова.")
        await state.finish()
        return
    
    # Сохраняем отзыв
    if product_id not in reviews_db:
        reviews_db[product_id] = []
    
    review = {
        'user_id': str(message.from_user.id),
        'username': message.from_user.username or f"user_{message.from_user.id}",
        'rating': int(rating),
        'text': review_text,
        'date': datetime.now().strftime("%d.%m.%Y %H:%M")
    }
    reviews_db[product_id].append(review)
    save_data()
    
    await message.answer(
        f"✅ Спасибо за ваш отзыв!\n\n"
        f"Ваша оценка: {'⭐️' * int(rating)}\n"
        f"Отзыв опубликован и будет виден другим покупателям.",
        parse_mode="HTML",
        reply_markup=get_back_to_reviews_keyboard(product_id)
    )
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('read_reviews_'))
async def read_reviews(call: types.CallbackQuery):
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    reviews = reviews_db.get(product_id, [])
    if not reviews:
        await call.message.answer(
            f"📖 Отзывы о товаре\n\n"
            f"📦 {product.get('subcategory', '')}\n\n"
            f"📭 Пока нет отзывов. Будьте первым!",
            parse_mode="HTML",
            reply_markup=get_review_keyboard(product_id)
        )
        await call.answer()
        return
    
    # Считаем средний рейтинг
    avg_rating = sum(r['rating'] for r in reviews) / len(reviews)
    
    reviews_text = f"📖 Отзывы о товаре\n\n"
    reviews_text += f"📦 {product.get('subcategory', '')}\n"
    reviews_text += f"⭐️ Средний рейтинг: {avg_rating:.1f} из 5 ({len(reviews)} отзывов)\n\n"
    reviews_text += "─" * 30 + "\n\n"
    
    # Показываем последние 10 отзывов
    for review in reviews[-10:][::-1]:
        stars = '⭐️' * review['rating']
        reviews_text += f"{stars} ({review['rating']}/5)\n"
        reviews_text += f"👤 @{review['username']}, {review['date']}\n"
        if review.get('text'):
            reviews_text += f"💬 {review['text']}\n"
        reviews_text += "\n" + "─" * 20 + "\n\n"
    
    await call.message.answer(
        reviews_text,
        parse_mode="HTML",
        reply_markup=get_review_keyboard(product_id)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_review")
async def cancel_review(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await call.message.answer("❌ Отзыв отменен")
    await call.answer()

# ==================== ВЫБОР ВЕСА ЦЫПЛЕНКА ====================
@dp.callback_query_handler(lambda c: c.data.startswith('select_weight_'))
async def select_weight_start(call: types.CallbackQuery, state: FSMContext):
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    if product.get('subcategory') != "🐓 Цыпленок бройлер":
        await call.answer("❌ Выбор по весу доступен только для цыпленка бройлера", show_alert=True)
        return
    
    # Проверяем наличие доступных тушек
    available = [p for p in individual_products_db.values() 
                 if p.get('product_id') == product_id and p.get('status') == 'available']
    
    if not available:
        await call.answer("❌ К сожалению, сейчас нет доступных тушек для выбора", show_alert=True)
        return
    
    await state.update_data(product_id=product_id)
    await SelectWeightState.weight_category.set()
    await call.message.answer(
        f"🐔 Выберите желаемый вес цыпленка\n\n"
        f"📦 Товар: {product.get('subcategory')}\n"
        f"💰 Цена: {product.get('price', 0)} руб./кг\n\n"
        f"В наличии: {len(available)} шт.\n\n"
        f"Выберите весовую категорию:",
        parse_mode="HTML",
        reply_markup=get_weight_category_keyboard(product_id)
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('weight_'))
async def process_weight_selection(call: types.CallbackQuery, state: FSMContext):
    parts = call.data.split('_')
    product_id = parts[-1]
    
    # Определяем весовую категорию
    if '2.0_2.5' in call.data:
        min_weight, max_weight = 2.0, 2.5
        category_name = "2.0-2.5 кг"
    elif '2.6_3.0' in call.data:
        min_weight, max_weight = 2.6, 3.0
        category_name = "2.6-3.0 кг"
    elif '3.0+' in call.data:
        min_weight, max_weight = 3.0, 999
        category_name = "3.0+ кг"
    else:
        await call.answer("❌ Неверная категория веса", show_alert=True)
        return
    
    # Ищем доступную тушку в этой категории
    available = [p for p in individual_products_db.values() 
                 if p.get('product_id') == product_id and 
                 p.get('status') == 'available' and
                 min_weight <= p.get('weight', 0) <= max_weight]
    
    if not available:
        await call.answer("❌ К сожалению, в этой категории сейчас нет доступных тушек", show_alert=True)
        return
    
    # Выбираем первую доступную (можно улучшить логику выбора)
    selected = available[0]
    
    async with state.proxy() as data:
        data['weight_category'] = category_name
        data['selected_product'] = selected['id']
    
    await SelectWeightState.product_id.set()
    
    weight = selected.get('weight', 0)
    price_per_kg = products_db.get(product_id, {}).get('price', 0)
    total_price = weight * price_per_kg
    
    await call.message.answer(
        f"✅ Тушка забронирована!\n\n"
        f"🐓 Цыпленок бройлер\n"
        f"⚖️ Вес: {weight} кг\n"
        f"💰 Цена за кг: {price_per_kg} руб.\n"
        f"💰 Итого: ~{total_price:.0f} руб.\n\n"
        f"📦 Категория: {category_name}\n\n"
        f"Теперь добавьте товар в корзину обычным способом.\n"
        f"Вес забронированной тушки будет указан в заказе.",
        parse_mode="HTML"
    )
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel_weight")
async def cancel_weight_selection(call: types.CallbackQuery, state: FSMContext):
    await state.finish()
    await call.message.answer("❌ Выбор веса отменен")
    await call.answer()

# ==================== ЗАПУСК БОТА ====================
async def on_startup(dp):
    """Действия при запуске"""
    print("=" * 50)
    print("🤖 БОТ СЕМЕЙНОЙ ФЕРМЫ РУССКИЙ ТАЙ")
    print("=" * 50)
    
    # ===== ПОДКЛЮЧЕНИЕ К MONGODB =====
    print("🔌 Подключение к MongoDB...")
    
    # Проверяем, есть ли переменная окружения
    if not MONGODB_URI or 'YourPassword' in MONGODB_URI:
        print("⚠️ MONGODB_URI не настроен! Используем локальный режим...")
        print("   Для облачной базы настройте переменную MONGODB_URI на Bothost")
        print("   Инструкция в файле .env.example")
    else:
        if connect_to_mongodb():
            print("✅ MongoDB подключена!")
            load_data()
        else:
            print("⚠️ Не удалось подключиться к MongoDB. Работаем без базы данных.")
            print("   Проверьте MONGODB_URI в переменных окружения Bothost")
    # ==================================
    
    # ===== ПРИНУДИТЕЛЬНЫЙ СБРОС ВЕБХУКА =====
    print("\n🔄 Сбрасываем вебхук и очищаем обновления...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await asyncio.sleep(2)
        print("✅ Вебхук сброшен")
    except Exception as e:
        print(f"⚠️ Ошибка при сбросе вебхука: {e}")

    # Проверяем, что вебхук действительно удален
    try:
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            print(f"⚠️ Был установлен вебхук на {webhook_info.url}, но теперь удален")
        else:
            print("✅ Вебхуков не обнаружено")
    except Exception as e:
        print(f"⚠️ Ошибка проверки вебхука: {e}")
    # =========================================

    # ===== АВТОУДАЛЕНИЕ ПРИ ЗАПУСКЕ (152-ФЗ) =====
    try:
        await auto_delete_old_orders(days=30)
        asyncio.create_task(schedule_daily_cleanup())
        print("✅ Автоудаление старых заказов настроено")
    except Exception as e:
        print(f"⚠️ Ошибка настройки автоудаления: {e}")
    # =============================================

    # ===== ЗАПУСК ПРОВЕРКИ ЗАМОРОЗКИ МЯСА =====
    try:
        asyncio.create_task(start_freeze_checker())
        print("⏰ Запущена проверка заморозки мяса (48 часов)")
    except Exception as e:
        print(f"⚠️ Ошибка запуска проверки заморозки: {e}")
    # ==========================================

    print("\n" + "=" * 50)
    print(f"👑 Владелец: {OWNER_ID}")
    print(f"👥 Администраторов: {len(admins_db)}")
    print("=" * 50)
    print("✅ БЕЗОПАСНОСТЬ АКТИВИРОВАНА:")
    print("   • Токен через переменные окружения")
    print("   • Защита от флуда (rate limiting)")
    print("   • Валидация всех вводимых данных")
    print("   • Автоблокировка при чрезмерном флуде")
    print("   • Санитизация логов (user_id скрыты)")
    print("   • Автоудаление персональных данных (152-ФЗ)")
    print("   • Система управления администраторами")
    if db is not None:
        print("   • MongoDB Atlas (облачная база)")
    else:
        print("   • Локальный режим (без MongoDB)")
    print("=" * 50)

    try:
        me = await bot.get_me()
        print(f"✅ Бот запущен: @{me.username}")
        await bot.send_message(
            OWNER_ID,
            "🤖 <b>Бот семейной фермы Русский ТАЙ запущен!</b>\n\n"
            "🛡️ <b>Защита активирована:</b>\n"
            "• Токен в переменных окружениях\n"
            "• Защита от флуда (1 сек/сообщение)\n"
            "• Автоблокировка флудеров на 5 мин\n"
            "• Валидация всех данных\n"
            "• Автоудаление заказов через 30 дней (152-ФЗ)\n"
            f"• Администраторов: {len(admins_db)}",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    from aiogram import executor
    import asyncio
    
    print("🚀 Запуск бота...")
    
    # Создаём event loop для Python 3.14
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    except Exception:
        pass

    try:
        executor.start_polling(
            dp,
            skip_updates=True,
            on_startup=on_startup
        )
    except KeyboardInterrupt:
        print("\n🛑 Бот остановлен")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
