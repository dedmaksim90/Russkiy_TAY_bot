import asyncio
import logging
import json
import uuid
import random
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton
)

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = "8595769512:AAGt130PyN2rKm7fnmXRyBOWHKHeaNGEG8g"  
ADMIN_ID = 439446887
CHANNEL_ID = "@test_shop654"
DELIVERY_COST = 300  # Стоимость доставки
FREE_DELIVERY_THRESHOLD = 2000  # Бесплатная доставка от этой суммы
PICKUP_ADDRESS = "Нижний Новгород ул. Профинтерна д.26"

# Контактные телефоны
CONTACT_PHONES = "+79506111165 Ирина и +79200783330 Сергей"

# Средние веса для мяса (в кг)
MEAT_AVERAGE_WEIGHTS = {
    "🐓 Цыпленок бройлер": 2.5,  # ~2 кг
    "🐔 Молодой петушок": 1,   # ~2.5 кг
    "👑 Цесарка": 1.4,         # ~1.8 кг
    "🐦 Перепелка": 0.2          # ~200 г
}

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

# ==================== СТРУКТУРА КАТЕГОРИЙ ====================
CATEGORIES = {
    "🥚 Яйцо": {
        "name": "🥚 Яйцо",
        "subcategories": ["🐔 Куриное", "🐦 Перепелиное", "👑 Цесариное"],
        "unit": "шт",  # Единица измерения
        "multiplier": {  # Кратность упаковки
            "🐔 Куриное": 10,
            "🐦 Перепелиное": 20,
            "👑 Цесариное": 10
        },
        "exact_price": True  # Точная цена
    },
    "🍗 Мясо": {
        "name": "🍗 Мясо", 
        "subcategories": ["🐓 Цыпленок бройлер", "🐔 Молодой петушок", "👑 Цесарка", "🐦 Перепелка"],
        "unit": "шт",  # Теперь в штуках тушки
        "price_per_kg": True,  # Цена указана за кг
        "average_weight": MEAT_AVERAGE_WEIGHTS,  # Средний вес тушки
        "exact_price": False  # Ориентировочная цена
    },
    "🥫 Полуфабрикаты": {
        "name": "🥫 Полуфабрикаты",
        "subcategories": ["🌭 Колбаса", "🥩 Тушенка"],
        "unit": "кг",
        "price_per_kg": True,  # Цена указана за кг
        "exact_price": False  # Ориентировочная цена
    }
}

# ==================== БАЗА ДАННЫХ ====================
products_db = {}
orders_db = {}
user_carts = {}
notifications_db = {}  # Уведомления о появлении товара
product_views_db = {}  # Статистика просмотров товаров
order_return_items_db = {}  # Возвращенные товары после отмены заказа
manual_add_requests_db = {}  # Статистика запросов на добавление вручную
user_stats_db = {}  # Статистика по клиентам

# ==================== ФУНКЦИИ ДЛЯ СОХРАНЕНИЯ ДАННЫХ ====================
def save_data():
    """Сохранить данные в файл"""
    data = {
        'products': products_db,
        'orders': orders_db,
        'carts': user_carts,
        'notifications': notifications_db,
        'product_views': product_views_db,
        'order_return_items': order_return_items_db,
        'manual_add_requests': manual_add_requests_db,
        'user_stats': user_stats_db
    }
    with open('shop_data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

def load_data():
    """Загрузить данные из файла"""
    global products_db, orders_db, user_carts, notifications_db, product_views_db, order_return_items_db, manual_add_requests_db, user_stats_db
    try:
        with open('shop_data.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            products_db = data.get('products', {})
            orders_db = data.get('orders', {})
            user_carts = data.get('carts', {})
            notifications_db = data.get('notifications', {})
            product_views_db = data.get('product_views', {})
            order_return_items_db = data.get('order_return_items', {})
            manual_add_requests_db = data.get('manual_add_requests', {})
            user_stats_db = data.get('user_stats', {})
    except FileNotFoundError:
        products_db = {}
        orders_db = {}
        user_carts = {}
        notifications_db = {}
        product_views_db = {}
        order_return_items_db = {}
        manual_add_requests_db = {}
        user_stats_db = {}

def increment_product_view(product_id: str):
    """Увеличить счетчик просмотров товара"""
    today = str(date.today())
    
    if today not in product_views_db:
        product_views_db[today] = {}
    
    if product_id not in product_views_db[today]:
        product_views_db[today][product_id] = 0
    
    product_views_db[today][product_id] += 1
    save_data()

def increment_manual_add_request(product_id: str):
    """Увеличить счетчик запросов на добавление вручную"""
    today = str(date.today())
    
    if today not in manual_add_requests_db:
        manual_add_requests_db[today] = {}
    
    if product_id not in manual_add_requests_db[today]:
        manual_add_requests_db[today][product_id] = 0
    
    manual_add_requests_db[today][product_id] += 1
    save_data()

def update_user_stats(user_id: str, order_data: dict, status_change: str = None):
    """Обновить статистику пользователя"""
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
    
    # Обновляем даты
    current_date = datetime.now().strftime("%d.%m.%Y %H:%M")
    if not stats.get('first_order_date'):
        stats['first_order_date'] = current_date
    stats['last_order_date'] = current_date
    
    save_data()

def get_product_views_stats(days: int = 7) -> dict:
    """Получить статистику просмотров за последние дни"""
    stats = {}
    today = date.today()
    
    for i in range(days):
        day = str(today - timedelta(days=i))
        if day in product_views_db:
            for product_id, views in product_views_db[day].items():
                if product_id not in stats:
                    stats[product_id] = 0
                stats[product_id] += views
    
    return stats

def get_manual_add_stats(days: int = 7) -> dict:
    """Получить статистику запросов на добавление вручную"""
    stats = {}
    today = date.today()
    
    for i in range(days):
        day = str(today - timedelta(days=i))
        if day in manual_add_requests_db:
            for product_id, requests in manual_add_requests_db[day].items():
                if product_id not in stats:
                    stats[product_id] = 0
                stats[product_id] += requests
    
    return stats

def get_random_thank_you_message() -> str:
    """Получить случайное благодарственное сообщение"""
    return random.choice(THANK_YOU_MESSAGES)

# ==================== НАСТРОЙКА БОТА ====================
bot = Bot(token=BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
logging.basicConfig(level=logging.INFO)

# ==================== СОСТОЯНИЯ ====================
class AddProduct(StatesGroup):
    category = State()
    subcategory = State()
    price = State()
    quantity = State()  # Остаток
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

class AdjustQuantityState(StatesGroup):
    product_id = State()
    quantity = State()

class AdjustStockState(StatesGroup):  # НОВОЕ состояние для изменения остатков на складе
    product_id = State()
    quantity = State()

class ManualAddToCartState(StatesGroup):
    product_id = State()
    quantity = State()

class PostponeOrderState(StatesGroup):
    order_id = State()
    new_date = State()

# ==================== КЛАВИАТУРЫ ====================
def get_main_keyboard(is_admin=False):
    """Клавиатура для покупателей"""
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
    
    if is_admin:
        keyboard.add(KeyboardButton("👑 Панель админа"))
    
    return keyboard

def get_start_keyboard(is_admin=False):
    """Клавиатура для первого входа (с кнопкой начала выбора товаров)"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    
    # Основная кнопка для начала выбора товаров
    keyboard.add(KeyboardButton("🛍️ Начнем выбирать полезный продукт!"))
    
    if is_admin:
        keyboard.add(KeyboardButton("👑 Панель админа"))
    
    return keyboard

def get_admin_keyboard():
    """Клавиатура для администратора"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    keyboard.add(
        KeyboardButton("➕ Добавить товар"),
        KeyboardButton("📦 Пополнить остатки"),
        KeyboardButton("📤 Опубликовать в канал")
    )
    keyboard.add(
        KeyboardButton("✏️ Управление товарами"),
        KeyboardButton("📊 Статистика"),
        KeyboardButton("📈 Аналитика"),
        KeyboardButton("📋 Активные заказы"),
        KeyboardButton("👥 Клиенты"),
        KeyboardButton("👤 Режим покупателя"),
        KeyboardButton("🏠 В начало")
    )
    return keyboard

def get_categories_keyboard(is_admin=False):
    """Клавиатура с категориями"""
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for category in CATEGORIES.keys():
        keyboard.add(KeyboardButton(category))
    if is_admin:
        keyboard.add(KeyboardButton("↩️ Назад"), KeyboardButton("👑 Панель админа"))
    else:
        keyboard.add(KeyboardButton("↩️ Назад"), KeyboardButton("🏠 В начало"))
    return keyboard

def get_subcategories_keyboard(category_name: str, is_admin=False):
    """Клавиатура с рубриками (только названия рубрик)"""
    category = CATEGORIES.get(category_name)
    if not category:
        return get_categories_keyboard(is_admin)
    
    keyboard = ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    for subcat in category["subcategories"]:
        keyboard.add(KeyboardButton(subcat))  # ИСПРАВЛЕНО: показываем только рубрику без категории
    
    if is_admin:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("👑 Панель админа"))
    else:
        keyboard.add(KeyboardButton("↩️ К категориям"), KeyboardButton("🏠 В начало"))
    
    return keyboard

def get_product_keyboard(product_id: str, product_data: dict, show_cart_button: bool = False, is_admin: bool = False):
    """Кнопки для товара"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    if is_admin:
        # Для админа показываем кнопки управления товаром
        keyboard.add(
            InlineKeyboardButton("📦 Изменить остаток", callback_data=f"adjust_{product_id}"),
            InlineKeyboardButton("✏️ Редактировать товар", callback_data=f"edit_{product_id}"),
            InlineKeyboardButton("🗑️ Удалить товар", callback_data=f"delete_{product_id}"),
            InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
        )
    else:
        # Для покупателей
        if product_data.get('quantity', 0) > 0:
            keyboard.add(
                InlineKeyboardButton("🛒 В корзину", callback_data=f"add_{product_id}"),
                InlineKeyboardButton("📝 Добавить вручную", callback_data=f"manual_add_{product_id}")
            )
        else:
            keyboard.add(
                InlineKeyboardButton("🔔 Уведомить о появлении", callback_data=f"notify_{product_id}")
            )
        
        # Кнопка быстрого перехода в корзину
        if show_cart_button:
            keyboard.add(InlineKeyboardButton("🛒 Перейти в корзину", callback_data="go_to_cart"))
    
    return keyboard

def get_cart_keyboard(cart_items):
    """Клавиатура для корзины"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Оформить заказ", callback_data="checkout"),
        InlineKeyboardButton("🗑️ Очистить корзину", callback_data="clear_cart")
    )
    
    # Кнопки для изменения количества
    for i, item in enumerate(cart_items):
        product_id = item['id']
        product_name = item['name'][:20] + "..." if len(item['name']) > 20 else item['name']
        keyboard.add(
            InlineKeyboardButton(f"➕ {product_name}", callback_data=f"inc_{product_id}"),
            InlineKeyboardButton(f"➖ {product_name}", callback_data=f"dec_{product_id}"),
            InlineKeyboardButton(f"📝 Изменить количество", callback_data=f"change_{product_id}")
        )
    
    # Добавляем кнопки "К категориям" и "В начало"
    keyboard.add(
        InlineKeyboardButton("🛍️ К категориям", callback_data="view_categories"),
        InlineKeyboardButton("🏠 В начало", callback_data="go_home")
    )
    
    return keyboard

def get_delivery_keyboard():
    """Клавиатура для выбора способа получения"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🚗 Самовывоз", callback_data="pickup"),
        InlineKeyboardButton("🚚 Доставка", callback_data="delivery")
    )
    return keyboard

def get_back_to_catalog_keyboard(is_admin=False):
    """Клавиатура для возврата в каталог"""
    keyboard = InlineKeyboardMarkup()
    keyboard.add(InlineKeyboardButton("🛍️ Вернуться в каталог", callback_data="back_to_catalog"))
    if is_admin:
        keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    else:
        keyboard.add(InlineKeyboardButton("🏠 В начало", callback_data="go_home"))
    return keyboard

def get_products_for_adjust_keyboard():
    """Клавиатура с товарами для изменения остатков"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Группируем товары по категориям и подкатегориям
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        
        if key not in products_by_category:
            products_by_category[key] = product  # Берем только первый товар из каждой рубрики
    
    # Создаем кнопки для каждой рубрики
    for key, product in products_by_category.items():
        # ИСПРАВЛЕНО: показываем только название рубрики в кнопке
        product_name = f"{product.get('subcategory', '')}"
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

def get_order_management_keyboard(order_id: str):
    """Клавиатура для управления заказом"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # Основные действия
    keyboard.add(
        InlineKeyboardButton("✅ Заказ выдан", callback_data=f"complete_{order_id}"),
        InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_{order_id}")
    )
    
    # Дополнительные действия
    keyboard.add(
        InlineKeyboardButton("⏰ Перенести", callback_data=f"postpone_{order_id}"),
        InlineKeyboardButton("📊 Статистика клиента", callback_data=f"client_stats_{order_id}")
    )
    
    keyboard.add(InlineKeyboardButton("🔙 Назад к списку", callback_data="back_to_orders_list"))
    
    return keyboard

def get_order_confirmation_keyboard(order_id: str):
    """Клавиатура для подтверждения заказа"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    keyboard.add(
        InlineKeyboardButton("✅ Подтвердить заказ", callback_data=f"confirm_{order_id}"),
        InlineKeyboardButton("❌ Отклонить", callback_data=f"reject_{order_id}")
    )
    
    keyboard.add(
        InlineKeyboardButton("📞 Связаться", callback_data=f"contact_{order_id}")
    )
    
    return keyboard

def get_client_orders_keyboard(user_id: str):
    """Клавиатура для просмотра заказов клиента"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Ищем заказы клиента
    client_orders = [order for order_id, order in orders_db.items() 
                    if order.get('user_id') == user_id]
    
    # Сортируем по дате (новые первые)
    client_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    
    for order in client_orders[:5]:  # Показываем последние 5 заказов
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

def get_active_orders_keyboard():
    """Клавиатура для активных заказов"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # Получаем активные заказы
    active = [o for o in orders_db.values() if o.get('status') in ['🆕 Новый', '✅ Подтвержден']]
    
    if not active:
        keyboard.add(InlineKeyboardButton("📭 Нет активных заказов", callback_data="no_active_orders"))
        keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
        return keyboard
    
    # Сортируем: сначала новые, потом подтвержденные
    new_orders = sorted([o for o in active if o.get('status') == '🆕 Новый'], 
                       key=lambda x: x.get('created_at', ''), reverse=True)
    confirmed_orders = sorted([o for o in active if o.get('status') == '✅ Подтвержден'],
                            key=lambda x: x.get('created_at', ''), reverse=True)
    
    # Ограничиваем количество показываемых заказов
    max_orders = 10
    all_orders = new_orders + confirmed_orders
    orders_to_show = all_orders[:max_orders]
    
    for order in orders_to_show:
        order_id = order['id']
        status = order.get('status', '')
        total = order.get('total', 0)
        
        # Иконка статуса
        if status == '🆕 Новый':
            status_icon = "🆕"
        elif status == '✅ Подтвержден':
            status_icon = "✅"
        else:
            status_icon = "📦"
        
        # Создаем кнопку для заказа
        button_text = f"{status_icon} Заказ #{order_id} - {total} руб."
        keyboard.add(
            InlineKeyboardButton(
                button_text,
                callback_data=f"manage_order_{order_id}"
            )
        )
    
    # Кнопка обновления
    keyboard.add(InlineKeyboardButton("🔄 Обновить список", callback_data="refresh_active_orders"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    
    return keyboard

def get_product_management_keyboard():
    """Клавиатура для управления товарами"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    
    # Группируем товары по категориям и подкатегориям
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        
        if key not in products_by_category:
            products_by_category[key] = product  # Берем только первый товар из каждой рубрики
    
    # Создаем кнопки для каждой рубрики
    for key, product in products_by_category.items():
        # ИСПРАВЛЕНО: показываем только название рубрики в кнопке
        product_name = f"{product.get('subcategory', '')}"
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        
        keyboard.add(InlineKeyboardButton(
            f"✏️ {product_name} ({product.get('price', 0)} руб.)",
            callback_data=f"edit_{product['id']}"
        ))
    
    if not products_by_category:
        keyboard.add(InlineKeyboardButton("📭 Нет товаров для редактирования", callback_data="no_products"))
    
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    
    return keyboard

def get_edit_product_keyboard(product_id: str):
    """Клавиатура для редактирования конкретного товара"""
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

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def show_large_notification(call: types.CallbackQuery, message: str, duration: int = 2):
    """Показать крупное уведомление посередине экрана"""
    try:
        # Отправляем уведомление как отдельное сообщение
        notification = await call.message.answer(
            f"<b>{message}</b>",
            parse_mode="HTML"
        )
        
        # Ждем указанное время
        await asyncio.sleep(duration)
        
        # Удаляем уведомление
        await notification.delete()
        
    except Exception as e:
        print(f"Ошибка при показе уведомления: {e}")

def get_category_info(category_name: str, subcategory_name: str) -> dict:
    """Получить информацию о категории и субкатегории"""
    category = CATEGORIES.get(category_name)
    if not category:
        return {}
    
    info = {
        'unit': category.get('unit', 'шт'),
        'multiplier': category.get('multiplier', {}).get(subcategory_name, 1),
        'price_per_kg': category.get('price_per_kg', False),
        'average_weight': category.get('average_weight', {}).get(subcategory_name, 0),
        'exact_price': category.get('exact_price', False)
    }
    return info

def calculate_product_price(product_data: dict, quantity: int = 1) -> tuple:
    """Рассчитать цену товара с учетом кратности и среднего веса"""
    price = product_data.get('price', 0)
    category = CATEGORIES.get(product_data.get('category', ''))
    subcategory = product_data.get('subcategory', '')
    
    if not category:
        return 0, ""
    
    # Для яиц: цена * кратность * количество
    if category.get('unit') == 'шт' and not category.get('price_per_kg'):
        multiplier = category.get('multiplier', {}).get(subcategory, 1)
        total_price = price * multiplier * quantity
        return total_price, f"({multiplier} шт. × {price} руб./шт.)"
    
    # Для мяса: цена за кг * средний вес * количество
    elif category.get('price_per_kg') and category.get('average_weight'):
        avg_weight = category.get('average_weight', {}).get(subcategory, 0)
        if avg_weight > 0:
            estimated_price = price * avg_weight * quantity
            note = f"*≈{estimated_price:.0f} руб. за {quantity} шт. (средний вес {avg_weight} кг)"
            return estimated_price, note
    
    # Для других товаров с ценой за кг
    elif category.get('price_per_kg'):
        return 0, "*Итоговая цена будет рассчитана при получении"
    
    return 0, ""

def format_product_info(product_data: dict) -> str:
    """Форматировать информацию о товаре"""
    # ИСПРАВЛЕНО: показываем только название рубрики в заголовке
    info = f"<b>{product_data.get('subcategory', '')}</b>\n"
    
    category_info = get_category_info(product_data.get('category', ''), product_data.get('subcategory', ''))
    
    if category_info.get('price_per_kg') and category_info.get('average_weight', 0) > 0:
        # Для мяса с известным средним весом
        avg_weight = category_info.get('average_weight', 0)
        price_per_kg = product_data.get('price', 0)
        estimated_price = price_per_kg * avg_weight
        
        info += f"💰 <b>Цена:</b> {price_per_kg} руб./кг\n"
        info += f"📦 <b>Средний вес тушки:</b> ~{avg_weight} кг\n"
        info += f"💰 <b>Примерная цена за тушку*:</b> ~{estimated_price:.0f} руб.\n\n"
        info += f"<i>*Расчетная цена. Итоговая стоимость зависит от фактического веса тушки.</i>\n\n"
        
    elif category_info.get('price_per_kg'):
        # Для других товаров с ценой за кг
        info += f"💰 <b>Цена:</b> {product_data.get('price', 0)} руб./кг\n"
        info += f"<i>*Итоговая стоимость будет рассчитана при получении заказа</i>\n\n"
    else:
        # Для яиц
        multiplier = category_info.get('multiplier', 1)
        price_per_unit = product_data.get('price', 0)
        total_price = price_per_unit * multiplier
        
        info += f"💰 <b>Цена за упаковку ({multiplier} шт):</b> {total_price} руб.\n"
        info += f"💰 <b>Цена за 1 шт:</b> {price_per_unit} руб.\n\n"
    
    info += f"📦 <b>Остаток:</b> {product_data.get('quantity', 0)} "
    info += f"{category_info.get('unit', 'шт')}\n"
    
    if product_data.get('created_at'):
        info += f"\n📅 Добавлено: {product_data.get('created_at')}"
    
    return info

def format_order_info(order_data: dict) -> str:
    """Форматировать информацию о заказе"""
    order = order_data
    text = f"<b>Заказ #{order['id']}</b>\n"
    text += f"📅 <b>Создан:</b> {order.get('created_at', 'Не указана')}\n"
    text += f"📦 <b>Статус:</b> {order.get('status', 'Не указан')}\n"
    
    if order.get('status_updated_at'):
        text += f"🕒 <b>Статус обновлен:</b> {order.get('status_updated_at')}\n"
    
    # Способ получения
    if order.get('delivery_method') == 'pickup':
        text += f"🚗 <b>Способ:</b> Самовывоз\n"
        text += f"📍 <b>Адрес:</b> {PICKUP_ADDRESS}\n"
    else:
        text += f"🚚 <b>Способ:</b> Доставка\n"
        text += f"📍 <b>Адрес:</b> {order.get('address', 'Не указан')}\n"
    
    # Покупатель
    text += f"👤 <b>Покупатель:</b> @{order.get('username', 'без username')}\n"
    text += f"🆔 <b>ID пользователя:</b> {order.get('user_id', 'Не указан')}\n\n"
    
    # Товары
    text += "<b>Состав заказа:</b>\n"
    for item in order.get('items', []):
        if item.get('price_per_kg'):
            if item.get('average_weight', 0) > 0:
                item_price = item['price'] * item['average_weight'] * item['quantity']
                text += f"• {item['name']} - {item['quantity']} шт.\n"
                text += f"  Примерная стоимость: ~{item_price:.0f} руб.\n"
            else:
                text += f"• {item['name']} - {item['quantity']} кг\n"
        else:
            item_price = item['price'] * CATEGORIES.get(item['name'].split('|')[0].strip(), {}).get('multiplier', {}).get(item['name'].split('|')[-1].strip(), 1) * item['quantity']
            text += f"• {item['name']} - {item['quantity']} уп.\n"
            text += f"  Сумма: {item_price} руб.\n"
    
    text += f"\n💰 <b>Итого:</b> {order.get('total', 0)} руб.\n"
    
    return text

def format_client_stats(user_id: str) -> str:
    """Форматировать статистику клиента"""
    if user_id not in user_stats_db:
        return "📭 Статистика по клиенту не найдена."
    
    stats = user_stats_db[user_id]
    
    text = f"📊 <b>Статистика клиента</b>\n\n"
    text += f"👤 <b>Username:</b> @{stats.get('username', 'не указан')}\n"
    text += f"🆔 <b>ID:</b> {user_id}\n\n"
    
    text += f"📈 <b>Общая статистика:</b>\n"
    text += f"• Всего заказов: {stats.get('total_orders', 0)}\n"
    text += f"• Выполнено: {stats.get('completed_orders', 0)}\n"
    text += f"• Отменено: {stats.get('canceled_orders', 0)}\n"
    text += f"• Перенесено: {stats.get('postponed_orders', 0)}\n"
    text += f"• Всего потрачено: {stats.get('total_spent', 0)} руб.\n\n"
    
    if stats.get('first_order_date'):
        text += f"📅 <b>Первый заказ:</b> {stats.get('first_order_date')}\n"
    
    if stats.get('last_order_date'):
        text += f"📅 <b>Последний заказ:</b> {stats.get('last_order_date')}\n"
    
    if stats.get('last_status_change'):
        text += f"🕒 <b>Последнее изменение статуса:</b> {stats.get('last_status_change')}\n"
    
    return text

# ==================== ОСНОВНЫЕ КОМАНДЫ ====================
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    """Команда /start с кнопкой начала выбора товаров"""
    if message.from_user.id == ADMIN_ID:
        await message.answer(
            "👑 <b>Панель администратора</b>\n\n"
            "Вы в режиме администратора. Используйте кнопки ниже для управления хозяйством.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
    else:
        await message.answer(
            "🏡 <b>Добро пожаловать в Русский ТАЙ!</b>\n\n"
            "Семейная ферма в экологически чистом месте Керженского заповедника.\n\n"
            "Наши продукты - это забота о вашем здоровье и качестве жизни!",
            parse_mode="HTML",
            reply_markup=get_start_keyboard()
        )

@dp.message_handler(text="🛍️ Начнем выбирать полезный продукт!")
async def start_shopping(message: types.Message):
    """Начать выбор товаров (заменяет старую команду старт)"""
    is_admin = (message.from_user.id == ADMIN_ID)
    await message.answer(
        "🛍️ <b>Отлично! Давайте выберем самые полезные и свежие продукты!</b>\n\n"
        "Выберите действие:",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(is_admin=is_admin)
    )

# ==================== ПЕРЕКЛЮЧЕНИЕ РЕЖИМОВ ====================
@dp.message_handler(text="🏠 В начало")
async def go_to_home(message: types.Message):
    """Быстрый переход в начало"""
    await cmd_start(message)

@dp.callback_query_handler(lambda c: c.data == "go_home")
async def go_home_callback(call: types.CallbackQuery):
    """Быстрый переход в начало из callback"""
    await call.message.edit_reply_markup(None)
    await cmd_start(call.message)

@dp.message_handler(text="👑 Панель админа")
async def switch_to_admin_mode(message: types.Message):
    """Переключиться в режим администратора"""
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ У вас нет прав администратора.")
        return
    
    await message.answer(
        "👑 <b>Переключение в режим администратора</b>\n\n"
        "Теперь вы можете управлять хозяйством.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )

@dp.message_handler(text="👤 Режим покупателя")
async def switch_to_user_mode(message: types.Message):
    """Переключиться в режим покупателя"""
    if message.from_user.id != ADMIN_ID:
        return
    
    await message.answer(
        "👤 <b>Переключение в режим покупателя</b>\n\n"
        "Теперь вы видите интерфейс как обычный покупатель.",
        parse_mode="HTML",
        reply_markup=get_main_keyboard(is_admin=False)
    )

@dp.callback_query_handler(lambda c: c.data == "admin_panel")
async def admin_panel_callback(call: types.CallbackQuery):
    """Переход в панель админа из callback"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав")
        return
    
    await call.message.edit_reply_markup(None)
    await call.message.answer(
        "👑 <b>Панель администратора</b>\n\n"
        "Вы в режиме администратора. Используйте кнопки ниже для управления хозяйством.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )

@dp.message_handler(text="🔙 На главную")
async def go_to_main(message: types.Message):
    """Вернуться на главную"""
    await cmd_start(message)

@dp.message_handler(text="↩️ Назад")
async def go_back(message: types.Message):
    """Вернуться назад"""
    is_admin = (message.from_user.id == ADMIN_ID)
    if is_admin:
        await message.answer("↩️ Возвращаемся...", reply_markup=get_admin_keyboard())
    else:
        await message.answer("↩️ Возвращаемся...", reply_markup=get_main_keyboard(is_admin=is_admin))

@dp.message_handler(text="↩️ К категориям")
async def go_to_categories(message: types.Message):
    """Вернуться к категориям"""
    await show_catalog(message)

@dp.callback_query_handler(lambda c: c.data == "back_to_catalog")
async def back_to_catalog_callback(call: types.CallbackQuery):
    """Вернуться в каталог из товара"""
    await call.message.edit_reply_markup(None)
    await show_catalog(call.message)

# ==================== КАТАЛОГ ====================
@dp.message_handler(text="🛍️ Каталог")
async def show_catalog(message: types.Message):
    """Показать категории"""
    is_admin = (message.from_user.id == ADMIN_ID)
    await message.answer(
        "📂 <b>Выберите категорию:</b>",
        parse_mode="HTML",
        reply_markup=get_categories_keyboard(is_admin=is_admin)
    )

@dp.callback_query_handler(lambda c: c.data == "view_categories")
async def callback_view_categories(call: types.CallbackQuery):
    """Обработчик кнопки 'К категориям' из корзины"""
    await call.message.edit_reply_markup(None)
    await show_catalog(call.message)

@dp.message_handler(lambda m: m.text in CATEGORIES.keys())
async def show_category(message: types.Message):
    """Показать рубрики категории"""
    category = CATEGORIES.get(message.text)
    if not category:
        return
    
    is_admin = (message.from_user.id == ADMIN_ID)
    await message.answer(
        f"📂 <b>{message.text}</b>\n\nВыберите рубрику:",
        parse_mode="HTML",
        reply_markup=get_subcategories_keyboard(message.text, is_admin=is_admin)
    )

# ==================== ПОКАЗ ТОВАРОВ (ИСПРАВЛЕННЫЙ) ====================
@dp.message_handler(lambda m: any(subcat in m.text for category in CATEGORIES.values() for subcat in category["subcategories"]))
async def show_products(message: types.Message):
    """Показать товары рубрики (ИСПРАВЛЕННЫЙ)"""
    try:
        # Определяем категорию по подкатегории
        subcategory_text = message.text
        
        # Ищем товары с этой подкатегорией
        product = None
        for prod in products_db.values():
            if prod.get('subcategory') == subcategory_text:
                product = prod
                break
        
        if not product:
            is_admin = (message.from_user.id == ADMIN_ID)
            if is_admin:
                # Для админа - предлагаем добавить товар
                await message.answer(
                    f"📭 В рубрике '{subcategory_text}' пока нет товаров.\n\n"
                    f"Хотите добавить товар? Используйте кнопку '➕ Добавить товар' в панели админа.",
                    reply_markup=get_admin_keyboard()
                )
            else:
                await message.answer(f"📭 В рубрике '{subcategory_text}' пока нет товаров.")
            return
        
        caption = format_product_info(product)
        
        # Для админа добавляем отметку
        is_admin = (message.from_user.id == ADMIN_ID)
        if is_admin:
            caption = f"👑 <b>РЕЖИМ АДМИНИСТРАТОРА</b>\n\n{caption}"
        
        if product.get('photo'):
            msg = await message.answer_photo(
                product['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=get_product_keyboard(product['id'], product, show_cart_button=not is_admin, is_admin=is_admin)
            )
        else:
            msg = await message.answer(caption, parse_mode="HTML", reply_markup=get_product_keyboard(product['id'], product, show_cart_button=not is_admin, is_admin=is_admin))
        
        # Увеличиваем счетчик просмотров только для обычных пользователей
        if not is_admin:
            increment_product_view(product['id'])
            
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")

# ==================== УПРАВЛЕНИЕ ТОВАРАМИ (НОВЫЙ ФУНКЦИОНАЛ) ====================
@dp.message_handler(text="✏️ Управление товарами")
async def manage_products(message: types.Message):
    """Управление товарами (редактирование, удаление)"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if not products_db:
        await message.answer("📭 В хозяйстве пока нет товаров.", reply_markup=get_admin_keyboard())
        return
    
    text = "✏️ <b>Управление товарами</b>\n\n"
    text += "Выберите товар для редактирования или удаления:\n\n"
    
    # Показываем список товаров
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        
        if key not in products_by_category:
            products_by_category[key] = product
    
    for i, (key, product) in enumerate(products_by_category.items(), 1):
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        text += f"{i}. <b>{product.get('subcategory', '')}</b>\n"
        text += f"   📦 Остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        text += f"   💰 Цена: {product.get('price', 0)} руб."
        
        if category_info.get('price_per_kg'):
            text += "/кг\n"
        else:
            text += "/шт\n"
        
        text += f"   🔹 ID: <code>{product.get('id')}</code>\n\n"
    
    text += "Нажмите на кнопку с товаром ниже для управления:"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_product_management_keyboard())

@dp.callback_query_handler(lambda c: c.data.startswith('edit_'))
async def edit_product_start(call: types.CallbackQuery):
    """Начать редактирование товара"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    caption = format_product_info(product)
    caption = f"✏️ <b>Редактирование товара</b>\n\n{caption}"
    
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
    """Начать изменение цены товара"""
    if call.from_user.id != ADMIN_ID:
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
        f"💰 <b>Изменение цены товара</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Текущая цена: {product.get('price', 0)} руб. {unit}\n\n"
        f"Введите новую цену (только число):",
        parse_mode="HTML"
    )

@dp.message_handler(state=EditProduct.new_price)
async def process_edit_price(message: types.Message, state: FSMContext):
    """Обработать новую цену товара"""
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        
        if not product:
            await message.answer("❌ Товар не найден в базе данных")
            await state.finish()
            return
        
        new_price = int(message.text)
        if new_price <= 0:
            raise ValueError
        
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
            f"✅ <b>Цена товара изменена!</b>\n\n"
            f"<b>{product.get('subcategory', '')}</b>\n"
            f"💰 Было: {old_price} руб. {unit}\n"
            f"💰 Стало: {new_price} руб. {unit}\n"
            f"📊 Изменение: {new_price - old_price} руб.",
            parse_mode="HTML",
            reply_markup=get_admin_keyboard()
        )
        
    except ValueError:
        await message.answer("❌ Введите корректную цену (положительное число)!")
    except Exception as e:
        await message.answer(f"❌ Ошибка при изменении цены: {str(e)}")
    finally:
        await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('edit_quantity_'))
async def edit_product_quantity_start(call: types.CallbackQuery):
    """Начать изменение остатка товара через меню редактирования"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    
    await call.message.answer(
        f"📦 <b>Изменение остатков товара:</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Текущий остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n\n"
        f"Введите новое количество:\n"
        f"<i>Можно ввести:</i>\n"
        f"• <code>10</code> - установить 10 шт\n"
        f"• <code>+5</code> - добавить 5 шт\n"
        f"• <code>-3</code> - убрать 3 шт\n"
        f"• <code>0</code> - обнулить остатки",
        parse_mode="HTML"
    )
    
    # Используем состояние для изменения остатков
    from aiogram.dispatcher import FSMContext
    await AdjustStockState.product_id.set()
    await AdjustStockState.quantity.set()
    await dp.current_state().update_data(product_id=product_id)

@dp.callback_query_handler(lambda c: c.data.startswith('edit_photo_'))
async def edit_product_photo_start(call: types.CallbackQuery, state: FSMContext):
    """Начать изменение фото товара"""
    if call.from_user.id != ADMIN_ID:
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
        f"📸 <b>Изменение фото товара</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n\n"
        f"Отправьте новое фото для товара:",
        parse_mode="HTML"
    )

@dp.message_handler(content_types=types.ContentType.PHOTO, state=EditProduct.new_photo)
async def process_edit_photo(message: types.Message, state: FSMContext):
    """Обработать новое фото товара"""
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        
        if not product:
            await message.answer("❌ Товар не найден в базе данных")
            await state.finish()
            return
        
        old_photo = product.get('photo', 'не было')
        product['photo'] = message.photo[-1].file_id
        
        save_data()
        
        await message.answer_photo(
            message.photo[-1].file_id,
            caption=f"✅ <b>Фото товара обновлено!</b>\n\n"
                   f"<b>{product.get('subcategory', '')}</b>\n\n"
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
    """Подтверждение удаления товара"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Проверяем, есть ли товар в активных заказах
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
    
    # Создаем клавиатуру для подтверждения удаления
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("✅ Да, удалить", callback_data=f"delete_yes_{product_id}"),
        InlineKeyboardButton("❌ Нет, отмена", callback_data=f"delete_no_{product_id}")
    )
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    
    await call.message.answer(
        f"⚠️ <b>Подтверждение удаления товара</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Остаток: {product.get('quantity', 0)} шт\n"
        f"💰 Цена: {product.get('price', 0)} руб.\n\n"
        f"<b>ВНИМАНИЕ!</b> Это действие невозможно отменить.\n"
        f"Товар будет полностью удален из базы данных.\n\n"
        f"Вы уверены, что хотите удалить этот товар?",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.callback_query_handler(lambda c: c.data.startswith('delete_yes_'))
async def delete_product_yes(call: types.CallbackQuery):
    """Подтвержденное удаление товара"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    product_name = product.get('subcategory', 'товар')
    
    # Удаляем товар из базы данных
    del products_db[product_id]
    
    # Удаляем связанные уведомления
    if product_id in notifications_db:
        del notifications_db[product_id]
    
    save_data()
    
    await call.message.answer(
        f"🗑️ <b>Товар удален!</b>\n\n"
        f"<b>{product_name}</b> успешно удален из базы данных.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    
    await call.answer(f"✅ Товар '{product_name}' удален", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('delete_no_'))
async def delete_product_no(call: types.CallbackQuery):
    """Отмена удаления товара"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    await call.message.answer(
        f"✅ <b>Удаление отменено</b>\n\n"
        f"Товар <b>{product.get('subcategory', '')}</b> не был удален.",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    
    await call.answer("❌ Удаление отменено", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('delete_'))
async def delete_product_direct(call: types.CallbackQuery):
    """Прямое удаление товара (старый обработчик)"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Перенаправляем на подтверждение удаления
    await delete_product_confirm(call)

@dp.callback_query_handler(lambda c: c.data == "back_to_product_management")
async def back_to_product_management(call: types.CallbackQuery):
    """Вернуться к управлению товарами"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await manage_products(call.message)

# ==================== КОРЗИНА (ИСПРАВЛЕННАЯ) ====================
@dp.message_handler(text="🛒 Корзина")
async def show_cart(message: types.Message):
    """Показать корзину - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
    user_id = str(message.from_user.id)
    cart = user_carts.get(user_id, [])
    
    if not cart:
        is_admin = (message.from_user.id == ADMIN_ID)
        await message.answer("🛒 Ваша корзина пуста.", 
                           reply_markup=get_main_keyboard(is_admin=is_admin))
        return
    
    # Формируем текст корзины
    total = 0
    text = "🛒 <b>Ваша корзина:</b>\n\n"
    
    # Проверяем, есть ли в корзине товары с не точной ценой
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
                    # Для мяса с известным средним весом
                    estimated_price, note = calculate_product_price(product, item['quantity'])
                    text += f"• {item['name']} - {item['quantity']} шт.\n"
                    text += f"  Примерная стоимость*: ~{estimated_price:.0f} руб.\n"
                    total += estimated_price
                else:
                    # Для других товаров с ценой за кг
                    text += f"• {item['name']} - {item['quantity']} кг\n"
                    text += f"  <i>Цена будет известна при получении</i>\n"
                    has_exact_price_only = False
            else:
                # Для яиц
                item_price, note = calculate_product_price(product, item['quantity'])
                text += f"• {item['name']} - {item['quantity']} упак.\n"
                text += f"  Цена: {item_price} руб. {note}\n"
                total += item_price
    
    # Добавляем примечание в зависимости от состава корзины
    if has_inexact_price:
        # ИСПРАВЛЕНО: заменяем <small> на <i>
        text += f"\n<i>*Расчетная стоимость. Итоговая цена зависит от фактического веса.</i>\n"
    
    if has_exact_price_only:
        text += f"\n💰 <b>Итого к оплате:</b> {total:.0f} руб."
    else:
        text += f"\n💰 <b>Примерная сумма:</b> ~{total:.0f} руб.\n"
        text += f"<i>Итоговая стоимость будет рассчитана при получении</i>"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))

@dp.callback_query_handler(lambda c: c.data == "go_to_cart")
async def go_to_cart_callback(call: types.CallbackQuery):
    """Быстрый переход в корзину из товара"""
    user_id = str(call.from_user.id)
    cart = user_carts.get(user_id, [])
    
    if not cart:
        await call.answer("🛒 Ваша корзина пуста.", show_alert=True)
        return
    
    # Формируем текст корзины
    total = 0
    text = "🛒 <b>Ваша корзина:</b>\n\n"
    
    # Проверяем, есть ли в корзине товары с не точной ценой
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
                    text += f"  <i>Цена будет известна при получении</i>\n"
                    has_exact_price_only = False
            else:
                item_price, note = calculate_product_price(product, item['quantity'])
                text += f"• {item['name']} - {item['quantity']} упак.\n"
                text += f"  Цена: {item_price} руб. {note}\n"
                total += item_price
    
    # Добавляем примечание в зависимости от состава корзины
    if has_inexact_price:
        # ИСПРАВЛЕНО: заменяем <small> на <i>
        text += f"\n<i>*Расчетная стоимость. Итоговая цена зависит от фактического веса.</i>\n"
    
    if has_exact_price_only:
        text += f"\n💰 <b>Итого к оплате:</b> {total:.0f} руб."
    else:
        text += f"\n💰 <b>Примерная сумма:</b> ~{total:.0f} руб.\n"
        text += f"<i>Итоговая стоимость будет рассчитана при получении</i>"
    
    # Отправляем новое сообщение с корзиной вместо редактирования
    await call.message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))
    await call.answer("🛒 Переходим в корзину...")

@dp.callback_query_handler(lambda c: c.data.startswith('add_'))
async def add_to_cart(call: types.CallbackQuery):
    """Добавить товар в корзину"""
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Проверяем, не админ ли
    if call.from_user.id == ADMIN_ID:
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    
    # Увеличиваем счетчик просмотров
    increment_product_view(product_id)
    
    # Проверяем остаток
    if product.get('quantity', 0) <= 0:
        await call.answer("❌ Товара нет в наличии", show_alert=True)
        return
    
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        user_carts[user_id] = []
    
    # Проверяем, есть ли уже товар из этой же рубрики в корзине
    product_category = product.get('category', '')
    product_subcategory = product.get('subcategory', '')
    
    # Ищем товар в корзине (если был добавлен ранее)
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    
    if item_index >= 0:
        # Увеличиваем количество
        user_carts[user_id][item_index]['quantity'] += 1
        current_quantity = user_carts[user_id][item_index]['quantity']
    else:
        # Добавляем новый товар
        # ИСПРАВЛЕНО: сохраняем только название рубрики
        user_carts[user_id].append({
            'id': product_id,
            'name': product.get('subcategory', ''),
            'quantity': 1
        })
        current_quantity = 1
    
    save_data()
    
    # Показываем уведомление
    await call.answer(f"✅ {product.get('subcategory', 'Товар')} добавлен в корзину! 📦 В корзине: {current_quantity} шт.", show_alert=False)
    
    # Обновляем клавиатуру, добавляя кнопку перехода в корзину
    new_keyboard = get_product_keyboard(product_id, product, show_cart_button=True, is_admin=False)
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
        # Если не получается обновить клавиатуру, продолжаем

@dp.callback_query_handler(lambda c: c.data.startswith('manual_add_'))
async def manual_add_to_cart_start(call: types.CallbackQuery, state: FSMContext):
    """Начать ручное добавление товара в корзину"""
    product_id = call.data.split('_')[2]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Проверяем, не админ ли
    if call.from_user.id == ADMIN_ID:
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    
    # Проверяем остаток
    if product.get('quantity', 0) <= 0:
        await call.answer("❌ Товара нет в наличии", show_alert=True)
        return
    
    # Увеличиваем счетчик запросов на ручное добавление
    increment_manual_add_request(product_id)
    
    user_id = str(call.from_user.id)
    
    # Проверяем, есть ли уже товар в корзине
    current_quantity = 0
    for item in user_carts.get(user_id, []):
        if item['id'] == product_id:
            current_quantity = item['quantity']
            break
    
    await state.update_data(product_id=product_id)
    await ManualAddToCartState.quantity.set()
    
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    
    await call.message.answer(
        f"📝 <b>Ручное добавление в корзину</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Доступно: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        f"📦 Сейчас в корзине: {current_quantity}\n\n"
        f"Введите количество для добавления в корзину:",
        parse_mode="HTML"
    )

@dp.message_handler(state=ManualAddToCartState.quantity)
async def process_manual_add_quantity(message: types.Message, state: FSMContext):
    """Обработать количество для ручного добавления"""
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        
        if not product:
            await message.answer("❌ Товар не найден")
            await state.finish()
            return
        
        quantity = int(message.text)
        if quantity <= 0:
            await message.answer("❌ Введите положительное число!")
            await state.finish()
            return
        
        # Проверяем остаток
        if quantity > product.get('quantity', 0):
            await message.answer(f"❌ Недостаточно товара! Доступно: {product.get('quantity', 0)}")
            await state.finish()
            return
        
        user_id = str(message.from_user.id)
        if user_id not in user_carts:
            user_carts[user_id] = []
        
        # Ищем товар в корзине
        item_index = -1
        for i, item in enumerate(user_carts[user_id]):
            if item['id'] == product_id:
                item_index = i
                break
        
        if item_index >= 0:
            # Проверяем общее количество
            new_total = user_carts[user_id][item_index]['quantity'] + quantity
            if new_total > product.get('quantity', 0):
                await message.answer(f"❌ Недостаточно товара! Доступно: {product.get('quantity', 0)}")
                await state.finish()
                return
            
            user_carts[user_id][item_index]['quantity'] = new_total
            current_quantity = new_total
        else:
            # Добавляем новый товар
            # ИСПРАВЛЕНО: сохраняем только название рубрики
            user_carts[user_id].append({
                'id': product_id,
                'name': product.get('subcategory', ''),
                'quantity': quantity
            })
            current_quantity = quantity
        
        save_data()
        
        await message.answer(
            f"✅ <b>Товар добавлен в корзину!</b>\n\n"
            f"<b>{product.get('subcategory', '')}</b>\n"
            f"📦 Добавлено: {quantity} {category_info.get('unit', 'шт')}\n"
            f"📦 Всего в корзине: {current_quantity}",
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=(message.from_user.id == ADMIN_ID))
        )
        
    except ValueError:
        await message.answer("❌ Введите корректное число!")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
    finally:
        await state.finish()

# ==================== ИЗМЕНЕНИЕ КОЛИЧЕСТВА В КОРЗИНЕ (для пользователей) ====================
@dp.callback_query_handler(lambda c: c.data.startswith('change_'))
async def change_quantity_start(call: types.CallbackQuery, state: FSMContext):
    """Начать изменение количества товара в корзине"""
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    user_id = str(call.from_user.id)
    cart = user_carts.get(user_id, [])
    
    if not cart:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    
    # Ищем товар в корзине
    current_quantity = 0
    for item in cart:
        if item['id'] == product_id:
            current_quantity = item['quantity']
            break
    
    if current_quantity == 0:
        await call.answer("❌ Товар не найден в корзине", show_alert=True)
        return
    
    await state.update_data(product_id=product_id)
    await AdjustQuantityState.quantity.set()
    
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    
    await call.message.answer(
        f"📝 <b>Изменение количества в корзине</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Доступно: {product.get('quantity', 0) + current_quantity} {category_info.get('unit', 'шт')}\n"
        f"📦 Сейчас в корзине: {current_quantity}\n\n"
        f"Введите новое количество:",
        parse_mode="HTML"
    )

@dp.message_handler(state=AdjustQuantityState.quantity)
async def process_adjust_quantity(message: types.Message, state: FSMContext):
    """Обработать изменение количества в корзине (для пользователей)"""
    try:
        data = await state.get_data()
        product_id = data.get('product_id')
        product = products_db.get(product_id)
        
        if not product:
            await message.answer("❌ Товар не найден")
            await state.finish()
            return
        
        quantity = int(message.text)
        if quantity < 0:
            await message.answer("❌ Количество не может быть отрицательным!")
            await state.finish()
            return
        
        user_id = str(message.from_user.id)
        if user_id not in user_carts:
            user_carts[user_id] = []
        
        # Ищем товар в корзине
        item_index = -1
        for i, item in enumerate(user_carts[user_id]):
            if item['id'] == product_id:
                item_index = i
                break
        
        if item_index >= 0:
            if quantity == 0:
                # Удаляем товар из корзины
                del user_carts[user_id][item_index]
                message_text = "🗑️ Товар удален из корзины"
            else:
                # Проверяем доступность
                if quantity > product.get('quantity', 0) + user_carts[user_id][item_index]['quantity']:
                    await message.answer(f"❌ Недостаточно товара! Доступно: {product.get('quantity', 0)}")
                    await state.finish()
                    return
                
                # Обновляем количество в корзине
                user_carts[user_id][item_index]['quantity'] = quantity
                message_text = f"📦 Количество обновлено: {quantity} шт"
        else:
            await message.answer("❌ Товар не найден в корзине")
            await state.finish()
            return
        
        save_data()
        
        # Обновляем сообщение корзины
        cart = user_carts.get(user_id, [])
        if not cart:
            await message.answer("🛒 Корзина пуста", 
                               reply_markup=get_main_keyboard(is_admin=(message.from_user.id == ADMIN_ID)))
        else:
            # Формируем текст корзины
            total = 0
            text = "🛒 <b>Ваша корзина:</b>\n\n"
            
            has_inexact_price = False
            has_exact_price_only = True
            
            for item in cart:
                product_item = products_db.get(item['id'])
                if product_item:
                    category_info = get_category_info(product_item.get('category', ''), product_item.get('subcategory', ''))
                    
                    if not category_info.get('exact_price', True):
                        has_inexact_price = True
                        has_exact_price_only = False
                    
                    if category_info.get('price_per_kg'):
                        if category_info.get('average_weight', 0) > 0:
                            estimated_price, note = calculate_product_price(product_item, item['quantity'])
                            text += f"• {item['name']} - {item['quantity']} шт.\n"
                            text += f"  Примерная стоимость*: ~{estimated_price:.0f} руб.\n"
                            total += estimated_price
                        else:
                            text += f"• {item['name']} - {item['quantity']} кг\n"
                            text += f"  <i>Цена будет известна при получении</i>\n"
                            has_exact_price_only = False
                    else:
                        item_price, note = calculate_product_price(product_item, item['quantity'])
                        text += f"• {item['name']} - {item['quantity']} упак.\n"
                        text += f"  Цена: {item_price} руб. {note}\n"
                        total += item_price
            
            # Добавляем примечание в зависимости от состава корзины
            if has_inexact_price:
                text += f"\n<i>*Расчетная стоимость. Итоговая цена зависит от фактического веса.</i>\n"
            
            if has_exact_price_only:
                text += f"\n💰 <b>Итого к оплате:</b> {total:.0f} руб."
            else:
                text += f"\n💰 <b>Примерная сумма:</b> ~{total:.0f} руб.\n"
                text += f"<i>Итоговая стоимость будет рассчитана при получении</i>"
            
            await message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))
        
    except ValueError:
        await message.answer("❌ Введите корректное число!")
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
    finally:
        await state.finish()

# ==================== ИЗМЕНЕНИЕ ОСТАТКОВ НА СКЛАДЕ (для админа) ====================
@dp.callback_query_handler(lambda c: c.data.startswith('adjust_'))
async def adjust_quantity_start(call: types.CallbackQuery, state: FSMContext):
    """Начало изменения остатков через кнопку"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
    
    # Используем новое состояние для изменения остатков на складе
    await state.update_data(product_id=product_id)
    await AdjustStockState.quantity.set()
    
    await call.message.answer(
        f"📦 <b>Изменение остатков на складе:</b>\n\n"
        f"<b>{product.get('subcategory', '')}</b>\n"
        f"📦 Текущий остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n\n"
        f"Введите новое количество:\n"
        f"<i>Можно ввести:</i>\n"
        f"• <code>10</code> - установить 10 шт\n"
        f"• <code>+5</code> - добавить 5 шт\n"
        f"• <code>-3</code> - убрать 3 шт\n"
        f"• <code>0</code> - обнулить остатки",
        parse_mode="HTML"
    )

@dp.message_handler(state=AdjustStockState.quantity)
async def process_adjust_stock_quantity(message: types.Message, state: FSMContext):
    """Обработать изменение остатков на складе (для админа)"""
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
        
        # Определяем тип операции
        if quantity_str.startswith('+'):
            # Добавление
            delta = int(quantity_str[1:])
            new_quantity = old_quantity + delta
            operation = "добавлено"
            
        elif quantity_str.startswith('-'):
            # Вычитание
            delta = int(quantity_str[1:])
            new_quantity = old_quantity - delta
            operation = "вычтено"
            
        else:
            # Установка точного значения
            new_quantity = int(quantity_str)
            operation = "установлено"
        
        # Проверяем, чтобы не было отрицательных остатков
        if new_quantity < 0:
            await message.answer("❌ Нельзя установить отрицательное количество!")
            await state.finish()
            return
        
        # Сохраняем изменения В БАЗЕ ДАННЫХ ТОВАРОВ
        product['quantity'] = new_quantity
        save_data()
        
        # Проверяем, были ли уведомления
        if old_quantity == 0 and new_quantity > 0 and product_id in notifications_db:
            await send_notifications(product_id)
        
        await message.answer(
            f"✅ <b>Остатки на складе обновлены!</b>\n\n"
            f"<b>{product.get('subcategory', '')}</b>\n"
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

@dp.callback_query_handler(lambda c: c.data.startswith('inc_'))
async def increase_quantity(call: types.CallbackQuery):
    """Увеличить количество товара в корзине"""
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    
    # Ищем товар в корзине
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    
    if item_index >= 0:
        # Проверяем остаток
        if user_carts[user_id][item_index]['quantity'] < product.get('quantity', 0):
            user_carts[user_id][item_index]['quantity'] += 1
            current_quantity = user_carts[user_id][item_index]['quantity']
            save_data()
            
            # Показываем уведомление
            await call.answer(f"➕ {product.get('subcategory', 'Товар')}\n📦 Теперь: {current_quantity} шт.", show_alert=False)
            
            # Обновляем сообщение корзины
            await update_cart_message(call, user_id)
        else:
            await call.answer("❌ Недостаточно товара на складе", show_alert=True)
    else:
        await call.answer("❌ Товар не найден в корзине", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('dec_'))
async def decrease_quantity(call: types.CallbackQuery):
    """Уменьшить количество товара в корзине"""
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    
    # Ищем товар в корзине
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    
    if item_index >= 0:
        if user_carts[user_id][item_index]['quantity'] > 1:
            user_carts[user_id][item_index]['quantity'] -= 1
            current_quantity = user_carts[user_id][item_index]['quantity']
            save_data()
            
            # Показываем уведомление
            await call.answer(f"➖ {product.get('subcategory', 'Товар')}\n📦 Теперь: {current_quantity} шт.", show_alert=False)
            
            # Обновляем сообщение корзины
            await update_cart_message(call, user_id)
        else:
            # Удаляем товар из корзины
            product_name = user_carts[user_id][item_index]['name']
            del user_carts[user_id][item_index]
            save_data()
            
            # Показываем уведомление
            await call.answer(f"🗑️ {product_name}\n❌ Удалено из корзины", show_alert=False)
            
            if user_carts[user_id]:
                await update_cart_message(call, user_id)
            else:
                # Отправляем новое сообщение о пустой корзине
                await call.message.answer("🛒 Корзина пуста")
    else:
        await call.answer("❌ Товар не найден в корзине", show_alert=True)

async def update_cart_message(call: types.CallbackQuery, user_id: str):
    """Обновить сообщение корзины"""
    cart = user_carts.get(user_id, [])
    if not cart:
        # Отправляем новое сообщение о пустой корзине
        await call.message.answer("🛒 Корзина пуста")
        return
    
    # Формируем текст корзины
    total = 0
    text = "🛒 <b>Ваша корзина:</b>\n\n"
    
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
                    text += f"  <i>Цена будет известна при получении</i>\n"
                    has_exact_price_only = False
            else:
                item_price, note = calculate_product_price(product, item['quantity'])
                text += f"• {item['name']} - {item['quantity']} упак.\n"
                text += f"  Цена: {item_price} руб. {note}\n"
                total += item_price
    
    # Добавляем примечание в зависимости от состава корзины
    if has_inexact_price:
        # ИСПРАВЛЕНО: заменяем <small> на <i>
        text += f"\n<i>*Расчетная стоимость. Итоговая цена зависит от фактического веса.</i>\n"
    
    if has_exact_price_only:
        text += f"\n💰 <b>Итого к оплате:</b> {total:.0f} руб."
    else:
        text += f"\n💰 <b>Примерная сумма:</b> ~{total:.0f} руб.\n"
        text += f"<i>Итоговая стоимость будет рассчитана при получении</i>"
    
    try:
        # Пробуем редактировать сообщение
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))
    except Exception as e:
        # Если не получается редактировать (например, у сообщения есть фото), отправляем новое
        print(f"Ошибка при редактировании сообщения корзины: {e}")
        await call.message.answer(text, parse_mode="HTML", reply_markup=get_cart_keyboard(cart))

@dp.callback_query_handler(lambda c: c.data == "clear_cart")
async def clear_cart_callback(call: types.CallbackQuery):
    """Очистить корзину"""
    user_id = str(call.from_user.id)
    user_carts[user_id] = []
    save_data()
    
    # Показываем уведомление
    await call.answer("🗑️ Корзина очищена", show_alert=False)
    
    # Отправляем новое сообщение о пустой корзине
    await call.message.answer("🛒 Корзина пуста")

# ==================== АКТИВНЫЕ ЗАКАЗЫ С ИНТЕРАКТИВНЫМ УПРАВЛЕНИЕМ ====================
@dp.message_handler(text="📋 Активные заказы")
async def show_active_orders(message: types.Message):
    """Показать активные заказы с кнопками управления"""
    if message.from_user.id != ADMIN_ID:
        return
    
    # Получаем активные заказы
    active = [o for o in orders_db.values() if o.get('status') in ['🆕 Новый', '✅ Подтвержден']]
    
    if not active:
        await message.answer("📭 Нет активных заказов.", reply_markup=get_admin_keyboard())
        return
    
    # Формируем текст
    text = "📋 <b>Активные заказы</b>\n\n"
    
    # Статистика по статусам
    new_count = len([o for o in active if o.get('status') == '🆕 Новый'])
    confirmed_count = len([o for o in active if o.get('status') == '✅ Подтвержден'])
    
    text += f"🆕 <b>Новые:</b> {new_count}\n"
    text += f"✅ <b>Подтвержденные:</b> {confirmed_count}\n"
    text += f"📊 <b>Всего активных:</b> {len(active)}\n\n"
    text += "<i>Нажмите на заказ для управления:</i>"
    
    # Получаем клавиатуру с заказами
    keyboard = get_active_orders_keyboard()
    
    await message.answer(text, parse_mode="HTML", reply_markup=keyboard)

@dp.callback_query_handler(lambda c: c.data.startswith('manage_order_'))
async def manage_specific_order(call: types.CallbackQuery):
    """Управление конкретным заказом"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    # Исправляем: берем только order_id после префикса
    order_id = call.data.replace('manage_order_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    # Форматируем информацию о заказе
    order_text = format_order_info(order)
    
    # Добавляем историю статусов
    if 'status_history' in order:
        order_text += "\n<b>История статусов:</b>\n"
        for history in order['status_history'][-3:]:  # Показываем последние 3 статуса
            order_text += f"• {history['status']} - {history['timestamp']}\n"
    
    # Создаем клавиатуру управления
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
    
    # Общие кнопки
    keyboard.add(
        InlineKeyboardButton("💬 Связаться", callback_data=f"contact_{order_id}"),
        InlineKeyboardButton("📊 Статистика клиента", callback_data=f"client_stats_{order_id}")
    )
    keyboard.add(InlineKeyboardButton("🔙 Назад к списку", callback_data="back_to_orders_list"))
    keyboard.add(InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel"))
    
    try:
        await call.message.edit_text(
            f"<b>Управление заказом #{order_id}</b>\n\n{order_text}",
            parse_mode="HTML",
            reply_markup=keyboard
        )
    except Exception as e:
        # Если не получается редактировать, отправляем новое сообщение
        print(f"Ошибка при редактировании сообщения заказа: {e}")
        await call.message.answer(
            f"<b>Управление заказом #{order_id}</b>\n\n{order_text}",
            parse_mode="HTML",
            reply_markup=keyboard
        )
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "back_to_orders_list")
async def back_to_orders_list(call: types.CallbackQuery):
    """Вернуться к списку активных заказов"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    # Получаем активные заказы
    active = [o for o in orders_db.values() if o.get('status') in ['🆕 Новый', '✅ Подтвержден']]
    
    if not active:
        try:
            await call.message.edit_text(
                "📭 Нет активных заказов.",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("🔄 Обновить", callback_data="refresh_active_orders"),
                    InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
                )
            )
        except:
            await call.message.answer(
                "📭 Нет активных заказов.",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("🔄 Обновить", callback_data="refresh_active_orders"),
                    InlineKeyboardButton("👑 Панель админа", callback_data="admin_panel")
                )
            )
        return
    
    # Формируем текст
    text = "📋 <b>Активные заказы</b>\n\n"
    
    # Статистика по статусам
    new_count = len([o for o in active if o.get('status') == '🆕 Новый'])
    confirmed_count = len([o for o in active if o.get('status') == '✅ Подтвержден'])
    
    text += f"🆕 <b>Новые:</b> {new_count}\n"
    text += f"✅ <b>Подтвержденные:</b> {confirmed_count}\n"
    text += f"📊 <b>Всего активных:</b> {len(active)}\n\n"
    text += "<i>Нажмите на заказ для управления:</i>"
    
    # Получаем клавиатуру с заказами
    keyboard = get_active_orders_keyboard()
    
    try:
        await call.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    except:
        await call.message.answer(text, parse_mode="HTML", reply_markup=keyboard)
    
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "refresh_active_orders")
async def refresh_active_orders(call: types.CallbackQuery):
    """Обновить список активных заказов"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    await call.answer("🔄 Обновляем список...", show_alert=False)
    await show_active_orders(call.message)

@dp.callback_query_handler(lambda c: c.data == "no_active_orders")
async def no_active_orders_callback(call: types.CallbackQuery):
    """Обработка кнопки "Нет активных заказов" """
    await call.answer("📭 Нет активных заказов", show_alert=True)

# ==================== ОБРАБОТКА КНОПОК АДМИНА ДЛЯ ЗАКАЗОВ ====================
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_'))
async def confirm_order(call: types.CallbackQuery):
    """Подтвердить заказ (первичное подтверждение)"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    order_id = call.data.replace('confirm_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    # Обновляем статус
    order['status'] = '✅ Подтвержден'
    order['status_history'].append({
        'status': '✅ Подтвержден',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    save_data()
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            order['user_id'],
            f"✅ <b>Ваш заказ #{order_id} подтвержден!</b>\n\n"
            f"Администратор подтвердил ваш заказ.\n"
            f"Скоро с вами свяжутся для уточнения деталей.\n\n"
            f"Спасибо за покупку! 🛍️",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    
    # Обновляем сообщение админа с новыми кнопками управления
    order_text = call.message.text.split('\n\n', 1)[1] if '\n\n' in call.message.text else call.message.text
    updated_text = f"✅ <b>ЗАКАЗ #{order_id} ПОДТВЕРЖДЕН</b>\n\n{order_text}"
    
    # Создаем новую клавиатуру для подтвержденного заказа
    keyboard = InlineKeyboardMarkup(row_width=2)
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
            updated_text,
            parse_mode="HTML",
            reply_markup=keyboard
        )
    except:
        await call.message.answer(
            updated_text,
            parse_mode="HTML",
            reply_markup=keyboard
        )
    
    await call.answer("✅ Заказ подтвержден", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith('reject_'))
async def reject_order(call: types.CallbackQuery):
    """Отклонить заказ при первичном подтверждении"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    order_id = call.data.replace('reject_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    # Возвращаем товары на склад
    for item in order.get('items', []):
        product = products_db.get(item['id'])
        if product:
            product['quantity'] = product.get('quantity', 0) + item['quantity']
    
    # Обновляем статус
    order['status'] = '❌ Отклонен'
    order['status_history'].append({
        'status': '❌ Отклонен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    # Обновляем статистику пользователя
    update_user_stats(order['user_id'], order, 'canceled')
    
    save_data()
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            order['user_id'],
            f"❌ <b>Ваш заказ #{order_id} отклонен</b>\n\n"
            f"К сожалению, администратор отклонил ваш заказ.\n"
            f"Товары возвращены на склад.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    
    await call.answer("❌ Заказ отклонен", show_alert=True)
    
    # Возвращаемся к списку заказов
    await back_to_orders_list(call)

@dp.callback_query_handler(lambda c: c.data.startswith('complete_'))
async def complete_order(call: types.CallbackQuery):
    """Отметить заказ как выполненный (выдан)"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    order_id = call.data.replace('complete_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    # Обновляем статус
    order['status'] = '✅ Выполнен'
    order['status_history'].append({
        'status': '✅ Выполнен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    # Обновляем статистику пользователя
    update_user_stats(order['user_id'], order, 'completed')
    
    save_data()
    
    # Отправляем благодарственное сообщение пользователю
    try:
        thank_you_message = get_random_thank_you_message()
        await bot.send_message(
            order['user_id'],
            f"✅ <b>Ваш заказ #{order_id} выполнен!</b>\n\n"
            f"Заказ успешно выдан/доставлен.\n\n"
            f"{thank_you_message}",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке благодарственного сообщения: {e}")
    
    await call.answer("✅ Заказ отмечен как выполненный", show_alert=True)
    
    # Возвращаемся к списку заказов
    await back_to_orders_list(call)

@dp.callback_query_handler(lambda c: c.data.startswith('cancel_'))
async def cancel_order(call: types.CallbackQuery):
    """Отменить подтвержденный заказ"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    order_id = call.data.replace('cancel_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    # Возвращаем товары на склад
    for item in order.get('items', []):
        product = products_db.get(item['id'])
        if product:
            product['quantity'] = product.get('quantity', 0) + item['quantity']
    
    # Обновляем статус
    order['status'] = '❌ Отменен'
    order['status_history'].append({
        'status': '❌ Отменен',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{call.from_user.id}"
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    # Обновляем статистику пользователя
    update_user_stats(order['user_id'], order, 'canceled')
    
    save_data()
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            order['user_id'],
            f"❌ <b>Ваш заказ #{order_id} отменен</b>\n\n"
            f"К сожалению, администратор отменил ваш заказ.\n"
            f"Товары возвращены на склад.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    
    await call.answer("❌ Заказ отменен", show_alert=True)
    
    # Возвращаемся к списку заказов
    await back_to_orders_list(call)

@dp.callback_query_handler(lambda c: c.data.startswith('postpone_'))
async def postpone_order_start(call: types.CallbackQuery, state: FSMContext):
    """Начать перенос заказа"""
    if call.from_user.id != ADMIN_ID:
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
        f"⏰ <b>Перенос заказа #{order_id}</b>\n\n"
        f"Введите новую дату и время для заказа (например: 'завтра 18:00' или '25.12.2024 15:00'):",
        parse_mode="HTML"
    )

@dp.message_handler(state=PostponeOrderState.new_date)
async def process_postpone_date(message: types.Message, state: FSMContext):
    """Обработать новую дату для переноса заказа"""
    data = await state.get_data()
    order_id = data.get('order_id')
    order = orders_db.get(order_id)
    
    if not order:
        await message.answer("❌ Заказ не найден")
        await state.finish()
        return
    
    new_date = message.text.strip()
    
    # Обновляем статус с указанием новой даты
    order['status'] = f'⏰ Перенесен ({new_date})'
    order['status_history'].append({
        'status': f'⏰ Перенесен ({new_date})',
        'timestamp': datetime.now().strftime("%d.%m.%Y %H:%M"),
        'changed_by': f"admin_{message.from_user.id}",
        'new_date': new_date
    })
    order['status_updated_at'] = datetime.now().strftime("%d.%m.%Y %H:%M")
    
    # Обновляем статистику пользователя
    update_user_stats(order['user_id'], order, 'postponed')
    
    save_data()
    
    # Уведомляем пользователя
    try:
        await bot.send_message(
            order['user_id'],
            f"⏰ <b>Ваш заказ #{order_id} перенесен</b>\n\n"
            f"Администратор перенес ваш заказ на {new_date}.\n"
            f"С вами свяжутся для уточнения деталей.\n\n"
            f"Если у вас есть вопросы, свяжитесь с администратором.",
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при отправке уведомления пользователю: {e}")
    
    await message.answer(
        f"✅ <b>Заказ #{order_id} перенесен на {new_date}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_keyboard()
    )
    
    await state.finish()

@dp.callback_query_handler(lambda c: c.data.startswith('client_stats_'))
async def show_client_stats(call: types.CallbackQuery):
    """Показать статистику клиента"""
    if call.from_user.id != ADMIN_ID:
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

@dp.callback_query_handler(lambda c: c.data.startswith('contact_'))
async def contact_client(call: types.CallbackQuery):
    """Связаться с клиентом"""
    if call.from_user.id != ADMIN_ID:
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
            f"💬 <b>Связь с клиентом</b>\n\n"
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

@dp.callback_query_handler(lambda c: c.data.startswith('view_order_'))
async def view_client_order(call: types.CallbackQuery):
    """Просмотреть заказ клиента"""
    if call.from_user.id != ADMIN_ID:
        await call.answer("❌ Недостаточно прав", show_alert=True)
        return
    
    order_id = call.data.replace('view_order_', '')
    order = orders_db.get(order_id)
    
    if not order:
        await call.answer("❌ Заказ не найден", show_alert=True)
        return
    
    order_text = format_order_info(order)
    
    # Добавляем историю статусов
    if 'status_history' in order:
        order_text += "\n<b>История статусов:</b>\n"
        for history in order['status_history']:
            order_text += f"• {history['status']} - {history['timestamp']}\n"
    
    await call.message.answer(order_text, parse_mode="HTML", reply_markup=get_admin_keyboard())
    await call.answer("📋 Информация о заказе", show_alert=True)

# ==================== ОФОРМЛЕНИЕ ЗАКАЗА ====================
@dp.callback_query_handler(lambda c: c.data == "checkout")
async def start_checkout(call: types.CallbackQuery):
    """Начать оформление заказа"""
    user_id = str(call.from_user.id)
    cart = user_carts.get(user_id, [])
    
    if not cart:
        await call.answer("❌ Корзина пуста", show_alert=True)
        return
    
    # Проверяем наличие товаров
    for item in cart:
        product = products_db.get(item['id'])
        if not product or product.get('quantity', 0) < item['quantity']:
            await call.answer(f"❌ Недостаточно товара: {product.get('subcategory', 'Товар')}", show_alert=True)
            return
    
    await CheckoutState.delivery_method.set()
    await call.message.answer(
        "🚚 <b>Выберите способ получения заказа:</b>\n\n"
        f"🚗 <b>Самовывоз:</b> {PICKUP_ADDRESS}\n"
        f"🚚 <b>Доставка:</b> {DELIVERY_COST} руб. (бесплатно от {FREE_DELIVERY_THRESHOLD} руб.)\n\n"
        f"<i>После оформления заказа с вами свяжется администратор для подтверждения</i>",
        parse_mode="HTML",
        reply_markup=get_delivery_keyboard()
    )

@dp.callback_query_handler(lambda c: c.data in ["pickup", "delivery"], state=CheckoutState.delivery_method)
async def process_delivery_method(call: types.CallbackQuery, state: FSMContext):
    """Обработать способ доставки"""
    delivery_method = call.data
    
    async with state.proxy() as data:
        data['delivery_method'] = delivery_method
    
    if delivery_method == "delivery":
        await CheckoutState.address.set()
        await call.message.answer("🏠 Введите адрес доставки:")
    else:
        # Для самовывоза сразу создаем заказ
        await create_order(call, state, PICKUP_ADDRESS)

@dp.message_handler(state=CheckoutState.address)
async def process_address(message: types.Message, state: FSMContext):
    """Обработать адрес и создать заказ"""
    address = message.text
    
    if not address or len(address.strip()) < 5:
        await message.answer("❌ Введите корректный адрес (минимум 5 символов)")
        return
    
    await create_order(message, state, address)

async def create_order(message_or_call, state: FSMContext, address: str):
    """Создать заказ"""
    if isinstance(message_or_call, types.Message):
        user_id = str(message_or_call.from_user.id)
        username = message_or_call.from_user.username
        bot = message_or_call.bot
    else:
        user_id = str(message_or_call.from_user.id)
        username = message_or_call.from_user.username
        bot = message_or_call.bot
    
    cart = user_carts.get(user_id, [])
    
    async with state.proxy() as data:
        delivery_method = data.get('delivery_method')
    
    if not cart:
        if isinstance(message_or_call, types.Message):
            await message_or_call.answer("❌ Корзина пуста")
        else:
            await message_or_call.answer("❌ Корзина пуста")
        await state.finish()
        return
    
    # Рассчитываем сумму
    total = 0
    order_items = []
    
    # Проверяем, есть ли в заказе товары с не точной ценой
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
                'name': product.get('subcategory', ''),  # ИСПРАВЛЕНО: сохраняем только название рубрики
                'quantity': item['quantity'],
                'price': product.get('price', 0),
                'price_per_kg': category_info.get('price_per_kg', False),
                'average_weight': category_info.get('average_weight', 0),
                'exact_price': category_info.get('exact_price', True)
            })
            
            # Уменьшаем остаток
            product['quantity'] = product.get('quantity', 0) - item['quantity']
    
    # Добавляем стоимость доставки
    if delivery_method == "delivery" and total < FREE_DELIVERY_THRESHOLD:
        total += DELIVERY_COST
    
    # Создаем заказ
    order_id = str(uuid.uuid4())[:8]
    orders_db[order_id] = {
        'id': order_id,
        'user_id': user_id,
        'username': username,
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
        'has_exact_price': has_exact_price_only
    }
    
    # Обновляем статистику пользователя
    update_user_stats(user_id, orders_db[order_id])
    
    # Сохраняем данные
    save_data()
    
    # Формируем текст заказа для админа
    order_text = f"🎉 <b>НОВЫЙ ЗАКАЗ #{order_id}</b>\n\n"
    
    for item in order_items:
        if item.get('price_per_kg'):
            if item.get('average_weight', 0) > 0:
                item_price = item['price'] * item['average_weight'] * item['quantity']
                order_text += f"• {item['name']} - {item['quantity']} шт.\n"
                order_text += f"  Примерная стоимость: ~{item_price:.0f} руб.\n"
            else:
                order_text += f"• {item['name']} - {item['quantity']} кг\n"
        else:
            # Для яиц нужно получить категорию товара для расчета множителя
            product_item = products_db.get(item['id'])
            if product_item:
                category_info_item = get_category_info(product_item.get('category', ''), product_item.get('subcategory', ''))
                multiplier = category_info_item.get('multiplier', 1)
                item_price = item['price'] * multiplier * item['quantity']
                order_text += f"• {item['name']} - {item['quantity']} уп.\n"
                order_text += f"  Сумма: {item_price} руб.\n"
    
    order_text += f"\n💰 <b>Итого:</b> {total} руб.\n"
    
    if delivery_method == 'pickup':
        order_text += f"🚗 <b>Способ:</b> Самовывоз\n"
        order_text += f"📍 <b>Адрес:</b> {PICKUP_ADDRESS}\n"
    else:
        order_text += f"🚚 <b>Способ:</b> Доставка\n"
        order_text += f"📍 <b>Адрес:</b> {address}\n"
        if total - DELIVERY_COST < FREE_DELIVERY_THRESHOLD:
            order_text += f"🚚 <b>Доставка:</b> {DELIVERY_COST} руб.\n"
    
    order_text += f"👤 <b>Покупатель:</b> @{username or 'без username'}\n"
    order_text += f"🆔 <b>ID пользователя:</b> {user_id}\n\n"
    order_text += f"💬 <b>Для связи с покупателем:</b> @{username or 'не указан'}"
    
    # Уведомляем админа с кнопкой для связи
    admin_keyboard = get_order_confirmation_keyboard(order_id)
    
    await bot.send_message(ADMIN_ID, order_text, parse_mode="HTML", reply_markup=admin_keyboard)
    
    # Очищаем корзину
    user_carts[user_id] = []
    save_data()
    
    await state.finish()
    
    # Формируем ответ для пользователя
    user_response = f"✅ <b>Заказ #{order_id} оформлен!</b>\n\n"
    
    if delivery_method == 'pickup':
        user_response += f"🚗 <b>Способ получения:</b> Самовывоз\n"
        user_response += f"📍 <b>Адрес:</b> {PICKUP_ADDRESS}\n"
    else:
        user_response += f"🚚 <b>Способ получения:</b> Доставка\n"
        user_response += f"📍 <b>Адрес доставки:</b> {address}\n"
        if total - DELIVERY_COST < FREE_DELIVERY_THRESHOLD:
            user_response += f"🚚 <b>Стоимость доставки:</b> {DELIVERY_COST} руб.\n"
    
    user_response += f"👤 <b>Ваш username:</b> @{username or 'не указан'}\n\n"
    
    if has_exact_price_only:
        user_response += f"💰 <b>Сумма к оплате:</b> {total} руб.\n\n"
    else:
        user_response += f"💰 <b>Примерная сумма:</b> ~{total} руб.\n"
        user_response += f"<i>Итоговая стоимость будет рассчитана при получении</i>\n\n"
    
    user_response += "📞 <b>С вами свяжется администратор для подтверждения заказа</b>\n\n"
    user_response += "Спасибо за заказ! 🛍️"
    
    if isinstance(message_or_call, types.Message):
        await message_or_call.answer(
            user_response,
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=(message_or_call.from_user.id == ADMIN_ID))
        )
    else:
        await message_or_call.message.answer(
            user_response,
            parse_mode="HTML",
            reply_markup=get_main_keyboard(is_admin=(message_or_call.from_user.id == ADMIN_ID))
        )

# ==================== АДМИН ФУНКЦИИ ====================
@dp.message_handler(text="📊 Статистика")
async def show_stats(message: types.Message):
    """Показать статистику"""
    if message.from_user.id != ADMIN_ID:
        return
    
    # Подсчитываем товары в наличии
    in_stock = sum(1 for p in products_db.values() if p.get('quantity', 0) > 0)
    out_of_stock = sum(1 for p in products_db.values() if p.get('quantity', 0) == 0)
    
    # Сумма всех товаров на складе
    total_stock_value = 0
    for product in products_db.values():
        if product.get('quantity', 0) > 0:
            category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
            if not category_info.get('price_per_kg'):
                item_price, note = calculate_product_price(product, product.get('quantity', 0))
                total_stock_value += item_price
    
    # Статистика по заказам
    total_orders = len(orders_db)
    new_orders = len([o for o in orders_db.values() if o.get('status') == '🆕 Новый'])
    confirmed_orders = len([o for o in orders_db.values() if o.get('status') == '✅ Подтвержден'])
    completed_orders = len([o for o in orders_db.values() if o.get('status') == '✅ Выполнен'])
    canceled_orders = len([o for o in orders_db.values() if o.get('status') in ['❌ Отклонен', '❌ Отменен']])
    postponed_orders = len([o for o in orders_db.values() if o.get('status') == '⏰ Перенесен'])
    
    # Общая сумма всех выполненных заказов
    total_revenue = sum(o.get('total', 0) for o in orders_db.values() if o.get('status') == '✅ Выполнен')
    
    stats = (
        f"📊 <b>Статистика хозяйства</b>\n\n"
        f"🛍️ <b>Товары:</b>\n"
        f"• Всего товаров: {len(products_db)}\n"
        f"• В наличии: {in_stock}\n"
        f"• Нет в наличии: {out_of_stock}\n"
        f"• Стоимость остатков: ~{total_stock_value:.0f} руб.\n\n"
        f"📦 <b>Заказы:</b>\n"
        f"• Всего заказов: {total_orders}\n"
        f"• Новых: {new_orders}\n"
        f"• Подтвержденных: {confirmed_orders}\n"
        f"• Выполненных: {completed_orders}\n"
        f"• Отмененных: {canceled_orders}\n"
        f"• Перенесенных: {postponed_orders}\n"
        f"• Общая выручка: {total_revenue} руб.\n\n"
        f"👥 <b>Клиенты:</b>\n"
        f"• Всего клиентов: {len(user_stats_db)}\n"
        f"• Активных корзин: {len([c for c in user_carts.values() if c])}\n"
        f"• Ожидают уведомлений: {sum(len(v) for v in notifications_db.values())}\n\n"
        f"📈 <b>Неопубликованных товаров:</b> {len([p for p in products_db.values() if not p.get('published')])}"
    )
    
    await message.answer(stats, parse_mode="HTML", reply_markup=get_admin_keyboard())

@dp.message_handler(text="📈 Аналитика")
async def show_analytics(message: types.Message):
    """Показать аналитику"""
    if message.from_user.id != ADMIN_ID:
        return
    
    text = "📈 <b>Аналитика хозяйства</b>\n\n"
    
    # Статистика просмотров за последние 7 дней
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
    
    text += f"👁️ <b>Просмотры товаров за неделю:</b> {total_views}\n\n"
    
    if product_views_stats:
        text += "<b>Топ 5 просматриваемых товаров (за неделю):</b>\n"
        sorted_products = sorted(product_views_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        
        for i, (product_id, views) in enumerate(sorted_products, 1):
            product = products_db.get(product_id)
            if product:
                text += f"{i}. {product.get('subcategory', '')}\n"
                text += f"   👁️ Просмотров: {views}\n"
    
    # Статистика запросов на ручное добавление за последние 7 дней
    total_requests = 0
    manual_add_stats = {}
    
    for day in manual_add_requests_db:
        if day >= week_ago:
            for product_id, requests in manual_add_requests_db[day].items():
                total_requests += requests
                if product_id not in manual_add_stats:
                    manual_add_stats[product_id] = 0
                manual_add_stats[product_id] += requests
    
    text += f"\n📝 <b>Запросы на ручное добавление за неделю:</b> {total_requests}\n\n"
    
    if manual_add_stats:
        text += "<b>Топ 5 товаров по запросам (за неделю):</b>\n"
        sorted_manual = sorted(manual_add_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        
        for i, (product_id, requests) in enumerate(sorted_manual, 1):
            product = products_db.get(product_id)
            if product:
                text += f"{i}. {product.get('subcategory', '')}\n"
                text += f"   📝 Запросов: {requests}\n"
    
    # Статистика ожидающих уведомлений
    total_waiting = sum(len(v) for v in notifications_db.values())
    text += f"\n🔔 <b>Ожидают уведомлений:</b> {total_waiting} человек\n\n"
    
    if notifications_db:
        text += "<b>Товары с подписчиками:</b>\n"
        waiting_stats = []
        for product_id, users in notifications_db.items():
            product = products_db.get(product_id)
            if product and users:
                waiting_stats.append((product, len(users)))
        
        # Сортируем по количеству ожидающих
        waiting_stats.sort(key=lambda x: x[1], reverse=True)
        
        for i, (product, count) in enumerate(waiting_stats[:5], 1):
            text += f"{i}. {product.get('subcategory', '')}\n"
            text += f"   👥 Ожидают: {count} человек\n"
    
    # Статистика просмотров за сегодня
    today_str = str(today)
    if today_str in product_views_db:
        today_views = sum(product_views_db[today_str].values())
        text += f"\n📊 <b>Просмотров сегодня:</b> {today_views}\n"
    
    # Статистика запросов на ручное добавление за сегодня
    if today_str in manual_add_requests_db:
        today_requests = sum(manual_add_requests_db[today_str].values())
        text += f"📝 <b>Запросов на ручное добавление сегодня:</b> {today_requests}\n"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_admin_keyboard())

@dp.message_handler(text="👥 Клиенты")
async def show_clients(message: types.Message):
    """Показать статистику по клиентам"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if not user_stats_db:
        await message.answer("📭 Нет данных о клиентах.", reply_markup=get_admin_keyboard())
        return
    
    text = "👥 <b>Статистика по клиентам</b>\n\n"
    
    # Сортируем клиентов по сумме покупок
    sorted_clients = sorted(user_stats_db.items(), 
                          key=lambda x: x[1].get('total_spent', 0), 
                          reverse=True)
    
    total_clients = len(sorted_clients)
    total_spent = sum(stats.get('total_spent', 0) for _, stats in sorted_clients)
    total_orders = sum(stats.get('completed_orders', 0) for _, stats in sorted_clients)
    
    text += f"📊 <b>Общая статистика:</b>\n"
    text += f"• Всего клиентов: {total_clients}\n"
    text += f"• Всего заказов: {total_orders}\n"
    text += f"• Общая сумма покупок: {total_spent} руб.\n"
    text += f"• Средний чек: {int(total_spent / total_orders) if total_orders > 0 else 0} руб.\n\n"
    
    text += "<b>Топ 5 клиентов по сумме покупок:</b>\n"
    
    for i, (user_id, stats) in enumerate(sorted_clients[:5], 1):
        username = stats.get('username', 'без username')
        completed = stats.get('completed_orders', 0)
        canceled = stats.get('canceled_orders', 0)
        postponed = stats.get('postponed_orders', 0)
        total_spent_client = stats.get('total_spent', 0)
        
        text += f"{i}. @{username}\n"
        text += f"   Выполнено: {completed} | Отменено: {canceled} | Перенесено: {postponed}\n"
        text += f"   Потрачено: {total_spent_client} руб.\n"
        
        # Добавляем кнопку для просмотра заказов клиента
        if i == 1:  # Только для первого клиента показываем пример кнопки
            keyboard = get_client_orders_keyboard(user_id)
            await message.answer(text, parse_mode="HTML", reply_markup=keyboard)
            return
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_admin_keyboard())

# ==================== БЫСТРОЕ ПОПОЛНЕНИЕ ОСТАТКОВ ====================
@dp.message_handler(text="📦 Пополнить остатки")
async def add_quantity_start(message: types.Message):
    """Начать пополнение остатков"""
    if message.from_user.id != ADMIN_ID:
        return
    
    if not products_db:
        await message.answer("📭 В хозяйстве пока нет товаров.", reply_markup=get_admin_keyboard())
        return
    
    text = "📦 <b>Управление остатками товаров</b>\n\n"
    text += "Выберите товар для изменения остатков:\n\n"
    
    # Группируем товары по категориям и подкатегориям
    products_by_category = {}
    for product in products_db.values():
        category = product.get('category', '')
        subcategory = product.get('subcategory', '')
        key = f"{category}|{subcategory}"
        
        if key not in products_by_category:
            products_by_category[key] = product  # Берем только первый товар из каждой рубрики
    
    # Показываем список товаров
    for i, (key, product) in enumerate(products_by_category.items(), 1):
        category_info = get_category_info(product.get('category', ''), product.get('subcategory', ''))
        text += f"{i}. <b>{product.get('subcategory', '')}</b>\n"
        text += f"   📦 Остаток: {product.get('quantity', 0)} {category_info.get('unit', 'шт')}\n"
        text += f"   💰 Цена: {product.get('price', 0)} руб."
        
        if category_info.get('price_per_kg'):
            text += "/кг\n"
        else:
            text += "/шт\n"
        
        text += f"   🔹 ID: <code>{product.get('id')}</code>\n\n"
    
    text += "Нажмите на кнопку с товаром ниже для изменения остатков:"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_products_for_adjust_keyboard())

@dp.callback_query_handler(lambda c: c.data == "no_products")
async def no_products_callback(call: types.CallbackQuery):
    """Обработка кнопки "Нет товаров" """
    await call.answer("📭 В хозяйстве пока нет товаров", show_alert=True)

@dp.message_handler(text="➕ Добавить товар")
async def add_product_start(message: types.Message):
    """Начать добавление товара"""
    if message.from_user.id != ADMIN_ID:
        return
    
    await AddProduct.category.set()
    await message.answer("📝 Выберите категорию:", reply_markup=get_categories_keyboard(is_admin=True))

@dp.message_handler(state=AddProduct.category)
async def process_category_state(message: types.Message, state: FSMContext):
    """Обработать категорию"""
    if message.text not in CATEGORIES:
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
        data['category'] = message.text
    
    await AddProduct.next()
    await message.answer("📂 Выберите рубрику:", reply_markup=get_subcategories_keyboard(message.text, is_admin=True))

@dp.message_handler(state=AddProduct.subcategory)
async def process_subcategory_state(message: types.Message, state: FSMContext):
    """Обработать рубрику"""
    if message.text == "👑 Панель админа":
        await state.finish()
        await message.answer("❌ Добавление отменено", reply_markup=get_admin_keyboard())
        return
    
    if message.text == "↩️ К категориям":
        await AddProduct.category.set()
        await message.answer("↩️ Выберите категорию:", reply_markup=get_categories_keyboard(is_admin=True))
        return
    
    # ИСПРАВЛЕНО: обрабатываем только название рубрики
    async with state.proxy() as data:
        data['subcategory'] = message.text
    
    # Проверяем, есть ли уже товар в этой рубрике
    category = data['category']
    subcategory = data['subcategory']
    
    existing_products = [p for p in products_db.values() 
                        if p.get('category') == category and p.get('subcategory') == subcategory]
    
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
    """Обработать цену"""
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
        
        async with state.proxy() as data:
            data['price'] = price
        
        await AddProduct.next()
        await message.answer("📦 Введите количество на складе:")
    except:
        await message.answer("❌ Введите корректную цену (положительное число)!")

@dp.message_handler(state=AddProduct.quantity)
async def process_quantity_state(message: types.Message, state: FSMContext):
    """Обработать количество"""
    try:
        quantity = int(message.text)
        if quantity < 0:
            raise ValueError
        
        async with state.proxy() as data:
            data['quantity'] = quantity
        
        await AddProduct.next()
        await message.answer("📸 Отправьте фото товара:")
    except:
        await message.answer("❌ Введите корректное количество (неотрицательное число)!")

@dp.message_handler(content_types=types.ContentType.PHOTO, state=AddProduct.photo)
async def process_photo_state(message: types.Message, state: FSMContext):
    """Обработать фото"""
    async with state.proxy() as data:
        # Сохраняем товар
        product_id = str(uuid.uuid4())[:8]
        products_db[product_id] = {
            'id': product_id,
            'category': data['category'],
            'subcategory': data['subcategory'],
            'price': data['price'],
            'quantity': data['quantity'],
            'photo': message.photo[-1].file_id,
            'published': False,
            'created_at': datetime.now().strftime("%d.%m.%Y %H:%M")
        }
        
        # Сохраняем данные
        save_data()
        
        # Отправляем информацию о товаре
        await message.answer_photo(
            message.photo[-1].file_id,
            caption=f"✅ <b>Товар добавлен!</b>\n\n" + format_product_info(products_db[product_id]),
            parse_mode="HTML"
        )
    
    await state.finish()
    await message.answer("✅ Товар сохранен! Теперь можете опубликовать его в канале.", reply_markup=get_admin_keyboard())

# ==================== ПУБЛИКАЦИЯ В КАНАЛЕ ====================
@dp.message_handler(text="📤 Опубликовать в канал")
async def publish_to_channel(message: types.Message):
    """Опубликовать товары в канал"""
    if message.from_user.id != ADMIN_ID:
        return
    
    unpublished = [p for p in products_db.values() if not p.get('published') and p.get('quantity', 0) > 0]
    
    if not unpublished:
        await message.answer("📭 Нет новых товаров для публикации.", reply_markup=get_admin_keyboard())
        return
    
    published_count = 0
    
    for product in unpublished[:3]:  # Публикуем до 3 товаров
        try:
            caption = format_product_info(product)
            caption += "\n\n👇 Нажмите кнопку ниже, чтобы заказать"
            
            await bot.send_photo(
                CHANNEL_ID,
                product['photo'],
                caption=caption,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("🛒 Заказать", callback_data=f"channel_order_{product['id']}")
                )
            )
            
            product['published'] = True
            published_count += 1
            
        except Exception as e:
            await message.answer(f"❌ Ошибка при публикации: {str(e)}", reply_markup=get_admin_keyboard())
            break
    
    # Сохраняем данные после публикации
    save_data()
    
    await message.answer(f"✅ Опубликовано {published_count} товаров в канал {CHANNEL_ID}", reply_markup=get_admin_keyboard())

# ==================== МОИ ЗАКАЗЫ ====================
@dp.message_handler(text="📦 Мои заказы")
async def show_user_orders(message: types.Message):
    """Показать заказы пользователя"""
    user_id = str(message.from_user.id)
    
    # Ищем заказы пользователя
    user_orders = [order for order in orders_db.values() if order.get('user_id') == user_id]
    
    if not user_orders:
        await message.answer(
            "📭 У вас пока нет заказов.\n\n"
            "Совершите покупки в нашем каталоге и оформите заказ! 🛍️",
            reply_markup=get_main_keyboard(is_admin=(message.from_user.id == ADMIN_ID))
        )
        return
    
    # Сортируем заказы по дате (новые первыми)
    user_orders.sort(key=lambda x: x.get('created_at', ''), reverse=True)
    
    # Формируем ответ
    orders_text = "📦 <b>Ваши заказы:</b>\n\n"
    
    for i, order in enumerate(user_orders[:10], 1):  # Показываем последние 10 заказов
        orders_text += f"<b>Заказ #{order['id']}</b>\n"
        orders_text += f"📅 <b>Дата:</b> {order.get('created_at', 'Не указана')}\n"
        orders_text += f"📦 <b>Статус:</b> {order.get('status', 'Не указан')}\n"
        
        # Способ получения
        if order.get('delivery_method') == 'pickup':
            orders_text += f"🚗 <b>Способ:</b> Самовывоз\n"
            orders_text += f"📍 <b>Адрес:</b> {PICKUP_ADDRESS}\n"
        else:
            orders_text += f"🚚 <b>Способ:</b> Доставка\n"
            orders_text += f"📍 <b>Адрес:</b> {order.get('address', 'Не указан')}\n"
        
        if order.get('total', 0) > 0:
            orders_text += f"💰 <b>Сумма:</b> {order.get('total', 0)} руб.\n"
        
        orders_text += "─" * 20 + "\n\n"
    
    if len(user_orders) > 10:
        orders_text += f"\n<i>Показаны последние 10 из {len(user_orders)} заказов</i>"
    
    await message.answer(orders_text, parse_mode="HTML", reply_markup=get_main_keyboard(is_admin=(message.from_user.id == ADMIN_ID)))

# ==================== ИНФОРМАЦИЯ О ХОЗЯЙСТВЕ ====================
@dp.message_handler(text="ℹ️ О нас")
async def show_about(message: types.Message):
    """Показать информацию о хозяйстве"""
    about_text = (
        "🏡 <b>Русский ТАЙ - Семейная ферма</b>\n\n"
        "Мы находимся в экологически чистом месте Керженского заповедника.\n\n"
        "Предлагаем свежие и натуральные продукты:\n"
        "• 🥚 Свежие яйца разных видов\n"
        "• 🍗 Качественное мясо птицы\n"
        "• 🥫 Вкусные полуфабрикаты\n\n"
        "📞 <b>Контакты:</b>\n"
        f"• Адрес самовывоза: {PICKUP_ADDRESS}\n"
        f"• Телефон: {CONTACT_PHONES}\n\n"
        "🚚 <b>Доставка:</b>\n"
        f"• Стоимость: {DELIVERY_COST} руб. (бесплатно от {FREE_DELIVERY_THRESHOLD} руб.)\n"
        "• По городу: 1-2 дня\n\n"
        "⏰ <b>Работаем:</b> ежедневно с 9:00 до 21:00\n\n"
        "💬 <b>После оформления заказа с вами свяжется администратор для подтверждения</b>"
    )
    
    await message.answer(about_text, parse_mode="HTML", reply_markup=get_main_keyboard(is_admin=(message.from_user.id == ADMIN_ID)))

# ==================== УВЕДОМЛЕНИЯ ====================
@dp.callback_query_handler(lambda c: c.data.startswith('notify_'))
async def notify_product(call: types.CallbackQuery):
    """Подписаться на уведомление о появлении товара"""
    product_id = call.data.split('_')[1]
    product = products_db.get(product_id)
    
    if not product:
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Проверяем, не админ ли
    if call.from_user.id == ADMIN_ID:
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    
    user_id = str(call.from_user.id)
    
    if product_id not in notifications_db:
        notifications_db[product_id] = []
    
    # Проверяем, не подписан ли уже пользователь
    if user_id not in notifications_db[product_id]:
        notifications_db[product_id].append(user_id)
        save_data()
        await call.answer("🔔 Подписка оформлена! Вы будете уведомлены о появлении товара", show_alert=True)
    else:
        await call.answer("ℹ️ Вы уже подписаны на уведомление", show_alert=True)

async def send_notifications(product_id: str):
    """Отправить уведомления подписчикам"""
    product = products_db.get(product_id)
    if not product or product_id not in notifications_db:
        return
    
    for user_id in notifications_db[product_id]:
        try:
            await bot.send_message(
                user_id,
                f"🔔 <b>Товар появился в наличии!</b>\n\n"
                f"{product.get('subcategory', '')}\n"
                f"📦 Остаток: {product.get('quantity', 0)} шт\n\n"
                f"Скорее заказывайте! 🛍️",
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")
    
    # Очищаем список уведомлений для этого товара
    notifications_db[product_id] = []
    save_data()

# ==================== ОБРАБОТКА КНОПОК ИЗ КАНАЛА ====================
@dp.callback_query_handler(lambda c: c.data.startswith('channel_order_'))
async def process_channel_order(call: types.CallbackQuery):
    """Обработать заказ из канала"""
    product_id = call.data.split('_')[-1]
    product = products_db.get(product_id)
    
    if not product:
        # Отвечаем пользователю в личном сообщении, а не в канале
        await call.answer("❌ Товар не найден", show_alert=True)
        return
    
    # Проверяем, не админ ли
    if call.from_user.id == ADMIN_ID:
        await call.answer("👑 Вы в режиме админа. Переключитесь в режим покупателя.", show_alert=True)
        return
    
    # Добавляем в корзину
    user_id = str(call.from_user.id)
    if user_id not in user_carts:
        user_carts[user_id] = []
    
    # Проверяем, есть ли уже товар из этой же рубрики в корзине
    product_category = product.get('category', '')
    product_subcategory = product.get('subcategory', '')
    
    # Ищем товар в корзине
    item_index = -1
    for i, item in enumerate(user_carts[user_id]):
        if item['id'] == product_id:
            item_index = i
            break
    
    if item_index >= 0:
        user_carts[user_id][item_index]['quantity'] += 1
    else:
        # ИСПРАВЛЕНО: сохраняем только название рубрики
        user_carts[user_id].append({
            'id': product_id,
            'name': product.get('subcategory', ''),
            'quantity': 1
        })
    
    save_data()
    await call.answer(f"✅ {product.get('subcategory', 'Товар')} добавлен в корзину!", show_alert=True)
    
    # Получаем username бота
    try:
        bot_info = await call.bot.get_me()
        bot_username = bot_info.username
    except:
        bot_username = "RusskiyTAY_bot"
    
    # Отправляем сообщение пользователю в ЛИЧНЫЕ СООБЩЕНИЯ, а не в канал
    try:
        await call.bot.send_message(
            call.from_user.id,
            f"🛒 <b>Товар добавлен в корзину!</b>\n\n"
            f"<b>{product.get('subcategory', '')}</b> "
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
        # Если не удалось отправить личное сообщение, показываем просто уведомление
        await call.answer("✅ Товар добавлен! Перейдите в бота для оформления заказа.", show_alert=True)

# ==================== ЗАПУСК БОТА ====================
async def on_startup(dp):
    """Действия при запуске"""
    # Загружаем данные из файла
    load_data()
    
    print("=" * 50)
    print("🤖 БОТ СЕМЕЙНОЙ ФЕРМЫ РУССКИЙ ТАЙ")
    print("=" * 50)
    print(f"👑 Админ: {ADMIN_ID}")
    print(f"📢 Канал: {CHANNEL_ID}")
    print(f"🛍️ Товаров: {len(products_db)}")
    print(f"📦 Заказов: {len(orders_db)}")
    print(f"🛒 Корзин: {len(user_carts)}")
    print(f"🔔 Уведомлений: {sum(len(v) for v in notifications_db.values())}")
    print(f"👁️ Статистика просмотров: {sum(sum(day.values()) for day in product_views_db.values())}")
    print(f"👥 Клиентов в статистике: {len(user_stats_db)}")
    print("=" * 50)
    
    try:
        me = await bot.get_me()
        print(f"✅ Бот запущен: @{me.username}")
        await bot.send_message(ADMIN_ID, "🤖 Бот семейной фермы Русский ТАЙ запущен и готов к работе!")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    from aiogram import executor
    import asyncio
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    print("🚀 Запуск бота...")
    
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
