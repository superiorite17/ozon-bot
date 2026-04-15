import asyncio
import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

import os
BOT_TOKEN = os.getenv("BOT_TOKEN")

ACQUIRING_RATE = 0.02
LAST_MILE_RUB = 25.0
NONLOCAL_RATE = 0.08  # ожидаемая надбавка за нелокальность как % от цены * доля нелокальных продаж

COMMISSIONS_FILE = "commissions.xlsx"
ORDERS_FILE = "orders.csv"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

commissions_df: Optional[pd.DataFrame] = None


@dataclass
class OzonStats:
    local_share: float
    nonlocal_share: float


ozon_stats = OzonStats(local_share=1.0, nonlocal_share=0.0)

# Простое состояние пользователя без FSM
user_state = {}


def new_calc_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Новый расчёт")]],
        resize_keyboard=True
    )


def load_commissions(path: str) -> pd.DataFrame:
    raw = pd.read_excel(path, header=None)
    header = raw.iloc[0].tolist()
    df = raw.iloc[1:].copy()
    df.columns = header
    df = df.reset_index(drop=True)

    df.columns = [
        "category",
        "product_type",
        "fbo_100",
        "fbo_300",
        "fbo_1500",
        "fbo_5000",
        "fbo_10000",
        "fbo_10000_plus",
        "fbo_fresh_100",
        "fbo_fresh_300",
        "fbo_fresh_1500",
        "fbo_fresh_5000",
        "fbo_fresh_10000",
        "fbo_fresh_10000_plus",
        "fbs_100",
        "fbs_300",
        "fbs_1500",
        "fbs_5000",
        "fbs_10000",
        "fbs_10000_plus",
        "rfbs_1500",
        "rfbs_5000",
        "rfbs_10000",
        "rfbs_10000_plus",
    ]

    numeric_cols = [
        "fbo_100", "fbo_300", "fbo_1500", "fbo_5000", "fbo_10000", "fbo_10000_plus",
        "fbs_100", "fbs_300", "fbs_1500", "fbs_5000", "fbs_10000", "fbs_10000_plus",
    ]

    for col in numeric_cols:
        s = (
            df[col]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
        df[col] = pd.to_numeric(s, errors="coerce")

        # Если значение > 1, считаем что это проценты вида 41 -> 0.41
        # Если <= 1, считаем что это уже доля вида 0.41
        df.loc[df[col] > 1, col] = df.loc[df[col] > 1, col] / 100.0

    df["category"] = df["category"].astype(str).str.strip()
    df["product_type"] = df["product_type"].astype(str).str.strip()
    return df


def load_orders_stats(path: str) -> OzonStats:
    try:
        df = pd.read_csv(path, sep=";")
    except Exception:
        return OzonStats(local_share=1.0, nonlocal_share=0.0)

    required_cols = ["Кластер отгрузки", "Кластер доставки"]
    for col in required_cols:
        if col not in df.columns:
            return OzonStats(local_share=1.0, nonlocal_share=0.0)

    valid = df.dropna(subset=required_cols).copy()
    if valid.empty:
        return OzonStats(local_share=1.0, nonlocal_share=0.0)

    valid["is_local"] = (
        valid["Кластер отгрузки"].astype(str).str.strip()
        == valid["Кластер доставки"].astype(str).str.strip()
    )

    local_share = float(valid["is_local"].mean())
    return OzonStats(local_share=local_share, nonlocal_share=1.0 - local_share)


def get_commission_rate(row: pd.Series, scheme: str, tentative_price: float) -> float:
    prefix = "fbo" if scheme == "FBO" else "fbs"

    if tentative_price <= 100:
        return float(row[f"{prefix}_100"])
    elif tentative_price <= 300:
        return float(row[f"{prefix}_300"])
    elif tentative_price <= 1500:
        return float(row[f"{prefix}_1500"])
    elif tentative_price <= 5000:
        return float(row[f"{prefix}_5000"])
    elif tentative_price <= 10000:
        return float(row[f"{prefix}_10000"])
    return float(row[f"{prefix}_10000_plus"])


def get_logistics(volume_liters: float, tentative_price: float, scheme: str) -> float:
    # Рабочая упрощённая логика по твоим тарифным диапазонам
    high_price = tentative_price > 300

    if volume_liters <= 0.2:
        base = 56.0 if high_price else 17.28
    elif volume_liters <= 0.4:
        base = 63.0 if high_price else 19.32
    elif volume_liters <= 0.6:
        base = 67.0 if high_price else 21.35
    elif volume_liters <= 1.0:
        base = 67.0 if high_price else 23.38
    elif volume_liters <= 1.25:
        base = 71.0 if high_price else 25.42
    elif volume_liters <= 3.0:
        base = 74.0 if high_price else 31.52
    elif volume_liters <= 5.0:
        base = 89.0 if high_price else 38.63
    else:
        base = 100.0 if high_price else 45.0

    return base


def get_nonlocal_markup(tentative_price: float) -> float:
    return tentative_price * NONLOCAL_RATE * ozon_stats.nonlocal_share


def find_rows_by_product_type(product_type: str) -> pd.DataFrame:
    global commissions_df
    if commissions_df is None:
        return pd.DataFrame()

    pt = product_type.strip().lower()

    exact = commissions_df[commissions_df["product_type"].str.lower() == pt]
    if not exact.empty:
        return exact.reset_index(drop=True)

    partial = commissions_df[
        commissions_df["product_type"].str.lower().str.contains(pt, na=False)
    ]
    return partial.reset_index(drop=True)


def calculate_price(
    row: pd.Series,
    scheme: str,
    cost: float,
    markup_percent: float,
    volume: float,
    extra_percent: float,
    ads_percent: float,
    handling_rub: float,
) -> dict:
    """
    Формула:
    Цена = (Себестоимость + Себестоимость*Наценка + Логистика + Последняя миля + Нелокальность + Обработка(FBS))
           / (1 - Комиссия - Эквайринг - Доп.расходы - Реклама)
    """

    target_profit = cost * markup_percent

    # Итерации нужны, потому что комиссия / логистика / нелокальность зависят от цены
    price = cost + target_profit + 300.0

    for _ in range(5):
        commission_rate = get_commission_rate(row, scheme, price)
        logistics = get_logistics(volume, price, scheme)
        nonlocal_markup = get_nonlocal_markup(price)

        fixed_costs = cost + target_profit + logistics + LAST_MILE_RUB + nonlocal_markup
        if scheme == "FBS":
            fixed_costs += handling_rub

        percent_costs = commission_rate + ACQUIRING_RATE + extra_percent + ads_percent
        denominator = 1.0 - percent_costs

        if denominator <= 0:
            raise ValueError("Слишком большие процентные расходы")

        price = fixed_costs / denominator

    commission_rate = get_commission_rate(row, scheme, price)
    logistics = get_logistics(volume, price, scheme)
    nonlocal_markup = get_nonlocal_markup(price)

    commission = price * commission_rate
    acquiring = price * ACQUIRING_RATE
    extra_cost = price * extra_percent
    ads_cost = price * ads_percent
    handling = handling_rub if scheme == "FBS" else 0.0

    total_costs = (
        cost
        + logistics
        + LAST_MILE_RUB
        + nonlocal_markup
        + handling
        + commission
        + acquiring
        + extra_cost
        + ads_cost
    )

    profit = price - total_costs
    margin_to_cost = profit / cost if cost else 0.0

    return {
        "scheme": scheme,
        "price": round(price),
        "commission_rate": commission_rate,
        "logistics": logistics,
        "last_mile": LAST_MILE_RUB,
        "nonlocal_markup": nonlocal_markup,
        "acquiring": acquiring,
        "extra_cost": extra_cost,
        "ads_cost": ads_cost,
        "handling": handling,
        "profit": profit,
        "margin_to_cost": margin_to_cost,
    }


def start_calc(user_id: int):
    user_state[user_id] = {
        "step": "cost"
    }
    return (
        "Начинаем новый расчёт.\n\n"
        "Отправь себестоимость в рублях.\n"
        "Пример: 500"
    )


@dp.message()
async def handler(message: types.Message):
    user_id = message.from_user.id
    text = (message.text or "").strip()

    print(f"{user_id}: {text}")

    # Универсальные команды
    if text in ["/start"]:
        user_state.pop(user_id, None)
        await message.answer(
            "Привет.\n\n"
            "Я считаю цены для FBO и FBS по типу товара.\n\n"
            "Команды:\n"
            "/calc — новый расчёт\n"
            "/cancel — отменить текущий расчёт",
            reply_markup=ReplyKeyboardRemove()
        )
        return

    if text in ["/calc", "Новый расчёт"]:
        reply = start_calc(user_id)
        await message.answer(reply, reply_markup=ReplyKeyboardRemove())
        return

    if text == "/cancel":
        user_state.pop(user_id, None)
        await message.answer(
            "Текущий расчёт отменён.\n"
            "Нажми /calc или кнопку «Новый расчёт».",
            reply_markup=new_calc_keyboard()
        )
        return

    # Если пользователь не начал расчёт
    if user_id not in user_state:
        await message.answer(
            "Я пока ничего не считаю.\n"
            "Нажми /calc чтобы начать новый расчёт.",
            reply_markup=new_calc_keyboard()
        )
        return

    state = user_state[user_id]

    try:
        # 1. Себестоимость
        if state["step"] == "cost":
            state["cost"] = float(text.replace(",", "."))
            state["step"] = "markup"
            await message.answer("Теперь отправь желаемую наценку в %. Например: 15")
            return

        # 2. Наценка
        if state["step"] == "markup":
            state["markup"] = float(text.replace(",", ".")) / 100.0
            state["step"] = "volume"
            await message.answer("Теперь отправь объём в литрах. Например: 0.8")
            return

        # 3. Объём
        if state["step"] == "volume":
            state["volume"] = float(text.replace(",", "."))
            state["step"] = "product_type"
            await message.answer("Теперь отправь тип товара. Например: Автосканер")
            return

        # 4. Тип товара
        if state["step"] == "product_type":
            matches = find_rows_by_product_type(text)

            if matches.empty:
                await message.answer(
                    "Не нашёл такой тип товара.\n"
                    "Попробуй написать чуть иначе или короче."
                )
                return

            if len(matches) == 1:
                row = matches.iloc[0]
                state["selected_category"] = row["category"]
                state["selected_product_type"] = row["product_type"]
                state["step"] = "extra_percent"

                await message.answer(
                    f"Нашёл товар:\n"
                    f"Категория: {row['category']}\n"
                    f"Тип товара: {row['product_type']}\n\n"
                    f"Теперь отправь доп. расходы в %. Например: 3"
                )
                return

            # Несколько совпадений
            limited = matches.head(10).to_dict("records")
            state["product_matches"] = limited
            state["step"] = "product_choice"

            options = []
            for i, row in enumerate(limited, start=1):
                options.append(f"{i}. {row['category']} | {row['product_type']}")

            await message.answer(
                "Нашёл несколько вариантов.\n"
                "Отправь номер нужного варианта:\n\n" + "\n".join(options)
            )
            return

        # 5. Выбор варианта товара
        if state["step"] == "product_choice":
            try:
                choice = int(text)
            except Exception:
                await message.answer("Нужен номер варианта. Например: 1")
                return

            matches = state.get("product_matches", [])
            if not matches or choice < 1 or choice > len(matches):
                await message.answer("Такого номера нет. Отправь номер из списка.")
                return

            row = matches[choice - 1]
            state["selected_category"] = row["category"]
            state["selected_product_type"] = row["product_type"]
            state["step"] = "extra_percent"

            await message.answer(
                f"Выбрано:\n"
                f"Категория: {row['category']}\n"
                f"Тип товара: {row['product_type']}\n\n"
                f"Теперь отправь доп. расходы в %. Например: 3"
            )
            return

        # 6. Доп. расходы
        if state["step"] == "extra_percent":
            state["extra_percent"] = float(text.replace(",", ".")) / 100.0
            state["step"] = "ads_percent"
            await message.answer("Теперь отправь рекламу в %. Например: 12")
            return

        # 7. Реклама
        if state["step"] == "ads_percent":
            state["ads_percent"] = float(text.replace(",", ".")) / 100.0
            state["step"] = "handling"
            await message.answer("Теперь отправь обработку отправления для FBS в рублях. Например: 30")
            return

        # 8. Обработка FBS и финальный расчёт
        if state["step"] == "handling":
            state["handling"] = float(text.replace(",", "."))

            category = state["selected_category"]
            product_type = state["selected_product_type"]

            row_df = commissions_df[
                (commissions_df["category"] == category) &
                (commissions_df["product_type"] == product_type)
            ]

            if row_df.empty:
                await message.answer(
                    "Не удалось найти выбранный товар в таблице.\n"
                    "Нажми /calc чтобы начать заново.",
                    reply_markup=new_calc_keyboard()
                )
                user_state.pop(user_id, None)
                return

            row = row_df.iloc[0]

            fbo = calculate_price(
                row=row,
                scheme="FBO",
                cost=state["cost"],
                markup_percent=state["markup"],
                volume=state["volume"],
                extra_percent=state["extra_percent"],
                ads_percent=state["ads_percent"],
                handling_rub=0.0,
            )

            fbs = calculate_price(
                row=row,
                scheme="FBS",
                cost=state["cost"],
                markup_percent=state["markup"],
                volume=state["volume"],
                extra_percent=state["extra_percent"],
                ads_percent=state["ads_percent"],
                handling_rub=state["handling"],
            )

            answer = (
                f"Категория: {category}\n"
                f"Тип товара: {product_type}\n"
                f"Себестоимость: {state['cost']:.2f} ₽\n"
                f"Наценка: {state['markup'] * 100:.1f}%\n"
                f"Объём: {state['volume']:.2f} л\n"
                f"Нелокальные продажи: {ozon_stats.nonlocal_share * 100:.1f}%\n\n"

                f"FBO:\n"
                f"Цена: {fbo['price']} ₽\n"
                f"Прибыль: {fbo['profit']:.2f} ₽\n"
                f"Наценка к себестоимости: {fbo['margin_to_cost'] * 100:.1f}%\n"
                f"Комиссия: {fbo['commission_rate'] * 100:.1f}%\n"
                f"Логистика: {fbo['logistics']:.2f} ₽\n"
                f"Последняя миля: {fbo['last_mile']:.2f} ₽\n"
                f"Эквайринг: {fbo['acquiring']:.2f} ₽\n"
                f"Доп. расходы: {fbo['extra_cost']:.2f} ₽\n"
                f"Реклама: {fbo['ads_cost']:.2f} ₽\n"
                f"Нелокальность: {fbo['nonlocal_markup']:.2f} ₽\n\n"

                f"FBS:\n"
                f"Цена: {fbs['price']} ₽\n"
                f"Прибыль: {fbs['profit']:.2f} ₽\n"
                f"Наценка к себестоимости: {fbs['margin_to_cost'] * 100:.1f}%\n"
                f"Комиссия: {fbs['commission_rate'] * 100:.1f}%\n"
                f"Логистика: {fbs['logistics']:.2f} ₽\n"
                f"Последняя миля: {fbs['last_mile']:.2f} ₽\n"
                f"Эквайринг: {fbs['acquiring']:.2f} ₽\n"
                f"Доп. расходы: {fbs['extra_cost']:.2f} ₽\n"
                f"Реклама: {fbs['ads_cost']:.2f} ₽\n"
                f"Нелокальность: {fbs['nonlocal_markup']:.2f} ₽\n"
                f"Обработка отправления: {fbs['handling']:.2f} ₽"
            )

            await message.answer(answer, reply_markup=new_calc_keyboard())
            user_state.pop(user_id, None)
            return

    except ValueError:
        current_step = state.get("step")

        prompts = {
            "cost": "Сейчас я жду себестоимость. Например: 500",
            "markup": "Сейчас я жду наценку в %. Например: 15",
            "volume": "Сейчас я жду объём в литрах. Например: 0.8",
            "product_type": "Сейчас я жду тип товара. Например: Автосканер",
            "product_choice": "Сейчас я жду номер варианта. Например: 1",
            "extra_percent": "Сейчас я жду доп. расходы в %. Например: 3",
            "ads_percent": "Сейчас я жду рекламу в %. Например: 12",
            "handling": "Сейчас я жду обработку FBS в рублях. Например: 30",
        }

        await message.answer(prompts.get(current_step, "Нажми /calc чтобы начать заново."))
        return
    except Exception as e:
        await message.answer(
            f"Ошибка расчёта: {e}\n"
            f"Нажми /calc чтобы начать заново.",
            reply_markup=new_calc_keyboard()
        )
        user_state.pop(user_id, None)
        return


async def main():
    global commissions_df, ozon_stats

    print("Ozon калькулятор запущен")

    if os.path.exists(COMMISSIONS_FILE):
        commissions_df = load_commissions(COMMISSIONS_FILE)
        print("commissions.xlsx загружен")
    else:
        print("Не найден commissions.xlsx")

    if os.path.exists(ORDERS_FILE):
        ozon_stats = load_orders_stats(ORDERS_FILE)
        print(f"orders.csv загружен, нелокальные: {ozon_stats.nonlocal_share * 100:.1f}%")
    else:
        print("orders.csv не найден")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())