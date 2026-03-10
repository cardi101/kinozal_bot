from aiogram.fsm.state import State, StatesGroup


class EditInputState(StatesGroup):
    waiting_name = State()
    waiting_keywords = State()
    waiting_years = State()
