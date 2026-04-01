from msgspec import Struct


class StockValue(Struct):
    stock: int
    price: int
