import os

import requests


def _gateway_url() -> str:
    return os.getenv("DDS_GATEWAY_URL", "http://127.0.0.1:8000").rstrip("/")


########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> dict:
    return requests.post(f"{_gateway_url()}/stock/item/create/{price}").json()


def find_item(item_id: str) -> dict:
    return requests.get(f"{_gateway_url()}/stock/find/{item_id}").json()


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{_gateway_url()}/stock/add/{item_id}/{amount}").status_code


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{_gateway_url()}/stock/subtract/{item_id}/{amount}").status_code


########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{_gateway_url()}/payment/pay/{user_id}/{amount}").status_code


def create_user() -> dict:
    return requests.post(f"{_gateway_url()}/payment/create_user").json()


def find_user(user_id: str) -> dict:
    return requests.get(f"{_gateway_url()}/payment/find_user/{user_id}").json()


def add_credit_to_user(user_id: str, amount: float) -> int:
    return requests.post(f"{_gateway_url()}/payment/add_funds/{user_id}/{amount}").status_code


########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> dict:
    return requests.post(f"{_gateway_url()}/orders/create/{user_id}").json()


def add_item_to_order(order_id: str, item_id: str, quantity: int) -> int:
    return requests.post(f"{_gateway_url()}/orders/addItem/{order_id}/{item_id}/{quantity}").status_code


def find_order(order_id: str) -> dict:
    return requests.get(f"{_gateway_url()}/orders/find/{order_id}").json()


def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{_gateway_url()}/orders/checkout/{order_id}")


########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    return 200 <= status_code < 300


def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500
