import requests
from fastapi import HTTPException

from models import REQ_ERROR_STR


class PaymentClient:
    def __init__(self, session: requests.Session, gateway_url: str):
        self.session = session
        self.base_url = f"{gateway_url}/payment"

    def pay(self, user_id: str, amount: int) -> requests.Response:
        return self._post(f"/pay/{user_id}/{amount}")

    def _post(self, path: str) -> requests.Response:
        try:
            return self.session.post(f"{self.base_url}{path}")
        except requests.exceptions.RequestException as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
