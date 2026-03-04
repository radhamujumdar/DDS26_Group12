import requests
from fastapi import HTTPException

from models import REQ_ERROR_STR


class StockClient:
    def __init__(self, session: requests.Session, gateway_url: str):
        self.session = session
        self.base_url = f"{gateway_url}/stock"

    def find_item(self, item_id: str) -> requests.Response:
        return self._get(f"/find/{item_id}")

    def subtract(self, item_id: str, amount: int) -> requests.Response:
        return self._post(f"/subtract/{item_id}/{amount}")

    def add(self, item_id: str, amount: int) -> requests.Response:
        return self._post(f"/add/{item_id}/{amount}")

    def _post(self, path: str) -> requests.Response:
        try:
            return self.session.post(f"{self.base_url}{path}")
        except requests.exceptions.RequestException as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc

    def _get(self, path: str) -> requests.Response:
        try:
            return self.session.get(f"{self.base_url}{path}")
        except requests.exceptions.RequestException as exc:
            raise HTTPException(status_code=400, detail=REQ_ERROR_STR) from exc
