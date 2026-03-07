import httpx
import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=10.0) as session:
        yield session
