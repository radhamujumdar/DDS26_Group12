import uuid
import requests

BASE = "http://localhost:8000/payment"

def new_txn():
    return str(uuid.uuid4())

def create_user(credit=100):
    r = requests.post(f"{BASE}/create_user")
    user_id = r.json()["user_id"]
    requests.post(f"{BASE}/add_funds/{user_id}/{credit}")
    return user_id

def credit_of(user_id):
    return requests.get(f"{BASE}/find_user/{user_id}").json()["credit"]


# ── Happy path ──────────────────────────────────────────

def test_prepare_commit():
    user_id = create_user(100)
    txn_id  = new_txn()

    r = requests.post(f"{BASE}/prepare/{txn_id}",
                      json={"user_id": user_id, "amount": 40, "action": "pay"})
    assert r.status_code == 200
    assert r.json()["status"] == "prepared"
    assert credit_of(user_id) == 60          # tentatively deducted

    r = requests.post(f"{BASE}/commit/{txn_id}")
    assert r.status_code == 200
    assert credit_of(user_id) == 60          # stays deducted


def test_prepare_abort():
    user_id = create_user(100)
    txn_id  = new_txn()

    requests.post(f"{BASE}/prepare/{txn_id}",
                  json={"user_id": user_id, "amount": 40, "action": "pay"})
    assert credit_of(user_id) == 60

    r = requests.post(f"{BASE}/abort/{txn_id}")
    assert r.status_code == 200
    assert credit_of(user_id) == 100         # rolled back


def test_add_funds_via_2pc():
    user_id = create_user(50)
    txn_id  = new_txn()

    requests.post(f"{BASE}/prepare/{txn_id}",
                  json={"user_id": user_id, "amount": 25, "action": "add_funds"})
    assert credit_of(user_id) == 75

    requests.post(f"{BASE}/commit/{txn_id}")
    assert credit_of(user_id) == 75


# ── Insufficient funds ──────────────────────────────────

def test_insufficient_funds_votes_no():
    user_id = create_user(10)
    txn_id  = new_txn()

    r = requests.post(f"{BASE}/prepare/{txn_id}",
                      json={"user_id": user_id, "amount": 50, "action": "pay"})
    assert r.status_code == 409
    assert credit_of(user_id) == 10          # unchanged


# ── Idempotency ─────────────────────────────────────────

def test_prepare_is_idempotent():
    user_id = create_user(100)
    txn_id  = new_txn()

    r1 = requests.post(f"{BASE}/prepare/{txn_id}",
                       json={"user_id": user_id, "amount": 30, "action": "pay"})
    r2 = requests.post(f"{BASE}/prepare/{txn_id}",   # exact same call again
                       json={"user_id": user_id, "amount": 30, "action": "pay"})

    assert r1.status_code == r2.status_code == 200
    assert credit_of(user_id) == 70          # deducted only once


def test_commit_is_idempotent():
    user_id = create_user(100)
    txn_id  = new_txn()

    requests.post(f"{BASE}/prepare/{txn_id}",
                  json={"user_id": user_id, "amount": 30, "action": "pay"})

    r1 = requests.post(f"{BASE}/commit/{txn_id}")
    r2 = requests.post(f"{BASE}/commit/{txn_id}")
    assert r1.status_code == r2.status_code == 200
    assert credit_of(user_id) == 70


def test_abort_is_idempotent():
    user_id = create_user(100)
    txn_id  = new_txn()

    requests.post(f"{BASE}/prepare/{txn_id}",
                  json={"user_id": user_id, "amount": 30, "action": "pay"})

    r1 = requests.post(f"{BASE}/abort/{txn_id}")
    r2 = requests.post(f"{BASE}/abort/{txn_id}")
    assert r1.status_code == r2.status_code == 200
    assert credit_of(user_id) == 100


def test_abort_unknown_txn_is_ok():
    """Abort with no prepare record should succeed silently."""
    r = requests.post(f"{BASE}/abort/{new_txn()}")
    assert r.status_code == 200


def test_commit_unknown_txn_returns_404():
    r = requests.post(f"{BASE}/commit/{new_txn()}")
    assert r.status_code == 404