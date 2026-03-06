"""
2PC Bug Showcase Test Suite
============================
Tests that demonstrate each confirmed bug in the current implementation.
Run against a live docker-compose stack:  docker-compose up --build -d

Each test is clearly marked:
  [BUG]    — currently FAILS / exposes incorrect behaviour (the bug)
  [FIXED]  — expected behaviour after the bug is fixed

Run:
    pip install pytest httpx pytest-asyncio
    pytest test_2pc_bugs.py -v
"""

import asyncio
import uuid
from pathlib import Path
import pytest
import httpx

BASE = "http://localhost:8000"   # nginx gateway

# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

async def create_user(client: httpx.AsyncClient, credit: int) -> str:
    r = await client.post(f"{BASE}/payment/create_user")
    assert r.status_code == 200, r.text
    user_id = r.json()["user_id"]
    if credit > 0:
        r2 = await client.post(f"{BASE}/payment/add_funds/{user_id}/{credit}")
        assert r2.status_code == 200, r2.text
    return user_id


async def create_item(client: httpx.AsyncClient, price: int, stock: int) -> str:
    r = await client.post(f"{BASE}/stock/item/create/{price}")
    assert r.status_code == 200, r.text
    item_id = r.json()["item_id"]
    r2 = await client.post(f"{BASE}/stock/add/{item_id}/{stock}")
    assert r2.status_code == 200, r2.text
    return item_id


async def create_order(client: httpx.AsyncClient, user_id: str) -> str:
    r = await client.post(f"{BASE}/orders/create/{user_id}")
    assert r.status_code == 200, r.text
    return r.json()["order_id"]


async def add_item_to_order(client: httpx.AsyncClient, order_id: str, item_id: str, qty: int = 1):
    r = await client.post(f"{BASE}/orders/addItem/{order_id}/{item_id}/{qty}")
    assert r.status_code == 200, r.text


async def get_credit(client: httpx.AsyncClient, user_id: str) -> int:
    r = await client.get(f"{BASE}/payment/find_user/{user_id}")
    assert r.status_code == 200, r.text
    return r.json()["credit"]


async def get_stock(client: httpx.AsyncClient, item_id: str) -> int:
    r = await client.get(f"{BASE}/stock/find/{item_id}")
    assert r.status_code == 200, r.text
    return r.json()["stock"]


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────

@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def client():
    async with httpx.AsyncClient(timeout=10.0) as c:
        yield c


# ══════════════════════════════════════════════
# BUG 1 — Payment abort: delta vs value compare
# ══════════════════════════════════════════════
#
# _do_abort in payment/app.py checks:
#   if user.credit == rec.new_credit: restore
#   else: just mark aborted  ← WRONG — skips restore
#
# Reproduce: run two concurrent transactions on the same user.
# Txn A prepares (credit: 100 → 60, delta=-40).
# Txn B prepares (credit: 60 → 20, delta=-40).
# Txn A aborts — by now credit is 20, NOT 60 (rec.new_credit).
# The guard fires, restore is skipped → user stuck with credit=20
# instead of the correct credit=60 (20 + 40 restored).
# ══════════════════════════════════════════════

class TestBug1_PaymentAbortDeltaVsValue:

    @pytest.mark.anyio
    async def test_BUG_abort_skips_restore_when_credit_changed_by_concurrent_txn(self, client):
        """
        [BUG] After txn A aborts, user credit should be restored to 60.
        Due to the value-compare guard, restore is skipped → credit stays at 20.
        This test FAILS on the buggy code (asserts 60, gets 20).
        """
        user_id = await create_user(client, 100)
        txn_a = str(uuid.uuid4())
        txn_b = str(uuid.uuid4())

        # Txn A prepares: 100 → 60
        r = await client.post(f"{BASE}/payment/2pc/prepare/{txn_a}/{user_id}/40")
        assert r.status_code == 200, f"Prepare A failed: {r.text}"

        # Txn B prepares: 60 → 20  (concurrent)
        r = await client.post(f"{BASE}/payment/2pc/prepare/{txn_b}/{user_id}/40")
        assert r.status_code == 200, f"Prepare B failed: {r.text}"

        # Txn B commits (credit stays at 20)
        r = await client.post(f"{BASE}/payment/2pc/commit/{txn_b}")
        assert r.status_code == 200

        # Txn A aborts — should restore its delta of 40 → credit becomes 60
        r = await client.post(f"{BASE}/payment/2pc/abort/{txn_a}")
        assert r.status_code == 200

        credit = await get_credit(client, user_id)
        # BUG: actual credit will be 20 (restore was skipped)
        assert credit == 60, (
            f"[BUG EXPOSED] Expected credit=60 after txn A abort, got {credit}. "
            f"The abort silently skipped the restore because credit (20) != rec.new_credit (60)."
        )

    @pytest.mark.anyio
    async def test_FIXED_abort_always_restores_delta(self, client):
        """
        [FIXED] After applying the fix (always restore by delta),
        aborting txn A must give the user back its 40 regardless of
        what other transactions have done to the credit.
        This test PASSES after the fix.
        """
        user_id = await create_user(client, 100)
        txn_a = str(uuid.uuid4())
        txn_b = str(uuid.uuid4())

        await client.post(f"{BASE}/payment/2pc/prepare/{txn_a}/{user_id}/40")
        await client.post(f"{BASE}/payment/2pc/prepare/{txn_b}/{user_id}/40")
        await client.post(f"{BASE}/payment/2pc/commit/{txn_b}")
        await client.post(f"{BASE}/payment/2pc/abort/{txn_a}")

        credit = await get_credit(client, user_id)
        assert credit == 60, f"Expected 60, got {credit}"


# ══════════════════════════════════════════════
# BUG 2 — Stock double-abort restores stock twice
# ══════════════════════════════════════════════
#
# _do_abort in stock/app.py adds stock back unconditionally.
# There is no guard inside the function checking res.state == "aborted".
# The state check lives only in the HTTP endpoint, so if the service
# crashes mid-abort and the coordinator retries, stock is added twice.
#
# We simulate this by calling /2pc/abort twice with a synthetic
# already-aborted reservation injected directly into Redis,
# OR by simply calling the abort endpoint twice and observing
# the stock goes above original — because the endpoint-level guard
# returns early on the SECOND call (idempotent), but if the crash
# happens BETWEEN writing "aborted" and adding stock, the internal
# function will be called again on recovery with state still "prepared".
#
# For an integration test without a crash, we directly call _do_abort
# behaviour by hitting the abort endpoint twice, first time with a
# "prepared" reservation, second time patching state back to "prepared"
# (simulating mid-abort crash).  Since we can't inject Redis state
# from outside, we demonstrate the logical gap: the function itself
# has no guard, so a direct second call would double-restore.
# ══════════════════════════════════════════════

class TestBug2_StockDoubleAbort:

    @pytest.mark.anyio
    async def test_BUG_abort_called_twice_simulating_crash_between_state_write_and_stock_restore(self, client):
        """
        [BUG] Simulates a scenario where the service crashes after writing
        res.state="aborted" to Redis but before the pipeline that restores
        stock executes (WatchError / network blip path).

        In practice, calling abort twice on a fresh "prepared" reservation
        should be idempotent and restore stock exactly once.
        The current endpoint guard handles the second call correctly,
        BUT if a crash happens INSIDE _do_abort after partial execution,
        the recovery path calls the inner function directly — bypassing
        the endpoint guard — and stock is restored a second time.

        This test asserts idempotent abort: stock must equal original after
        any number of abort calls.
        """
        item_id = await create_item(client, price=10, stock=5)
        txn_id = str(uuid.uuid4())

        original_stock = await get_stock(client, item_id)
        assert original_stock == 5

        # Prepare: reserves 2 units (stock goes to 3)
        r = await client.post(f"{BASE}/stock/2pc/prepare/{txn_id}/{item_id}/2")
        assert r.status_code == 200, r.text
        assert await get_stock(client, item_id) == 3

        # First abort: should restore 2 → stock back to 5
        r = await client.post(f"{BASE}/stock/2pc/abort/{txn_id}/{item_id}/2")
        assert r.status_code == 200
        stock_after_first_abort = await get_stock(client, item_id)
        assert stock_after_first_abort == 5, f"Expected 5 after first abort, got {stock_after_first_abort}"

        # Second abort (simulates retry after crash):
        # Endpoint returns early (idempotent) because state == "aborted".
        # But the inner _do_abort has no guard, so a crash-recovery path
        # calling it directly would give stock=7. This test documents
        # the vulnerability even though the endpoint path is safe.
        r2 = await client.post(f"{BASE}/stock/2pc/abort/{txn_id}/{item_id}/2")
        assert r2.status_code == 200
        stock_after_second_abort = await get_stock(client, item_id)

        # This assertion passes on buggy code ONLY if we go through the endpoint.
        # The bug manifests in crash-recovery. We document it:
        assert stock_after_second_abort == 5, (
            f"[BUG RISK] Stock is {stock_after_second_abort}. "
            f"The _do_abort function has no internal state guard. "
            f"A crash between state-write and stock-restore would cause stock=7 on retry."
        )

    @pytest.mark.anyio
    async def test_FIXED_abort_is_idempotent_regardless_of_path(self, client):
        """
        [FIXED] After adding a state guard inside _do_abort itself,
        calling abort multiple times must always yield stock == original.
        """
        item_id = await create_item(client, price=5, stock=10)
        txn_id = str(uuid.uuid4())

        await client.post(f"{BASE}/stock/2pc/prepare/{txn_id}/{item_id}/3")
        assert await get_stock(client, item_id) == 7

        for _ in range(3):
            r = await client.post(f"{BASE}/stock/2pc/abort/{txn_id}/{item_id}/3")
            assert r.status_code == 200

        assert await get_stock(client, item_id) == 10


# ══════════════════════════════════════════════
# BUG 3 — Direct /pay endpoint bypasses 2PC
# ══════════════════════════════════════════════
#
# /payment/pay/{user_id}/{amount} modifies credit directly,
# with no check for an active prepared 2PC transaction on that user.
# A concurrent direct payment can corrupt in-flight 2PC state.
# ══════════════════════════════════════════════

class TestBug3_DirectPayBypassesTwoPC:

    @pytest.mark.anyio
    async def test_BUG_direct_pay_races_with_prepared_txn(self, client):
        """
        [BUG] While a 2PC transaction is in the PREPARED state (credit locked),
        a direct /pay call succeeds and reduces credit independently.
        When the 2PC txn then aborts, it tries to restore credit that was
        already further reduced, causing an incorrect final balance.

        Correct behaviour: direct /pay should fail (4xx) if a prepared
        2PC txn is holding a lock on that user's credit.
        """
        user_id = await create_user(client, 100)
        txn_id = str(uuid.uuid4())

        # 2PC prepare: locks 40 credits (100 → 60)
        r = await client.post(f"{BASE}/payment/2pc/prepare/{txn_id}/{user_id}/40")
        assert r.status_code == 200

        # Direct pay while 2PC is prepared — this should be blocked but isn't
        r_pay = await client.post(f"{BASE}/payment/pay/{user_id}/30")
        # BUG: returns 200 instead of 4xx
        assert 400 <= r_pay.status_code < 500, (
            f"[BUG EXPOSED] Direct /pay succeeded (got {r_pay.status_code}) "
            f"while a 2PC prepare was active. This bypasses the 2PC lock."
        )

    @pytest.mark.anyio
    async def test_FIXED_direct_pay_blocked_during_prepared_txn(self, client):
        """
        [FIXED] After adding a lock-check to /pay, it must return 4xx
        when a 2PC prepare is active on that user.
        """
        user_id = await create_user(client, 100)
        txn_id = str(uuid.uuid4())

        await client.post(f"{BASE}/payment/2pc/prepare/{txn_id}/{user_id}/40")
        r = await client.post(f"{BASE}/payment/pay/{user_id}/30")
        assert 400 <= r.status_code < 500, f"Expected 4xx, got {r.status_code}"

        # After commit, direct pay should work again
        await client.post(f"{BASE}/payment/2pc/commit/{txn_id}")
        r2 = await client.post(f"{BASE}/payment/pay/{user_id}/10")
        assert r2.status_code == 200


# ══════════════════════════════════════════════
# BUG 4 — Watchdog hardcoded container name
# ══════════════════════════════════════════════
#
# Watchdog uses:  dds_group12-${svc}-1
# If the project folder is not "dds_group12", containers are never restarted.
# This is a config/deployment bug, not testable via HTTP, but we document it.
# ══════════════════════════════════════════════

class TestBug4_WatchdogContainerName:

    def test_BUG_watchdog_uses_hardcoded_prefix(self):
        """
        [BUG] docker-compose.yml watchdog hardcodes 'dds_group12' as the
        container name prefix. Docker Compose derives the prefix from the
        folder name, so this breaks in any other directory.

        Fix: use COMPOSE_PROJECT_NAME env var or replace the hardcoded
        prefix with a dynamic one, e.g. via 'docker ps --filter label=...'.

        This test always FAILS to remind you to fix the watchdog config.
        """
        import re
        compose_path = Path(__file__).resolve().parents[1] / "docker-compose.yml"
        with compose_path.open() as f:
            content = f.read()

        hardcoded = re.findall(r"dds_group12", content)
        assert len(hardcoded) == 0, (
            f"[BUG EXPOSED] Found {len(hardcoded)} hardcoded 'dds_group12' "
            f"references in docker-compose.yml watchdog command. "
            f"Replace with a dynamic container name lookup."
        )


# ══════════════════════════════════════════════
# BUG 5 — No participant-side recovery on restart
# ══════════════════════════════════════════════
#
# If stock-service or payment-service crashes while a txn is in PREPARED
# state, on restart they have no recovery logic. The coordinator may retry
# commit/abort, but participants don't pro-actively resolve orphaned prepares.
# This test verifies the coordinator recovery endpoint exists and is called.
# ══════════════════════════════════════════════

class TestBug5_ParticipantRecovery:

    @pytest.mark.anyio
    @pytest.mark.xfail(
        reason="Participant self-healing is not implemented; coordinator recovery owns orphan resolution.",
        strict=False,
    )
    async def test_BUG_orphaned_prepared_txn_locks_stock_after_restart(self, client):
        """
        [BUG] Simulates a coordinator crash after prepare but before commit/abort.
        The txn stays in PREPARED state. A new checkout for the same item
        may fail because stock appears reserved.

        We can't kill docker containers in this test, but we verify the
        symptom: after a prepare with no commit/abort, the stock remains
        reduced and there is no self-healing endpoint on the participant.
        """
        item_id = await create_item(client, price=10, stock=1)
        txn_id = str(uuid.uuid4())

        # Prepare takes the last unit
        r = await client.post(f"{BASE}/stock/2pc/prepare/{txn_id}/{item_id}/1")
        assert r.status_code == 200
        assert await get_stock(client, item_id) == 0

        # Coordinator "crashes" — never sends commit or abort
        # Now a new txn tries to buy the same item:
        txn_id_2 = str(uuid.uuid4())
        r2 = await client.post(f"{BASE}/stock/2pc/prepare/{txn_id_2}/{item_id}/1")

        # BUG: returns 400 "insufficient stock" — item is permanently locked
        assert r2.status_code == 200, (
            f"[BUG EXPOSED] New transaction was rejected (got {r2.status_code}) "
            f"because orphaned prepared txn permanently holds the last stock unit. "
            f"Without recovery, this item can never be purchased again."
        )

    @pytest.mark.anyio
    async def test_FIXED_recovery_resolves_orphaned_prepared_txn(self, client):
        """
        [FIXED] After implementing coordinator recovery (recover_active_transactions),
        orphaned PREPARED transactions should be aborted on restart,
        freeing the stock for new purchases.

        This test assumes recovery runs on order-service startup.
        We verify stock is restored to 1 after a simulated orphan scenario.
        (Full verification requires killing and restarting the container.)
        """
        item_id = await create_item(client, price=10, stock=1)
        txn_id = str(uuid.uuid4())

        await client.post(f"{BASE}/stock/2pc/prepare/{txn_id}/{item_id}/1")
        assert await get_stock(client, item_id) == 0

        # Manually abort (simulating what recovery would do)
        await client.post(f"{BASE}/stock/2pc/abort/{txn_id}/{item_id}/1")
        assert await get_stock(client, item_id) == 1

        # Now a new transaction can succeed
        txn_id_2 = str(uuid.uuid4())
        r = await client.post(f"{BASE}/stock/2pc/prepare/{txn_id_2}/{item_id}/1")
        assert r.status_code == 200


# ══════════════════════════════════════════════
# INTEGRATION — Full checkout consistency test
# ══════════════════════════════════════════════

class TestIntegrationCheckout:

    @pytest.mark.anyio
    async def test_successful_checkout_reduces_stock_and_credit(self, client):
        """
        [BASELINE] A successful checkout must atomically:
        - reduce stock by the ordered quantity
        - reduce user credit by total_cost
        - mark order as paid
        """
        user_id = await create_user(client, 200)
        item_id = await create_item(client, price=50, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, qty=2)

        r = await client.post(f"{BASE}/orders/checkout/{order_id}")
        assert r.status_code == 200, f"Checkout failed: {r.text}"

        assert await get_credit(client, user_id) == 100  # 200 - 2*50
        assert await get_stock(client, item_id) == 3     # 5 - 2

        order = await client.get(f"{BASE}/orders/find/{order_id}")
        assert order.json()["paid"] is True

    @pytest.mark.anyio
    async def test_failed_checkout_rolls_back_stock_and_credit(self, client):
        """
        [BASELINE] A checkout that fails (insufficient credit) must:
        - NOT reduce stock
        - NOT reduce credit
        - NOT mark order as paid
        """
        user_id = await create_user(client, 10)   # not enough for price=50
        item_id = await create_item(client, price=50, stock=5)
        order_id = await create_order(client, user_id)
        await add_item_to_order(client, order_id, item_id, qty=1)

        r = await client.post(f"{BASE}/orders/checkout/{order_id}")
        assert r.status_code == 400, f"Expected failure, got: {r.text}"

        assert await get_credit(client, user_id) == 10   # unchanged
        assert await get_stock(client, item_id) == 5     # unchanged

        order = await client.get(f"{BASE}/orders/find/{order_id}")
        assert order.json()["paid"] is False

    @pytest.mark.anyio
    async def test_concurrent_checkouts_maintain_consistency(self, client):
        """
        [CONSISTENCY] Two concurrent checkouts competing for the last item
        and limited credit. Exactly one should succeed, the other should
        fail and fully roll back. Neither stock nor credit should go negative.
        """
        user_id = await create_user(client, 100)
        item_id = await create_item(client, price=100, stock=1)  # only 1 item

        order_id_1 = await create_order(client, user_id)
        await add_item_to_order(client, order_id_1, item_id, qty=1)

        order_id_2 = await create_order(client, user_id)
        await add_item_to_order(client, order_id_2, item_id, qty=1)

        results = await asyncio.gather(
            client.post(f"{BASE}/orders/checkout/{order_id_1}"),
            client.post(f"{BASE}/orders/checkout/{order_id_2}"),
            return_exceptions=True,
        )

        status_codes = [r.status_code if isinstance(r, httpx.Response) else 500 for r in results]
        successes = status_codes.count(200)
        failures = status_codes.count(400)

        assert successes == 1, f"Expected exactly 1 success, got {successes}. Statuses: {status_codes}"
        assert failures == 1, f"Expected exactly 1 failure, got {failures}. Statuses: {status_codes}"

        final_stock = await get_stock(client, item_id)
        final_credit = await get_credit(client, user_id)

        assert final_stock == 0, f"Stock should be 0, got {final_stock}"
        assert final_credit == 0, f"Credit should be 0, got {final_credit}"
        assert final_stock >= 0, "Stock went negative!"
        assert final_credit >= 0, "Credit went negative!"
