import asyncio
from dataclasses import dataclass

from temporalio import activity
from temporalio.exceptions import ApplicationError


@dataclass
class Order:
    order_id: str
    item: str
    amount: float
    scenario: str = "complete"  # "complete", "ship_timeout" or "no_stock"


# Pretend this dictionary is the shipping provider's own database.
SHIPPING_PROVIDER_LABELS: dict[str, str] = {}


@activity.defn
async def authorize_payment(order: Order) -> str:
    # Idempotency key = order_id, so a retry can never charge twice.
    print(f"[payment]   Authorised £{order.amount:.2f} (idempotency key: {order.order_id})")
    return f"AUTH-{order.order_id}"


@activity.defn
async def reserve_inventory(order: Order) -> str:
    if order.scenario == "no_stock":
        # A business failure. Temporal will not bother retrying as the item is out of stock
        raise ApplicationError(f"{order.item} is out of stock", type="OutOfStock", non_retryable=True)
    print(f"[inventory] Reserved 1 x {order.item}")
    return f"RES-{order.order_id}"


@activity.defn
async def create_shipment(order: Order) -> str:
    attempt = activity.info().attempt

    # STEP 1: Before creating anything, ask the provider "do you already have a label for this order?"
    existing = SHIPPING_PROVIDER_LABELS.get(order.order_id)
    if existing:
        print(f"[shipping]  Attempt {attempt}: label {existing} ALREADY EXISTS -> reusing it, no duplicate")
        return existing

    # STEP 2: Create the label (the provider saves it on their side).
    label = f"LABEL-{order.order_id}"
    SHIPPING_PROVIDER_LABELS[order.order_id] = label
    print(f"[shipping]  Attempt {attempt}: created label {label}")

    # Simulate the follow-up question: the provider created the label,
    # but its reply never reaches us. Temporal's timeout fires and it retries.
    if order.scenario == "ship_timeout" and attempt == 1:
        print("[shipping]  ...provider is not responding (we don't know if it worked)")
        await asyncio.sleep(8)

    return label


@activity.defn
async def capture_payment(order: Order) -> str:
    print(f"[payment]   Captured £{order.amount:.2f} (idempotency key: {order.order_id})")
    return f"CAPTURE-{order.order_id}"


@activity.defn
async def send_confirmation(order: Order) -> str:
    print(f"[notify]    Confirmation email sent for order {order.order_id}")
    return "SENT"


# ---- How we "undo" a step if a later step fails ----

@activity.defn
async def void_payment(order: Order) -> str:
    print(f"[payment]   VOIDED authorisation for order {order.order_id} (customer not charged)")
    return "VOIDED"


@activity.defn
async def release_inventory(order: Order) -> str:
    print(f"[inventory] RELEASED reservation for order {order.order_id}")
    return "RELEASED"
