import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from activities import (
    authorize_payment, reserve_inventory, create_shipment,
    capture_payment, send_confirmation, void_payment, release_inventory,
)
from workflow import OrderWorkflow


async def main() -> None:
    logging.getLogger("temporalio.activity").setLevel(logging.ERROR)  # hide expected stack traces
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client,
        task_queue="orders",
        workflows=[OrderWorkflow],
        activities=[
            authorize_payment, reserve_inventory, create_shipment,
            capture_payment, send_confirmation, void_payment, release_inventory,
        ],
    )
    print("Worker started. Waiting for orders... (Ctrl+C to stop)")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
