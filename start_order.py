import asyncio
import sys

from temporalio.client import Client, WorkflowFailureError

from activities import Order
from workflow import OrderWorkflow


async def main() -> None:
    order_id = sys.argv[1]
    scenario = sys.argv[2] if len(sys.argv) > 2 else "complete"

    client = await Client.connect("localhost:7233")
    print(f"Submitting order {order_id} (scenario: {scenario})...")

    try:
        result = await client.execute_workflow(
            OrderWorkflow.run,
            Order(order_id=order_id, item="Football T-Shirt", amount=95.00, scenario=scenario),
            id=f"order-{order_id}",
            task_queue="orders",
        )
        print(result)
    except WorkflowFailureError as err:
        print(f"FAILED: {err.cause}")


if __name__ == "__main__":
    asyncio.run(main())
