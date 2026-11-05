from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from activities import (
        Order, authorize_payment, reserve_inventory, create_shipment,
        capture_payment, send_confirmation, void_payment, release_inventory,
    )

# How every step is retried and timed out.
STEP_OPTIONS = dict(
    start_to_close_timeout=timedelta(seconds=5),       # give up on one attempt after 5s
    retry_policy=RetryPolicy(
        initial_interval=timedelta(seconds=1),          # wait 1s, then 2s, then 4s...
        backoff_coefficient=2.0,
        maximum_attempts=5,
    ),
)


@workflow.defn
class OrderWorkflow:
    def __init__(self) -> None:
        self.status = "SUBMITTED"

    @workflow.query
    def get_status(self) -> str:
        return self.status

    @workflow.run
    async def run(self, order: Order) -> str:
        undo_steps = []  # things to undo if we fail part-way through

        try:
            self.status = "AUTHORISING_PAYMENT"
            await workflow.execute_activity(authorize_payment, order, **STEP_OPTIONS)
            undo_steps.append(void_payment)

            self.status = "RESERVING_INVENTORY"
            await workflow.execute_activity(reserve_inventory, order, **STEP_OPTIONS)
            undo_steps.append(release_inventory)

            self.status = "CREATING_SHIPMENT"
            await workflow.execute_activity(create_shipment, order, **STEP_OPTIONS)

            self.status = "CAPTURING_PAYMENT"
            await workflow.execute_activity(capture_payment, order, **STEP_OPTIONS)

            self.status = "SENDING_CONFIRMATION"
            await workflow.execute_activity(send_confirmation, order, **STEP_OPTIONS)

        except Exception as err:
            # Something failed.
            self.status = "ROLLING_BACK"
            for undo in reversed(undo_steps):
                await workflow.execute_activity(undo, order, **STEP_OPTIONS)
            self.status = "FAILED_ROLLED_BACK"
            raise ApplicationError(f"Order {order.order_id} rolled back: {getattr(err, 'cause', None) or err}")

        self.status = "COMPLETED"
        return f"Order {order.order_id} completed"
