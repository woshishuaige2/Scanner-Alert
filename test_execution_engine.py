import unittest

from execution_engine import ExecutionEngine


class FakeTwsApp:
    def __init__(self):
        self.next_order_id = 1
        self.order_status_callbacks = []
        self.error_callbacks = []
        self.placed_orders = []
        self.cancelled_orders = []

    def placeOrder(self, order_id, contract, order):
        self.placed_orders.append((order_id, contract, order))

    def cancelOrder(self, order_id):
        self.cancelled_orders.append(order_id)


class ExecutionEngineExitLifecycleTests(unittest.TestCase):
    def test_extended_hours_exit_cancel_does_not_close_position_and_allows_retry(self):
        tws = FakeTwsApp()
        engine = ExecutionEngine(
            tws_app=tws,
            account="DU123",
            tp_pct=5.0,
            sl_pct=5.0,
            investment_per_trade=500.0,
        )

        self.assertTrue(
            engine.execute_trade(
                "PBM",
                entry_price=15.81,
                stop_price=15.68,
                market_session="PREMARKET",
                bid=15.77,
                ask=15.84,
            )
        )

        engine._on_order_status(
            orderId=1,
            status="Filled",
            filled=31,
            remaining=0,
            avgFillPrice=15.83,
            parentId=0,
        )
        engine.on_market_update(
            "PBM",
            price=15.62,
            bid=15.73,
            ask=15.79,
            market_session="PREMARKET",
        )

        snapshot = engine.get_position_snapshot("PBM")
        self.assertTrue(snapshot["exit_pending"])
        first_exit_order_id = snapshot["active_exit_order_id"]
        self.assertEqual(first_exit_order_id, 3)

        engine._on_order_status(
            orderId=first_exit_order_id,
            status="Cancelled",
            filled=0,
            remaining=31,
            avgFillPrice=0,
            parentId=0,
        )

        snapshot = engine.get_position_snapshot("PBM")
        self.assertIsNotNone(snapshot)
        self.assertFalse(snapshot["exit_pending"])
        self.assertIsNone(snapshot["active_exit_order_id"])
        self.assertEqual(snapshot["last_exit_cancelled_status"], "Cancelled")
        self.assertEqual(engine.get_trade_history(), [])

        engine.on_market_update(
            "PBM",
            price=15.60,
            bid=15.60,
            ask=15.65,
            market_session="PREMARKET",
        )

        snapshot = engine.get_position_snapshot("PBM")
        self.assertTrue(snapshot["exit_pending"])
        self.assertNotEqual(snapshot["active_exit_order_id"], first_exit_order_id)


if __name__ == "__main__":
    unittest.main()
