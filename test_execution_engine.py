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
    def test_regular_hours_entry_uses_initial_disaster_stop(self):
        tws = FakeTwsApp()
        engine = ExecutionEngine(
            tws_app=tws,
            account="DU123",
            tp_pct=5.0,
            sl_pct=5.0,
            investment_per_trade=1000.0,
        )

        self.assertTrue(
            engine.execute_trade(
                "UCAR",
                entry_price=10.00,
                stop_price=9.80,
                market_session="REGULAR",
            )
        )

        initial_stop_order = tws.placed_orders[1][2]
        self.assertEqual(initial_stop_order.orderType, "STP")
        self.assertEqual(initial_stop_order.auxPrice, 9.50)

        engine._on_order_status(
            orderId=1,
            status="Filled",
            filled=100,
            remaining=0,
            avgFillPrice=10.00,
            parentId=0,
        )

        snapshot = engine.get_position_snapshot("UCAR")
        self.assertEqual(snapshot["current_stop_price"], 9.50)
        self.assertEqual(snapshot["strategy_stop_price"], 9.80)
        self.assertTrue(snapshot["initial_disaster_stop_active"])

    def test_initial_disaster_stop_tightens_after_bounce(self):
        tws = FakeTwsApp()
        engine = ExecutionEngine(
            tws_app=tws,
            account="DU123",
            tp_pct=5.0,
            sl_pct=5.0,
            investment_per_trade=1000.0,
        )

        self.assertTrue(
            engine.execute_trade(
                "UCAR",
                entry_price=10.00,
                stop_price=9.80,
                market_session="REGULAR",
            )
        )
        engine._on_order_status(
            orderId=1,
            status="Filled",
            filled=100,
            remaining=0,
            avgFillPrice=10.00,
            parentId=0,
        )

        engine.on_market_update(
            "UCAR",
            price=10.02,
            bid=10.01,
            ask=10.03,
            market_session="REGULAR",
        )

        snapshot = engine.get_position_snapshot("UCAR")
        self.assertEqual(snapshot["current_stop_price"], 9.80)
        self.assertFalse(snapshot["initial_disaster_stop_active"])
        tightened_stop_order = tws.placed_orders[-1][2]
        self.assertEqual(tightened_stop_order.orderType, "STP")
        self.assertEqual(tightened_stop_order.auxPrice, 9.80)

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

    def test_extended_hours_stop_exit_uses_aggressive_marketable_limit(self):
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
                "LABT",
                entry_price=4.49,
                stop_price=4.42,
                market_session="PREMARKET",
                bid=4.48,
                ask=4.50,
            )
        )
        engine._on_order_status(
            orderId=1,
            status="Filled",
            filled=112,
            remaining=0,
            avgFillPrice=4.49,
            parentId=0,
        )

        engine.on_market_update(
            "LABT",
            price=4.30,
            bid=4.30,
            ask=4.36,
            market_session="PREMARKET",
        )

        exit_order = tws.placed_orders[-1][2]
        self.assertEqual(exit_order.orderType, "LMT")
        self.assertEqual(exit_order.action, "SELL")
        self.assertEqual(exit_order.totalQuantity, 112)
        self.assertLess(exit_order.lmtPrice, 4.30)

    def test_partial_extended_hours_stop_exit_remains_tracked_and_reprices_lower(self):
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
                "LABT",
                entry_price=4.49,
                stop_price=4.42,
                market_session="PREMARKET",
                bid=4.48,
                ask=4.50,
            )
        )
        engine._on_order_status(
            orderId=1,
            status="Filled",
            filled=112,
            remaining=0,
            avgFillPrice=4.49,
            parentId=0,
        )
        engine.on_market_update(
            "LABT",
            price=4.30,
            bid=4.30,
            ask=4.36,
            market_session="PREMARKET",
        )

        exit_order_id = tws.placed_orders[-1][0]
        engine._on_order_status(
            orderId=exit_order_id,
            status="Submitted",
            filled=100,
            remaining=12,
            avgFillPrice=4.28,
            parentId=0,
        )

        snapshot = engine.get_position_snapshot("LABT")
        self.assertEqual(snapshot["remaining_shares"], 12)
        self.assertTrue(snapshot["exit_pending"])
        self.assertEqual(snapshot["active_exit_order_id"], exit_order_id)

        snapshot["last_stop_guard_reprice_at"] = None
        engine.positions["LABT"]["last_stop_guard_reprice_at"] = None
        engine.positions["LABT"]["active_exit_submitted_at"] = None
        engine.on_market_update(
            "LABT",
            price=4.20,
            bid=4.20,
            ask=4.25,
            market_session="PREMARKET",
        )

        repriced_order_id, _, repriced_order = tws.placed_orders[-1]
        self.assertEqual(repriced_order_id, exit_order_id)
        self.assertEqual(repriced_order.totalQuantity, 12)
        self.assertLess(repriced_order.lmtPrice, 4.20)


if __name__ == "__main__":
    unittest.main()
