"""Recovery invariants, independent of brokers and wall-clock timing."""
import unittest
from uuid import uuid4
from cab_watch import city
from cab_watch.live import advance, stamp

class LiveRecovery(unittest.TestCase):
    def setUp(self):
        self.trip = str(uuid4())
        self.user = str(uuid4())
        self.now = 1800000000000
        self.state = None

    def sample(self, seq, node="D", age=0, capture=None):
        lat, lon = city.gps(city.NODES[node])
        self.now += 1000
        event = dict(trip_id=self.trip, user_id=self.user, vehicle_id="CB#001", route=["G","H","I","J","K"],
                     point=dict(sequence=seq, captured_at=stamp(capture or self.now-age), lat=lat, lon=lon, motion=None))
        self.state, changes = advance(self.state, event, self.now)
        return changes

    def test_three_fresh_readings_then_resolve(self):
        self.sample(1); self.assertFalse(self.state["off_route"])
        self.sample(2); self.assertEqual(self.state["tracking"], "CONFIRMING")
        changes=self.sample(3)
        self.assertTrue(self.state["off_route"])
        self.assertEqual(changes[0]["confirming_capture"], self.state["captured_at"])
        self.sample(4,"H")
        self.assertFalse(self.state["off_route"])
        self.assertEqual(self.state["recent"][0]["close_reason"], "returned to route")

    def test_stale_reading_resets_and_preserves_last_fresh_position(self):
        self.sample(1); self.sample(2); self.sample(3)
        before = self.state["captured_at"]
        self.sample(4,"Z",age=300000)
        self.assertEqual(self.state["tracking"], "DELAYED")
        self.assertEqual(self.state["captured_at"], before)
        self.assertEqual(self.state["consecutive"], 0)
        self.sample(5); self.sample(6)
        self.assertFalse(self.state["off_route"])
        self.sample(7)
        self.assertTrue(self.state["off_route"])
        self.assertEqual(self.state["alert"]["opened_sequence"], 7)

    def test_missing_sequence_and_time_gap_reset_confirmation(self):
        self.sample(1); self.sample(2); self.sample(4)
        self.assertEqual(self.state["consecutive"], 1)
        self.sample(5)
        self.now += 20000
        self.sample(6)
        self.assertEqual(self.state["consecutive"], 1)
        self.assertFalse(self.state["off_route"])

    def test_duplicate_and_old_sequence_are_noops(self):
        self.sample(10); old=self.state
        self.assertEqual(self.sample(10,"H"), [])
        self.assertIs(self.state,old)
        self.sample(9); self.assertIs(self.state,old)

    def test_reversed_timestamp_resets_and_future_is_not_live(self):
        self.sample(1); self.sample(2)
        self.sample(3,capture=self.now-500)
        self.assertEqual(self.state["tracking"], "DELAYED")
        self.sample(4,age=-4000)
        self.assertEqual(self.state["consecutive"], 0)

    def test_completion_is_terminal_and_stale_arrival_is_not_completion(self):
        self.sample(1,"K",age=300000)
        self.assertFalse(self.state["completed"])
        changes=self.sample(2,"K")
        self.assertTrue(self.state["completed"])
        self.assertEqual(changes[-1]["kind"], "complete")
        old=self.state; self.sample(3)
        self.assertIs(self.state,old)

if __name__ == "__main__": unittest.main()
