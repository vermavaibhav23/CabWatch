"""Incremental continuity rules, independent of Kafka fetch timing."""
import unittest
from datetime import datetime, timezone
from cab_watch.journey import evaluate
from cab_watch import city

class IncrementalRules(unittest.TestCase):
    def setUp(self):
        self.trip=dict(planned_route=['G','H','I','J','K'],last_sequence=0,last_captured_at=None,off_route_count=0)
    def apply(self,seq,node='D',second=None):
        lat,lon=city.gps(city.NODES[node])
        p=dict(sequence=seq,lat=lat,lon=lon,captured_at=f'2020-01-01T00:00:{second if second is not None else seq:02d}Z')
        r=evaluate(self.trip,p)
        self.trip.update(last_sequence=seq,off_route_count=r['count'])
        if r['valid']:self.trip['last_captured_at']=r['captured']
        return r
    def test_three_then_return(self):
        self.assertEqual([self.apply(i)['count'] for i in (1,2,3,4)],[1,2,3,3])
        self.assertEqual(self.apply(5,'H')['count'],0)
    def test_gap_restarts_confirmation(self):
        self.apply(1);self.apply(2)
        self.assertEqual(self.apply(4)['count'],1)
        self.assertEqual(self.apply(5)['count'],2)
    def test_time_gap_and_reversal(self):
        self.apply(1);self.apply(2)
        self.assertEqual(self.apply(3,second=20)['count'],1)
        self.assertFalse(self.apply(4,second=19)['valid'])
        self.assertEqual(self.apply(5,second=21)['count'],1)
    def test_destination_needs_only_coordinates(self):
        result=self.apply(1,'K')
        self.assertTrue(result['completed']);self.assertEqual(result['count'],0)
    def test_invalid_time_cannot_confirm(self):
        lat,lon=city.gps(city.NODES['D'])
        for timestamp in ('broken','2020-01-01T00:00:00','2999-01-01T00:00:00Z'):
            self.assertFalse(evaluate(self.trip,dict(sequence=1,lat=lat,lon=lon,captured_at=timestamp))['valid'])
