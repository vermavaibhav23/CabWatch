import asyncio
import json
import unittest
from types import SimpleNamespace
from uuid import uuid4
from unittest.mock import AsyncMock
from cab_watch import city
from cab_watch.journey import evaluate
from cab_watch.stream import Stream

class OffsetRecovery(unittest.IsolatedAsyncioTestCase):
    async def test_failed_database_write_never_commits_then_retries(self):
        stream=object.__new__(Stream)
        calls=[]
        async def save(records):
            calls.append('save')
            if calls==['save']:raise RuntimeError('temporary database outage')
        stream.history=save
        class Consumer:
            async def getmany(self,**kwargs):
                if 'commit' in calls:raise asyncio.CancelledError
                return {'partition':[SimpleNamespace(offset=7)]}
            def assignment(self):return {'partition'}
            async def commit(self,offsets):
                self.offsets=offsets;calls.append('commit')
            def highwater(self,p):return None
        consumer=Consumer()
        with self.assertRaises(asyncio.CancelledError):await stream.consume(consumer)
        self.assertEqual(calls,['save','save','commit'])
        self.assertEqual(consumer.offsets,{'partition':8})

class KafkaAuthentication(unittest.IsolatedAsyncioTestCase):
    async def test_owner_does_not_depend_on_old_stream_key_cache(self):
        from cab_watch.auth import owner,issue
        from fastapi.security import HTTPAuthorizationCredentials
        account=dict(id=uuid4(),name='Test',jwt_key=b'x'*32)
        token=issue(account)['access_token']
        db=SimpleNamespace(fetchval=AsyncMock(return_value=account['jwt_key']))
        request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(db=db,pipeline=object())))
        credentials=HTTPAuthorizationCredentials(scheme='Bearer',credentials=token)
        self.assertEqual(await owner(request,credentials),account['id'])
