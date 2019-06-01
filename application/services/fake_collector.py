import random
import string
import uuid
import datetime
import logging

from kombu.messaging import Exchange
from nameko.messaging import Publisher
from nameko.constants import PERSISTENT
from nameko.rpc import rpc
from nameko.events import event_handler
from nameko.dependency_providers import DependencyProvider
from bson.json_util import dumps, loads

_log = logging.getLogger(__name__)


class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class FakeCollectorService(object):
    name = 'fake_collector'
    error = ErrorHandler()
    pub_input = Publisher(exchange=Exchange(name='all_inputs', type='topic', durable=True, auto_delete=True,
                                            delivery_mode=PERSISTENT))
    pub_notif = Publisher(exchange=Exchange(name='all_notifications', type='topic', durable=True, auto_delete=True,
                                            delivery_mode=PERSISTENT))

    @rpc
    def publish(self):
        status = random.choice(['CREATED', 'UPDATED', 'DELETED', 'UNCHANGED'])
        checksum = str(uuid.uuid4())
        id_ = str(uuid.uuid4())
        referential = {
            'events': [
                {
                    'id': id_,
                    'date': datetime.datetime.utcnow(),
                    'common_name': f'fake_event_{id_}',
                    'provider': 'fake',
                    'type': 'fake',
                    'content': {},
                    'entities': []
                }
            ]
        }
        datastore = [
            {
                'write_policy': 'insert',
                'meta': [('ID', 'VARCHAR(36)'), ('VALUE', 'FLOAT')],
                'target_table': 'fake',
                'records': [
                    {
                        'id': id_,
                        'value': random.uniform(0, 1)
                    }
                ]
            }
        ]
        event = {
            'id': str(uuid.uuid4()),
            'status': status,
            'checksum': checksum,
            'referential': referential,
            'datastore': datastore,
            'meta': {'source': 'fake'}
        }
        _log.info(f'Publishing event {event}')
        self.pub_input(dumps(event))
        return event

    @event_handler('loader', 'input_loaded')
    def ack(self, payload):
        _log.info(payload)
        msg = loads(payload)
        if 'meta' not in msg:
            return
        meta = msg['meta']
        if 'source' not in meta or meta['source'] != 'fake':
            return
        _log.info('Publishing notification ...')
        self.pub_notif(dumps({
            'id': msg['id'],
            'source': self.name,
            'content': 'nice fake message'
        }))
