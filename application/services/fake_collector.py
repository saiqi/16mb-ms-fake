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
from bson.json_util import dumps

_log = logging.getLogger(__name__)


class FakeCollectorService(object):
    name = 'fake_collector'
    publish = Publisher(exchange=Exchange(name='all_inputs', type='topic', durable=True, auto_delete=True,
                                          delivery_mode=PERSISTENT))

    @rpc
    def publish_input(self):
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
            'datastore': datastore
        }
        _log.info(f'Publishing event {event}')
        self.publish(dumps(event))
        return event

    @event_handler('loader', 'input_loaded')
    def ack(self, payload):
        _log.info(payload)
