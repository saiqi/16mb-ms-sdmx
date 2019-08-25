import logging
import re
import math
import functools
import itertools
import hashlib
from nameko.dependency_providers import DependencyProvider
from nameko.rpc import rpc
from nameko.timer import timer
from nameko.events import event_handler, BROADCAST
from nameko.messaging import Publisher
from nameko.constants import PERSISTENT
from kombu.messaging import Exchange
from nameko_mongodb import MongoDatabase
import bson.json_util
import pymongo
from application.dependencies.sdmx import SDMX

_log = logging.getLogger(__name__)


class ErrorHandler(DependencyProvider):

    def worker_result(self, worker_ctx, res, exc_info):
        if exc_info is None:
            return

        exc_type, exc, tb = exc_info
        _log.error(str(exc))


class SDMXCollectorError(Exception):
    pass


class SDMXCollectorService(object):
    name = 'sdmx_collector'
    database = MongoDatabase(result_backend=False)
    sdmx = SDMX()
    error = ErrorHandler()
    pub_input = Publisher(exchange=Exchange(
        name='all_inputs', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))
    pub_notif = Publisher(exchange=Exchange(
        name='all_notifications', type='topic', durable=True, auto_delete=True, delivery_mode=PERSISTENT))

    def add_dataflow(self, provider, dataflow):
        try:
            self.sdmx.initialize(provider, dataflow)
            self.sdmx.name
        except Exception as e:
            raise SDMXCollectorError(str(e))
        self.database['dataset'].create_index(
            [('provider', pymongo.ASCENDING), ('dataflow', pymongo.ASCENDING)])
        self.database['dataset'].create_index('id')
        _id = SDMXCollectorService.table_name(provider, dataflow)
        doc = {'provider': provider, 'dataflow': dataflow, 'id': _id}
        self.database['dataset'].update_one(doc, {'$set': doc}, upsert=True)
        return _id

    def get_dataflows(self):
        return self.database['dataset'].find({})

    @staticmethod
    def clean(l):
        return re.sub(r'[^0-9a-zA-Z_]+', '_', l)

    @staticmethod
    def table_name(provider, dataflow):
        return f'{SDMXCollectorService.clean(provider.lower())}_{SDMXCollectorService.clean(dataflow.lower())}'

    @staticmethod
    def to_table_meta(meta, provider, dataflow):
        table_name = SDMXCollectorService.table_name(provider, dataflow)
        dim_cl = [(d[0], d[2]) for d in meta['dimensions']]

        codelist = meta['codelist']

        def handle_dim_att(d):
            name, code = d
            cl = [c for c in codelist if c[0] == code]
            if not cl:
                return (SDMXCollectorService.clean(name.lower()), f'TEXT')
            size = functools.reduce(lambda x, y: len(
                y[1]) if len(y[1]) > x else x, cl, 1)
            return (SDMXCollectorService.clean(name.lower()), f'VARCHAR({size})')

        table_meta = [handle_dim_att(d) for d in itertools.chain(
            dim_cl, meta['attributes']) if d[1]]
        table_meta.append(('dim', 'VARCHAR(20)'))
        table_meta.append(('value', 'FLOAT'))

        return {
            'write_policy': 'truncate_bulk_insert',
            'meta': table_meta,
            'target_table': table_name,
            'chunk_size': 500
        }

    @staticmethod
    def checksum(data):
        return hashlib.md5(
            ''.join([str(r) for r in data]).encode('utf-8')).hexdigest()

    def get_status(self, provider, dataflow, checksum):
        old = self.database['dataset'].find_one(
            {'provider': provider, 'dataflow': dataflow})
        if not old or 'checksum' not in old:
            return 'CREATED'
        if old['checksum'] == checksum:
            return 'UNCHANGED'
        return 'UPDATED'

    def get_dataset(self, provider, dataflow):
        self.sdmx.initialize(provider, dataflow)
        meta = {
            'name': self.sdmx.name(),
            'codelist': self.sdmx.codelist(),
            'dimensions': self.sdmx.dimensions(),
            'attributes': self.sdmx.attributes()
        }
        table_meta = SDMXCollectorService.to_table_meta(
            meta, provider, dataflow)
        
        def handle_number(m, v):
            if m.lower() in ('float', 'double'):
                try:
                    d = float(v)
                    if math.isnan(d):
                        return None
                    return d
                except:
                    return None
            return v

        data = [{k[0]: handle_number(k[1], r.get(k[0], None)) for k in table_meta['meta']}
                for r in self.sdmx.data()]
        checksum = SDMXCollectorService.checksum(data)
        return {
            'referential': {
                'entities': [
                    {
                        'id': table_meta['target_table'],
                        'common_name': meta['name'],
                        'provider': 'internal',
                        'type': 'dataset',
                        'informations': {
                            'id': table_meta['target_table'],
                            'name': meta['name'],
                            'table': table_meta['target_table']
                        }
                    }
                ]
            },
            'datastore': [{
                **table_meta,
                'records': data
            }],
            'checksum': checksum,
            'id': table_meta['target_table'],
            'status': self.get_status(provider, dataflow, checksum),
            'meta': {
                'type': SDMXCollectorService.clean(provider).lower(),
                'source': 'sdmx'
            }
        }

    def update_checksum(self, id_, checksum):
        self.database['dataset'].update_one(
            {'id': id_}, {'$set': {'checksum': checksum}})

    @timer(interval=24*60*60)
    @rpc
    def publish(self):
        for f in self.get_dataflows():
            provider = f['provider']
            dataflow = f['dataflow']
            _log.info(
                f'Downloading dataset {dataflow} provided by {provider} ...')
            try:
                dataset = self.get_dataset(provider, dataflow)
            except:
                _log.error(f'Can not handle dataset {dataflow} provided by {provider}!')
                continue
            _log.info('Publishing ...')
            self.pub_input(bson.json_util.dumps(dataset))

    @event_handler(
        'loader', 'input_loaded', handler_type=BROADCAST, reliable_delivery=False)
    def ack(self, payload):
        msg = bson.json_util.loads(payload)
        meta = msg.get('meta', None)
        if not meta:
            return
        checksum = msg.get('checksum', None)
        if not checksum:
            return
        if 'source' not in meta or meta['source'] != 'sdmx':
            return
        t = meta['type']

        _log.info(f'Acknowledging {t} file: {msg["id"]}')
        self.update_checksum(msg['id'], checksum)

        _log.info(f'Publishing notification for {msg["id"]}')
        self.pub_notif(bson.json_util.dumps({
            'id': msg['id'],
            'source': 'sdmx',
            'type': t,
            'content': msg["id"]}))

    @event_handler(
        'api_service', 'input_config', handler_type=BROADCAST, reliable_delivery=False)
    def handle_input_config(self, payload):
        msg = bson.json_util.loads(payload)

        if 'meta' not in msg or 'source' not in msg['meta'] or msg['meta']['source'] != 'sdmx':
            return

        _log.info('Received a related input config ...')
        if 'config' not in msg:
            _log.warning('No config within the message. Ignoring ...')

        config = msg['config']

        if 'provider' not in config or 'dataflow' not in config:
            _log.warning(
                'Either provider or dataflow is missing within config')

        id_ = self.add_dataflow(config['provider'], config['dataflow'])

        self.pub_notif(bson.json_util.dumps({
            'id': id_,
            'source': msg['meta']['source'],
            'type': '',
            'content': 'A new SDMX feed has been added.'}))
