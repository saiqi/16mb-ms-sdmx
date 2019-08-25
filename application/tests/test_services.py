from application.services.sdmx_collector import SDMXCollectorService, SDMXCollectorError
from nameko.testing.services import worker_factory
from pymongo import MongoClient
import pytest
import eventlet
eventlet.monkey_patch()


@pytest.fixture
def database():
    client = MongoClient()

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


def test_add_dataflow(database):
    service = worker_factory(SDMXCollectorService, database=database)

    def mock_initialize(provider, dataflow):
        if provider != 'INSEE':
            raise ValueError('Unknown provider!')
        return
    service.sdmx.initialize.side_effect = mock_initialize

    service.add_dataflow('INSEE', 'DATAFLOW')
    assert service.database.dataset.find_one({'provider': 'INSEE'})

    with pytest.raises(SDMXCollectorError):
        service.add_dataflow('?', '?')


def test_get_dataset(database):
    service = worker_factory(SDMXCollectorService, database=database)

    def mock_codelist():
        return [
            ('CL_AGE', '0', 'desc'),
            ('CL_AGE', '10', 'desc'),
            ('CL_INDICATEUR', 'XY', 'desc'),
            ('CL_INDICATEUR', 'Y', 'desc'),
            ('CL_OBS_TYPE', 'DEF', 'desc'),
        ]
    service.sdmx.codelist.side_effect = mock_codelist

    def mock_attributes():
        return [
            ('obs_type', 'CL_OBS_TYPE'),
            ('other', None)
        ]
    service.sdmx.attributes.side_effect = mock_attributes

    def mock_dimensions():
        return [
            ('age', 'desc', 'CL_AGE'),
            ('indicateur', 'desc', 'CL_INDICATEUR'),
            ('unknown', 'desc', 'CL_DIM')
        ]
    service.sdmx.dimensions.side_effect = mock_dimensions

    def mock_data():
        return ([{'age': '0', 'indicateur': 'XY', 'dim': '2019-Q4', 'value': '35' if r > 0 else 'NaN'} for r in range(5)])
    service.sdmx.data.side_effect = mock_data

    dataset = service.get_dataset('INSEE', 'MY-DATASET')
    assert 'referential' in dataset
    assert 'datastore' in dataset
    assert 'checksum' in dataset
    assert 'id' in dataset
    assert 'meta' in dataset
    assert 'status' in dataset

    assert dataset['meta']['type'] == 'insee'
    assert dataset['meta']['source'] == 'sdmx'

    assert dataset['status'] == 'CREATED'

    datastore = dataset['datastore']
    assert isinstance(datastore, list)
    assert datastore[0]['target_table'] == 'insee_my_dataset'
    records = datastore[0]['records']
    assert isinstance(records, list)
    assert isinstance(records[0], dict)
    assert 'age' in records[0]
    assert records[0]['age'] == '0'
    assert 'indicateur' in records[0]
    assert 'dim' in records[0]
    assert 'value' in records[0]
    assert records[0]['value'] is None
    assert 'unknown' in records[0]
    assert records[0]['unknown'] is None

    meta = datastore[0]['meta']
    age = next(filter(lambda x: x[0] == 'age', meta))
    assert age[1] == 'VARCHAR(2)'
    unknown = next(filter(lambda x: x[0] == 'unknown', meta))
    assert unknown[1] == 'TEXT'

    referential = dataset['referential']
    assert referential['entities']
    assert isinstance(referential['entities'], list)
    assert 'common_name' in referential['entities'][0]
    assert 'id' in referential['entities'][0]
    assert referential['entities'][0]['id'] == 'insee_my_dataset'
