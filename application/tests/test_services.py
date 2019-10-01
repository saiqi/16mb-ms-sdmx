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

    def mock_initialize(root_url, agency, resource, version, kind, keys):
        if agency != 'INSEE':
            raise ValueError('Unknown provider!')
        return
    service.sdmx.initialize.side_effect = mock_initialize

    service.add_dataflow('http://foo.bar', 'INSEE', 'DATAFLOW', '2.1', 'specific', {})
    assert service.database.dataset.find_one({'agency': 'INSEE'})

    with pytest.raises(SDMXCollectorError):
        service.add_dataflow('http://foo.bar', '?', '?', '2.1', 'specific', {})


def test_get_dataset(database):
    service = worker_factory(SDMXCollectorService, database=database)

    def mock_codelist():
        return [
            ('CL_AGE', '0', 'desc', 'foo'),
            ('CL_AGE', '10', 'desc', 'foo'),
            ('CL_INDICATEUR', 'XY', 'desc', 'foo'),
            ('CL_INDICATEUR', 'Y', 'desc', 'foo'),
            ('CL_OBS_TYPE', 'DEF', 'desc', 'foo'),
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
            ('AGE', 'CL_AGE'),
            ('indicateur', 'CL_INDICATEUR'),
            ('unknown', 'CL_DIM')
        ]
    service.sdmx.dimensions.side_effect = mock_dimensions

    def mock_primary_measure():
        return 'obs_value'
    service.sdmx.primary_measure.side_effect = mock_primary_measure

    def mock_time_dimension():
        return 'time_dimension'
    service.sdmx.time_dimension.side_effect = mock_time_dimension

    def mock_data():
        return ([{'AGE': '0', 'indicateur': 'XY', 'time_dimension': '2019-Q4', 'obs_value': '35' if r > 0 else 'NaN'} for r in range(5)])
    service.sdmx.data.side_effect = mock_data

    dataset = service.get_dataset('http://foo.bar', 'INSEE', 'MY-DATASET', '2.1', 'specific', {})
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
    assert 'AGE' in records[0]
    assert records[0]['AGE'] == '0'
    assert 'indicateur' in records[0]
    assert 'obs_value' in records[0]
    assert 'time_dimension' in records[0]
    assert 'query' in records[0]
    assert records[0]['obs_value'] is None
    assert 'unknown' in records[0]
    assert records[0]['unknown'] is None
    assert datastore[1]['target_table'] == 'insee_codelist'
    assert len(datastore[1]['records'][0]) == 5
    assert 'ref' in datastore[1]['records'][0]

    meta = datastore[0]['meta']
    age = next(filter(lambda x: x[0] == 'AGE', meta))
    assert age[1] == 'VARCHAR(2)'
    unknown = next(filter(lambda x: x[0] == 'unknown', meta))
    assert unknown[1] == 'TEXT'

    referential = dataset['referential']
    assert referential['entities']
    assert isinstance(referential['entities'], list)
    assert 'common_name' in referential['entities'][0]
    assert 'id' in referential['entities'][0]
    assert referential['entities'][0]['id'] == 'insee_my_dataset'
