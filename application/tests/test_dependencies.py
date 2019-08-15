import eventlet
eventlet.monkey_patch()

import vcr
from application.dependencies.sdmx import SDMXWrapper

@vcr.use_cassette('application/tests/vcr_cassette/insee_meta.yaml')
def test_insee_metadata():
    sdmx = SDMXWrapper('INSEE', 'CHOMAGE-TRIM-NATIONAL')

    assert sdmx.name
    assert isinstance(sdmx.name, str)

    assert sdmx.dimensions
    assert isinstance(sdmx.dimensions, list)

    assert sdmx.codelist
    assert isinstance(sdmx.codelist, list)

    assert sdmx.attributes
    assert isinstance(sdmx.attributes, list)

@vcr.use_cassette('application/tests/vcr_cassette/insee_data.yaml')
def test_insee_data():
    sdmx = SDMXWrapper('INSEE', 'CHOMAGE-TRIM-NATIONAL')
    data = sdmx.data
    first = next(data)
    assert isinstance(first, dict)