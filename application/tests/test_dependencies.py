import eventlet
eventlet.monkey_patch()

import vcr
from nameko.testing.services import dummy, entrypoint_hook
from application.dependencies.sdmx import SDMXWrapper, SDMX

@vcr.use_cassette('application/tests/vcr_cassette/insee_meta.yaml')
def test_insee_metadata():
    sdmx = SDMXWrapper()
    sdmx.initialize('INSEE', 'CHOMAGE-TRIM-NATIONAL')

    assert sdmx.name()
    assert isinstance(sdmx.name(), str)

    assert sdmx.dimensions()
    assert isinstance(sdmx.dimensions(), list)

    assert sdmx.codelist()
    assert isinstance(sdmx.codelist(), list)

    assert sdmx.attributes()
    assert isinstance(sdmx.attributes(), list)

@vcr.use_cassette('application/tests/vcr_cassette/insee_data.yaml')
def test_insee_data():
    sdmx = SDMXWrapper()
    sdmx.initialize('INSEE', 'CHOMAGE-TRIM-NATIONAL')

    data = sdmx.data()
    first = next(data)
    assert isinstance(first, dict)


class DummyService(object):
    name = 'dummy_service'
    sdmx = SDMX()

    @dummy
    def get_meta(self, provider, dataflow):
        self.sdmx.initialize(provider, dataflow)

        return self.sdmx.codelist(), self.sdmx.attributes(), self.sdmx.dimensions()


@vcr.use_cassette('application/tests/vcr_cassette/insee_meta.yaml')
def test_end_to_end(container_factory):
    container = container_factory(DummyService, {})
    container.start()

    with entrypoint_hook(container, 'get_meta') as get_meta:
        c, a, d = get_meta('INSEE', 'CHOMAGE-TRIM-NATIONAL')
        assert isinstance(c, list)
        assert isinstance(a, list)
        assert isinstance(d, list)