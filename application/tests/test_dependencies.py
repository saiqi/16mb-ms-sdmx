import eventlet
eventlet.monkey_patch()

import vcr
from nameko.testing.services import dummy, entrypoint_hook
from application.dependencies.sdmx import SDMX


class DummyService(object):
    name = 'dummy_service'
    sdmx = SDMX()

    @dummy
    def get_meta(self, agency_id, resource_id):
        self.sdmx.initialize('https://bdm.insee.fr/series/sdmx', agency_id, resource_id, '2.1', 'specific', {})

        return self.sdmx.codelist(), self.sdmx.attributes(), self.sdmx.dimensions()


@vcr.use_cassette('application/tests/vcr_cassette/meta.yaml')
def test_end_to_end(container_factory):
    container = container_factory(DummyService, {})
    container.start()

    with entrypoint_hook(container, 'get_meta') as get_meta:
        c, a, d = get_meta('FR1', 'CHOMAGE-TRIM-NATIONAL')
        assert isinstance(c, list)
        assert isinstance(a, list)
        assert isinstance(d, list)