import itertools
from pandasdmx import Request
from nameko.dependency_providers import DependencyProvider


class SDMXWrapper(object):

    def initialize(self, provider, dataflow):
        self.req = Request(provider)
        self.provider = provider
        self.dataflow = dataflow
        self.flow = self.req.dataflow(dataflow)
        self.dsd = self.flow.dataflow[dataflow].structure()

    def name(self):
        return self.flow.dataflow[self.dataflow].name.get('en', None)

    def dimensions(self):
        return [(k, label, v.local_repr.enum.id if v.local_repr.enum else None)
                for k, v in self.dsd.dimensions.items() if k != 'TIME_PERIOD'
                for lang, label in v.concept.name.items() if lang == 'en']

    def attributes(self):
        return [(k, v.local_repr.enum.id if v.local_repr.enum else None)
                for k, v in self.dsd.attributes.items()]

    def codelist(self):
        return [(c, k, label)
                for c in self.flow.msg.codelist
                for k, v in self.flow.msg.codelist[c].items()
                for lang, label in v.name.items() if lang == 'en']
    
    def data(self):
        data_response = self.req.data(resource_id=self.dataflow)

        def handle_serie(serie):
            keys = dict(serie.key._asdict())
            return [{
                **keys,
                **{k: v for k, v in o._asdict().items() if k != 'attrib'},
                **{k: v for k, v in o['attrib']._asdict().items()}}
                for o in list(serie.obs())]

        return itertools.chain.from_iterable([handle_serie(s) for s in data_response.msg.data.series])


class SDMX(DependencyProvider):

    def get_dependency(self, worker_ctx):
        return SDMXWrapper()
