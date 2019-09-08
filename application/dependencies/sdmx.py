import itertools
import requests
from lxml import etree
from nameko.dependency_providers import DependencyProvider

STRUCTURE_NAMESPACES = {
    '2.1': {
        'default': {
            'mes': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'str': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
            'com': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        },
        'ILO': {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
            'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure'
        }
    },
    'ilo': {
        'default': {
            'mes': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/message',
            'str': 'http://www.SDMX.org/resources/SDMXML/schemas/v2_0/structure'
        }
    }
}

GENERIC_NAMESPACES = {
    '2.1': {
        'default': {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'generic': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/generic',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
    },
    'ilo': {
        'default': {}
    }
}

SPECIFIC_NAMESPACES = {
    '2.1': {
        'default': {
            'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
            'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
        }
    },
    'ilo': {
        'default': {}
    }
}

XMLS = (
    'application/xml',
    'application/vnd.sdmx.structure+xml',
    'text/xml',
    'application/vnd.sdmx.structurespecificdata+xml',
    'application/vnd.sdmx.genericdata+xml'
)

PREFIX_ALIASES = {
    'com': 'common',
    'str': 'structure',
    'mes': 'messsage'
}


class SDMXRequestError(Exception):
    pass


def parse_response(resp, content_type):
    clean_type = content_type.split(';')[0]
    if clean_type in XMLS:
        return etree.fromstring(resp.content)

    if clean_type == 'application/json':
        return resp.json()

    raise ValueError(f'Unsupported content type: {clean_type}')


def sdmx_request(url, **kwargs):
    resp = requests.get(url, **kwargs)

    if resp.status_code != 200:
        raise SDMXRequestError(
            f'Non 200 HTTP response: {resp.status_code}: {resp.text}')

    content_type = resp.headers.get('Content-Type', None)

    return parse_response(resp, content_type)


class SDMXML(object):

    def __init__(self, root_url, agency_id, version, kind):
        self.root_url = root_url
        self.agency_id = agency_id
        self.version = version
        self.structure_namespaces = STRUCTURE_NAMESPACES[version].get(
            agency_id, STRUCTURE_NAMESPACES[version]['default'])
        self.kind = kind
        if self.kind == 'specific':
            self.data_namespaces = SPECIFIC_NAMESPACES[version].get(
                agency_id, SPECIFIC_NAMESPACES[version]['default'])
        elif self.kind == 'generic':
            self.data_namespaces = GENERIC_NAMESPACES[version].get(
                agency_id, GENERIC_NAMESPACES[version]['default'])
        self.com_prefix = self._find_prefix('com', self.structure_namespaces)
        self.str_prefix = self._find_prefix('str', self.structure_namespaces)
        self.mes_prefix = self._find_prefix('mes', self.structure_namespaces)

    def _find_prefix(self, prefix, namespaces):
        if prefix in namespaces:
            return prefix
        return PREFIX_ALIASES[prefix]

    def _build_dataflow(self, tree):

        def build_dataflow_21(node):
            id_ = node.attrib['id']
            name = node.xpath(
                f'./{self.com_prefix}:Name[@xml:lang="en"]', namespaces=self.structure_namespaces)
            structures = node.xpath(f'./{self.str_prefix}:Structure/Ref[1]',
                                    namespaces=self.structure_namespaces)
            if not structures:
                raise SDMXRequestError(
                    'Can not find structure in dataflow!')
            struct = {
                'id': structures[0].attrib['id'],
                'agency_id': structures[0].attrib['agencyID']
            }

            return {
                'id': id_,
                'name': name[0].text if name else None,
                'structure': struct
            }

        def build_dataflow_ilo(node):
            id_ = node.attrib['id']
            name = node.xpath(
                f'./{self.str_prefix}:Name[@xml:lang="en"]', namespaces=self.structure_namespaces)
            family_ref = node.xpath(
                f'./{self.str_prefix}:KeyFamilyRef[1]', namespaces=self.structure_namespaces)
            if not family_ref:
                raise SDMXRequestError(
                    'Can not find structure in dataflow!')
            struct = {
                'id': family_ref[0].xpath(
                    f'./{self.str_prefix}:KeyFamilyID[1]', namespaces=self.structure_namespaces)[0].text,
                'agency_id': family_ref[0].xpath(
                    f'./{self.str_prefix}:KeyFamilyAgencyID[1]', namespaces=self.structure_namespaces)[0].text
            }
            return {
                'id': id_,
                'name': name[0].text if name else None,
                'structure': struct
            }

        def build_dataflow(node):
            if self.version == '2.1':
                return build_dataflow_21(node)
            elif self.version == 'ilo':
                return build_dataflow_ilo(node)
            raise ValueError(f'Unsupported version {self.version}')

        return [build_dataflow(n) for n in tree.xpath(
            f'//{self.str_prefix}:Dataflow', namespaces=self.structure_namespaces)]

    def dataflows(self):
        url = f'{self.root_url}/dataflow/{self.agency_id}'
        root = sdmx_request(url)
        return self._build_dataflow(root)

    def dataflow(self, dataflow):
        url = f'{self.root_url}/dataflow/{self.agency_id}/{dataflow}'
        root = sdmx_request(url)
        result = self._build_dataflow(root)
        if not result:
            return None
        return result[0]

    def _codelistv21(self, tree, dimensions, attributes):
        codelists = tree.xpath(
            f'.//{self.str_prefix}:Codelist', namespaces=self.structure_namespaces)

        def handle_codelist(node):
            id_ = node.attrib['id']

            def handle_code(code):
                return (
                    id_, 
                    code.attrib['id'], 
                    node.xpath(
                        f'./{self.com_prefix}:Name[@xml:lang="en"][1]', namespaces=self.structure_namespaces)[0].text,
                    code.xpath(
                        f'./{self.com_prefix}:Name[@xml:lang="en"][1]', namespaces=self.structure_namespaces)[0].text
                )
            return [handle_code(c) for c in node.xpath(f'./{self.str_prefix}:Code', namespaces=self.structure_namespaces)]

        if codelists:
            return list(itertools.chain.from_iterable([handle_codelist(n) for n in codelists]))

        dims = filter(lambda x: x[1] is not None,
                      itertools.chain(dimensions, attributes))

        def get_codelist(el):
            _, code_id = el
            url = f'{self.root_url}/codelist/{self.agency_id}/{code_id}'
            cl_root = sdmx_request(url)
            return cl_root.xpath(f'.//{self.str_prefix}:Codelist', namespaces=self.structure_namespaces)

        return list(itertools.chain.from_iterable(
            [handle_codelist(c) for d in dims for c in get_codelist(d)]))

    def _codelistilo(self, tree, dimensions, attributes):
        codelists = tree.xpath(
            './/str:CodeList', namespaces=self.structure_namespaces)

        def handle_codelist(node):
            id_ = node.attrib['id']

            def handle_code(code):
                return (id_, code.attrib['value'], node.xpath('./str:Name[@xml:lang="en"][1]', namespaces=self.structure_namespaces)[0].text)
            return [handle_code(c) for c in node.xpath('./str:Code', namespaces=self.structure_namespaces)]

        if codelists:
            return list(itertools.chain.from_iterable([handle_codelist(n) for n in codelists]))

        dims = filter(lambda x: x[1] is not None,
                      itertools.chain(dimensions, attributes))

        def get_codelist(el):
            _, code_id = el
            url = f'{self.root_url}/codelist/{self.agency_id}/{code_id}'
            cl_root = sdmx_request(url)
            return cl_root.xpath('.//str:CodeList', namespaces=self.structure_namespaces)

        return list(itertools.chain.from_iterable(
            [handle_codelist(c) for d in dims for c in get_codelist(d)]))

    def _codelist(self, tree, dimensions, attributes):
        if self.version == '2.1':
            return self._codelistv21(tree, dimensions, attributes)
        if self.version == 'ilo':
            return self._codelistilo(tree, dimensions, attributes)

    def _dsdv21(self, root):

        def handle_node(node):
            id_ = node.attrib['id']
            reprs = node.xpath(
                f'./{self.str_prefix}:LocalRepresentation[1]', namespaces=self.structure_namespaces)
            if not reprs:
                return (id_, None)
            enum = reprs[0].xpath(f'./{self.str_prefix}:Enumeration/Ref[1]',
                                  namespaces=self.structure_namespaces)
            if enum:
                return (id_, enum[0].attrib['id'])
            return (id_, None)

        dimensions = [
            handle_node(n) for n in root.xpath(
                f'//{self.str_prefix}:DimensionList/{self.str_prefix}:Dimension', namespaces=self.structure_namespaces)]
        attributes = [
            handle_node(n) for n in root.xpath(
                f'//{self.str_prefix}:AttributeList/{self.str_prefix}:Attribute', namespaces=self.structure_namespaces)]
        codelist = self._codelist(root, dimensions, attributes)
        td_node = root.xpath(
            f'//{self.str_prefix}:DimensionList/{self.str_prefix}:TimeDimension[1]', namespaces=self.structure_namespaces)
        if not td_node:
            raise SDMXRequestError('Time dimension not found!')
        pm_node = root.xpath(
            f'//{self.str_prefix}:MeasureList/{self.str_prefix}:PrimaryMeasure', namespaces=self.structure_namespaces)
        if not pm_node:
            raise SDMXRequestError('Primary measure not found')
        return {
            'dimensions': dimensions,
            'attributes': attributes,
            'codelist': codelist,
            'time_dimension': td_node[0].attrib['id'],
            'primary_measure': pm_node[0].attrib['id']
        }

    def _dsdilo(self, root):
        def handle_node(node):
            return (node.attrib['conceptRef'], node.attrib.get('codelist', None))

        dimensions = [
            handle_node(n) for n in root.xpath(
                f'//{self.str_prefix}:Dimension', namespaces=self.structure_namespaces)]
        attributes = [
            handle_node(n) for n in root.xpath(
                f'//{self.str_prefix}:Attribute', namespaces=self.structure_namespaces)]
        codelist = self._codelist(root, dimensions, attributes)
        pm_node = root.xpath(
            f'//{self.str_prefix}:PrimaryMeasure[1]', namespaces=self.structure_namespaces)
        if not pm_node:
            raise SDMXRequestError('Primary measure not found!')
        td_node = root.xpath(
            f'//{self.str_prefix}:TimeDimension[1]', namespaces=self.structure_namespaces)
        if not td_node:
            raise SDMXRequestError('Time dimension not found!')
        return {
            'dimensions': dimensions,
            'attributes': attributes,
            'codelist': codelist,
            'primary_measure': pm_node[0].attrib['conceptRef'],
            'time_dimension': td_node[0].attrib['conceptRef']
        }

    def dsd(self, dataflow):
        url = f'{self.root_url}/datastructure/{self.agency_id}/{dataflow}'
        root = sdmx_request(url)
        if self.version == '2.1':
            return self._dsdv21(root)
        if self.version == 'ilo':
            return self._dsdilo(root)

    def _dict_to_smdx_query(self, dimensions, keys):
        return '.'.join([
            keys.get(d[0], '') for d in dimensions
        ])

    def _data(self, resource_id, dsd, query=None):
        if self.kind != 'specific':
            raise ValueError(f'{self.kind} not supported yet!')
        headers = {
            'Accept': 'application/vnd.sdmx.structurespecificdata+xml;version=2.1'
            if self.kind == 'specific' else 'application/vnd.sdmx.genericdata+xml;version=2.1'}
        url = f'{self.root_url}/data/{resource_id}/{query or ""}'
        root = sdmx_request(url, headers=headers)

        def handle_serie(serie):
            dims = {d[0]: serie.attrib.get(d[0], None) for d in dsd['dimensions']}

            def handle_obs(obs):
                atts = {a[0]: obs.attrib.get(
                    a[0], None) for a in dsd['attributes'] 
                    + [(dsd['time_dimension'], None)] 
                    + [(dsd['primary_measure'], None)]}
                return dict(dims, **atts)
            
            return [handle_obs(n) for n in serie.xpath(
                './Obs', namespaces=self.data_namespaces)]

        return itertools.chain.from_iterable(
            [handle_serie(n) for n in root.xpath(
                '//Series', namespaces=self.data_namespaces)])

    def get_sdmx(self, resource_id, keys={}):
        df = self.dataflow(resource_id)
        dsd_id = df['structure']['id']
        dsd = self.dsd(dsd_id)
        query = self._dict_to_smdx_query(dsd['dimensions'], keys)
        return {
            'dataflow': df,
            'query': query,
            'data': self._data(resource_id, dsd, query),
            **dsd
        }


class SDMXWrapper(object):

    def initialize(self, root_url, agency_id, resource_id, version, kind, keys):
        req = SDMXML(root_url, agency_id, version, kind)
        self.flow = req.get_sdmx(resource_id, keys)

        self.agency_dataflows = SDMXML(root_url, agency_id, version, kind)\
            .dataflows()

    def name(self):
        return self.flow['dataflow']['name']

    def dimensions(self):
        return self.flow['dimensions']

    def attributes(self):
        return self.flow['attributes']

    def codelist(self):
        return self.flow['codelist']

    def data(self):
        return self.flow['data']

    def query(self):
        return self.flow['query']

    def dataflow(self):
        return self.flow['dataflow']

    def primary_measure(self):
        return self.flow['primary_measure']

    def time_dimension(self):
        return self.flow['time_dimension']


class SDMX(DependencyProvider):

    def get_dependency(self, worker_ctx):
        return SDMXWrapper()
