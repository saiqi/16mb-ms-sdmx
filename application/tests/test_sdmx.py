import eventlet
eventlet.monkey_patch()

import vcr
from application.dependencies.sdmx import SDMXML


def check_dataflow(df):
    assert df
    
    assert df[0]
    assert isinstance(df[0], dict)
    assert 'id' in df[0]
    assert df[0]['id']
    assert 'name' in df[0]
    assert 'structure' in df[0]
    assert isinstance(df[0]['structure'], dict)
    assert 'id' in df[0]['structure']
    assert 'agency_id' in df[0]['structure']

@vcr.use_cassette('application/tests/vcr_cassette/FR1/dataflow.yaml')
def test_fr1_dataflow():
    df = SDMXML('https://bdm.insee.fr/series/sdmx', 'FR1', '2.1', 'specific').dataflows()
    check_dataflow(df)

@vcr.use_cassette('application/tests/vcr_cassette/FR1/dataflow_chomage.yaml')
def test_fr1_chomage_dataflow():
    df = SDMXML('https://bdm.insee.fr/series/sdmx', 'FR1', '2.1', 'specific').dataflow('CHOMAGE-TRIM-NATIONAL')
    assert df
    assert isinstance(df, dict)

@vcr.use_cassette('application/tests/vcr_cassette/ESTAT/dataflow.yaml')
def test_estat_dataflow():
    df = SDMXML('http://ec.europa.eu/eurostat/SDMX/diss-web/rest', 'ESTAT', '2.1', 'specific').dataflows()
    check_dataflow(df)

@vcr.use_cassette('application/tests/vcr_cassette/WB/dataflow.yaml')
def test_wb_dataflow():
    df = SDMXML('http://api.worldbank.org/v2/sdmx/rest', 'WB', '2.1', 'specific').dataflows()
    check_dataflow(df)

@vcr.use_cassette('application/tests/vcr_cassette/ILO/dataflow.yaml')
def test_ilo_dataflow():
    df = SDMXML('https://www.ilo.org/ilostat/sdmx/ws/rest', 'ILO', 'ilo', 'specific').dataflows()
    check_dataflow(df)


@vcr.use_cassette('application/tests/vcr_cassette/ILO/dataflow_std.yaml')
def test_ilo_std_dataflow():
    df = SDMXML('https://www.ilo.org/sdmx/rest', 'ILO', '2.1', 'specific').dataflows()
    check_dataflow(df)


def check_dsd(df):
    assert df
    assert isinstance(df, dict)
    assert 'primary_measure' in df
    assert isinstance(df['primary_measure'], str)
    assert 'time_dimension' in df
    assert isinstance(df['time_dimension'], str)
    for el in ('dimensions', 'attributes', 'codelist'):
        assert el in df
        print(el, df[el])
        assert isinstance(df[el], list)
        assert isinstance(df[el][0], tuple)

@vcr.use_cassette('application/tests/vcr_cassette/FR1/dsd.yaml')
def test_fr1_dsd():
    df = SDMXML('https://bdm.insee.fr/series/sdmx', 'FR1', '2.1', 'specific').dsd('CHOMAGE-TRIM-NATIONAL')
    check_dsd(df)

@vcr.use_cassette('application/tests/vcr_cassette/ESTAT/dsd.yaml')
def test_estat_dsd():
    df = SDMXML('http://ec.europa.eu/eurostat/SDMX/diss-web/rest', 'ESTAT', '2.1', 'specific').dsd('DSD_nama_10_gdp')
    check_dsd(df)

@vcr.use_cassette('application/tests/vcr_cassette/WB/dsd.yaml')
def test_wb_dsd():
    df = SDMXML('http://api.worldbank.org/v2/sdmx/rest', 'WB', '2.1', 'specific').dsd('WDI')
    check_dsd(df)

@vcr.use_cassette('application/tests/vcr_cassette/ILO/dsd.yaml')
def test_ilo_dsd():
    df = SDMXML('https://www.ilo.org/ilostat/sdmx/ws/rest', 'ILO', 'ilo', 'specific').dsd('CP_ALL_ALL')
    check_dsd(df)

@vcr.use_cassette('application/tests/vcr_cassette/ILO/dsd_std.yaml')
def test_ilo_std_dsd():
    df = SDMXML('https://www.ilo.org/sdmx/rest', 'ILO', '2.1', 'specific').dsd('DF_YI_ALL_EMP_TEMP_SEX_AGE_NB')
    check_dsd(df)

def check_data(d):
    assert 'dataflow' in d
    assert 'query' in d
    assert 'dimensions' in d
    assert 'attributes' in d
    assert 'codelist' in d
    assert 'time_dimension' in d
    assert 'primary_measure' in d
    assert 'data' in d
    assert d['primary_measure'] == 'OBS_VALUE'
    data = list(d['data'])
    assert data[0]
    assert isinstance(data[0], dict)
    assert data[0][d['primary_measure']]
    assert data[0][d['time_dimension']]

@vcr.use_cassette('application/tests/vcr_cassette/FR1/data.yaml')
def test_fr1_data():
    d = SDMXML('https://bdm.insee.fr/series/sdmx', 'FR1', '2.1', 'specific').get_sdmx('CHOMAGE-TRIM-NATIONAL')
    check_data(d)

@vcr.use_cassette('application/tests/vcr_cassette/ESTAT/data.yaml')
def test_estat_data():
    d = SDMXML('http://ec.europa.eu/eurostat/SDMX/diss-web/rest', 'ESTAT', '2.1', 'specific').get_sdmx(
        'nama_10_gdp', keys={'FREQ': 'A', 'GEO': 'FR', 'UNIT': 'CLV10_MEUR', 'NA_ITEM': 'B1GQ'})
    check_data(d)

@vcr.use_cassette('application/tests/vcr_cassette/ILO/data.yaml')
def test_ilo_std_data():
    d = SDMXML('https://www.ilo.org/sdmx/rest', 'ILO', '2.1', 'specific').get_sdmx(
        'DF_YI_ALL_EMP_TEMP_SEX_AGE_NB', keys={'SEX': 'SEX_T', 'AGE': 'AGE_5YRBANDS_TOTAL'})
    check_data(d)