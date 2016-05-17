import os
import shutil
import pytest

from retriever import SCRIPT_LIST
from retriever.lib.get_opts import parser
from retriever.lib.tools import json2csv
from retriever.lib.tools import get_path_md5
from retriever.lib.tools import name_matches
from retriever.lib.tools import choose_engine

DUMP_DIR = "output_dumps"


def setup_ouput():
    if os.path.exists(DUMP_DIR):
        shutil.rmtree(DUMP_DIR)
    os.makedirs(DUMP_DIR)
    os.chdir(DUMP_DIR)
    os.system("retriever update")


def teardown():
    os.system(" rm -r output_*")
    os.system("rm -r raw_data/MoM2003")


def tocsv(arg=None,file_type=None):
    if arg:
        engine = choose_engine(arg.__dict__)
        script_list = SCRIPT_LIST()
        scripts = name_matches(script_list, arg.dataset)
        for dataset_i in scripts:
            dataset_i.download(engine, arg.debug, True)
    if file_type:
        if file_type == 'json':
            [json2csv(files) for r, dir, files in os.walk(DUMP_DIR)]
            os.remove("*.json")
        # if file_type == 'xml':
        #     [xml2csv(files) for r, dir, files in os.walk(DUMP_DIR)]
        #     os.remove("*.xml")
    os.chdir("..")
    current_md5 = get_path_md5("output_dumps")
    return current_md5

download = [
    ('DelMoral2010', '2b6e92b014ae73ea1f0195ecfdf6248d'),
    ('AvianBodySize', 'dce81ee0f040295cd14c857c18cc3f7e'),
    ('MoM2003', 'b54b80d0d1959bdea0bb8a59b70fa871'),
]
csvhashes = [
    ('DelMoral2010', 'e79d55ac15f1a70a6c7d3ad4e678ec0e'),
    ('AvianBodySize', 'f42702a53e7d99d16e909676f30e5aa8'),
    ('MoM2003', 'ef0a31c132cfe1c6594739c872f70f54'),
]
hashes = [
    ('DelMoral2010', '734831950c432d0002b84ed1d26f949e'),
    ('AvianBodySize', '0f503559426ba0c2dbd56e58882988f5'),
    ('MoM2003', '527456ac4f9abdfef36c76c4c4f4295f'),
]


@pytest.mark.parametrize("dataset,expected", hashes)
def test_sqlite_regression(dataset, expected):
    """Check for regression for a particular dataset imported to sqlite"""
    dbfile = os.path.normpath(os.path.join(os.getcwd(), "output_database"))
    os.system("retriever install sqlite {0} -f {1}".format(dataset, dbfile))
    setup_ouput()
    arg = parser.parse_args(['install', 'sqlite', dataset, '-f', dbfile])
    current_md5 = tocsv(arg)
    assert current_md5 == expected


@pytest.mark.parametrize("dataset,expected", hashes)
def test_postgres_regression(dataset, expected):
    """Check for regression for a particular dataset imported to postgres"""
    os.system('psql -U postgres -d testdb -h localhost -c "DROP SCHEMA IF EXISTS testschema CASCADE"')
    os.system("retriever install postgres %s -u postgres -d testdb -a testschema" % dataset)
    setup_ouput()
    arg = parser.parse_args(['install', 'postgres', dataset, '-u', 'postgres', '-d', 'testdb', '-a', 'testschema'])
    current_md5 = tocsv(arg)
    assert current_md5 == expected


@pytest.mark.parametrize("dataset,expected", hashes)
def test_mysql_regression(dataset, expected):
    """Check for regression for a particular dataset imported to mysql"""
    os.system('mysql -u travis -Bse "DROP DATABASE IF EXISTS testdb"')
    os.system("retriever install mysql %s -u travis -d  testdb" % dataset)
    setup_ouput()
    arg = parser.parse_args(['install', 'mysql', dataset, '-u', 'travis', '-d', 'testdb'])
    current_md5 = tocsv(arg)
    assert current_md5 == expected


@pytest.mark.parametrize("dataset,expected", download)
def test_download_regression( dataset, expected):
    """Check for regression for a particular dataset downloaded only"""
    os.system("retriever download {0} -p raw_data/{0}".format(dataset))
    current_md5 = get_path_md5("raw_data/%s" % (dataset), mode="rU")
    assert current_md5 == expected


@pytest.mark.parametrize("dataset,expected", csvhashes)
def test_csv_regression(dataset, expected):
    """Check for regression for a particular dataset imported to csv"""
    os.system("retriever install csv %s -t output_file_{table}" % dataset)
    os.system("cat output_file_* > output_file")
    current_md5 = get_path_md5('output_file', mode="rU")
    assert current_md5 == expected

# @pytest.mark.parametrize("dataset,expected", hashes)
# def test_jsonengine_regression(dataset, expected):
#     """Check for regression for a particular dataset imported to csv"""
#     setup_ouput()
#     os.system("retriever install json %s" % dataset)
#     current_md5 = tocsv(arg=None, file_type='json')
#     assert current_md5 == expected
