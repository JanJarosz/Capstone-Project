import pytest
import json
from src.project import identify_schema_source


@pytest.fixture(scope="session")
def schema_file(tmpdir_factory):
    data = json.dumps("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['goat', 'dog', 'cat']\","
                      "\"age\": \"int:rand(1,100)\"}")
    fn = tmpdir_factory.mktemp("data").join("sch.txt")
    with open(fn, 'w'):
        fn.write(data)
    return fn


def test_upload(schema_file):
    assert identify_schema_source(str(schema_file)) == {'age': 'int:rand(1,100)',
                                                        'date': 'timestamp:',
                                                        'name': 'str:rand',
                                                        'type': "str:['goat', 'dog', 'cat']"}
