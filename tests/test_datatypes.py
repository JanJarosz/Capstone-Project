import pytest
from src.project import process_schema
import json


dtypes = [
    (json.loads("{\"name\": \"int:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(1,100)\"}"), 'Name must be a "str"'),
    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"int:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(1,100)\"}"), 'Type must be a "str"'),
    (json.loads("{\"name\": \"str:rand\",\"date\": \"time:\",\"type\": \"int:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(1,100)\"}"), 'forbidden type of data to generate -> time'),
    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
     "\"age\": \"str:rand(1,100)\"}"), 'age must be an "int"')
]


@pytest.mark.parametrize('content, result', dtypes)
def test_dtypes(content, result):
    with pytest.raises(SystemExit) as cm:
        process_schema(content)
    assert cm.type == SystemExit
    assert cm.value.code == result
