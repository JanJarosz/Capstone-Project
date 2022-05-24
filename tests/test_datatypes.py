import pytest
from src.project import process_schema
import json


dtypes = [
    (json.loads("{\"name\": \"float:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(1,100)\"}"), 'forbidden type of data to generate -> float'),

    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"int:['client', 'client']\","
     "\"age\": \"int:rand(1,100)\"}"), 'list to draw must contains only digits -> client'),

    (json.loads("{\"name\": \"str:rand\",\"date\": \"time:\",\"type\": \"int:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(1,100)\"}"), 'forbidden type of data to generate -> time'),

    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
     "\"age\": \"int:rand(dog,100)\"}"), 'given age has to be an "int" -> rand(dog,100)')
]


@pytest.mark.parametrize('content, result', dtypes)
def test_dtypes(content, result):
    with pytest.raises(SystemExit) as cm:
        process_schema(content)
    assert cm.type == SystemExit
    assert cm.value.code == result
