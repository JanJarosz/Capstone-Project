import pytest
from src.project import process_schema
import json
from time import time
from unittest import mock

schemas = [
    (json.loads("{\"foo\": \"str:goat\",\"date\": \"timestamp:\",\"type\": \"str:goat\","
                "\"age\": \"int:50\"}"), {"foo": "goat", "date": 1234, "type": "goat", "age": 50}),

    (json.loads("{\"name\": \"str:rand\",\"birth\": \"timestamp:\",\"type\": \"str:goat\","
                "\"age\": \"int:50\"}"), {"name": "goat", "birth": 1234, "type": "goat", "age": 50}),

    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"something_stupid\": \"str:['goat', 'dog', 'cat']\","
                "\"age\": \"int:50\"}"), {"name": "goat", "date": 1234, "something_stupid": "goat", "age": 50}),

    (json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['goat', 'dog', 'cat']\","
                "\"ice_age\": \"int:rand(1,100)\"}"), {"name": "goat", "date": 1234, "type": "goat", "ice_age": 37})

]


@pytest.mark.parametrize('content, result', schemas)
@mock.patch('time.time', mock.MagicMock(return_value=1234))
@mock.patch('uuid.uuid4', mock.MagicMock(return_value='goat'))
@mock.patch('random.randint', mock.MagicMock(return_value=37))
@mock.patch('random.choice', mock.MagicMock(return_value='goat'))
def test_dtypes(content, result):
    data = process_schema(content)
    assert data == result
