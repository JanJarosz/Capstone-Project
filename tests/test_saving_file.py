import pytest
import os
import json
from src.project import process_schema
from src.project import generate_dataset_basic
from unittest import mock

data = json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
                  "\"age\": \"int:rand(1,100)\"}")


attrs1 = ['/Users/jjarosz/PycharmProjects/cap_project/tests', ['files', 1], 'saving_test', False, 10, False, 1]

attrs2 = ['/Users/jjarosz/PycharmProjects/cap_project/tests', ['files', 2], 'saving_test', 'count', 10, False, 1]

attrs3 = ['/Users/jjarosz/PycharmProjects/cap_project/tests', ['files', 2], 'saving_test', 'uuid', 10, False, 1]


def test_save_file():
    generate_dataset_basic(attrs1, data)
    assert os.path.exists(f"{attrs1[0]}/{attrs1[2]}.txt") == True
    for file in os.listdir(attrs1[0]):
        if file.endswith('test.txt'):
            os.remove(attrs1[0] + '/' + file)


def test_save_file_count():
    generate_dataset_basic(attrs2, data)
    for i in range(attrs2[1][1]):
        assert os.path.exists(attrs2[0] + '/' + f'{attrs2[2]}_{i}.txt') == True
    for file in os.listdir(attrs2[0]):
        if file.endswith('0.txt') or file.endswith('1.txt'):
            os.remove(attrs2[0] + '/' + file)


@mock.patch('uuid.uuid4', mock.MagicMock(return_value='goat'))
def test_save_file_uuid():
    generate_dataset_basic(attrs3, data)
    for _ in range(attrs3[1][1]):
        assert os.path.exists(attrs3[0] + '/' + f'{attrs3[2]}_goat.txt') == True
    for file in os.listdir(attrs3[0]):
        if file.endswith('goat.txt'):
            os.remove(attrs3[0] + '/' + file)
