import pytest
from src.project import clearing_path
import os
import tempfile


temp1 = tempfile.NamedTemporaryFile(prefix='todelete', delete=False)
temp2 = tempfile.NamedTemporaryFile(prefix='todelete', delete=False)
temp_path = os.path.dirname(temp2.name)
name_1 = temp1.name
name_2 = temp2.name
name_3 = 'random/random/random'
tempfiles = [
    (name_1, True),
    (name_2, True),
    (name_3, False)
]

@pytest.mark.parametrize('path, result', tempfiles)
def test_if_files_exist(path, result):
    assert os.path.exists(path) == result


def test_if_clearing_works():
    clearing_path(temp_path, True, 'todelete')
    assert os.path.exists(name_1) == False
    assert os.path.exists(name_2) == False
