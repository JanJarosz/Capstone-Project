from src.project import process_attributes
import pytest
import json

dictionaries_correct = [
    ({"path_to_files": "/Users/jjarosz/PycharmProjects/Capstone/files", "files_count": 10, "base_file_name": "basement",
      "file_prefix": "uuid", "data_lines": 100, "clear_path": True, "multiprocessing": 8},
     ['/Users/jjarosz/PycharmProjects/Capstone/files', ['files', 10], 'basement', 'uuid', 100, True, 8])
]

dictionaries_bugged = [
    ({"path_to_files": "blablabla", "files_count": 10, "base_file_name": "basement",
      "file_prefix": "uuid", "data_lines": 100, "clear_path": True, "multiprocessing": 8},
     'Path blablabla does not exist'),
]


@pytest.mark.parametrize('dict, result', dictionaries_correct)
def test_process_attrs(dict, result):
    assert process_attributes(dict) == result

@pytest.mark.parametrize('dict, result', dictionaries_bugged)
def test_process_attrs(dict, result):
    with pytest.raises(SystemExit) as cm:
        process_attributes(dict)
    assert cm.type == SystemExit
    assert cm.value.code == result
