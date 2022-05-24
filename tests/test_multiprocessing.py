import pytest
import os
import json
from src.project import generate_dataset_parallel
from multiprocessing import JoinableQueue
import multiprocessing as mp


data = json.loads("{\"name\": \"str:rand\",\"date\": \"timestamp:\",\"type\": \"str:['client', 'goat', 'booboo']\","
                  "\"age\": \"int:rand(1,100)\"}")

attrs2 = ['/Users/jjarosz/PycharmProjects/cap_project/tests', ['files', 10], 'saving_test', 'count', 10, False, 4]




def test_multiprocessing():
    work_to_do = JoinableQueue()
    [work_to_do.put(i) for i in range(attrs2[1][1])]
    process_pool = [mp.Process(target=generate_dataset_parallel,
                               args=(attrs2, data, work_to_do)) for _ in range(attrs2[6])]
    [p.start() for p in process_pool]
    [p.join() for p in process_pool]
    [p.close() for p in process_pool]

    for i in range(attrs2[1][1]-1):
        assert os.path.exists(attrs2[0] + '/' + f'{attrs2[2]}_{i}.txt') == True

    for file in os.listdir(attrs2[0]):
        if file.startswith('saving_test'):
            os.remove(attrs2[0] + '/' + file)

