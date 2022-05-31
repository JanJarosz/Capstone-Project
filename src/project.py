import argparse
import queue
from configparser import ConfigParser
import json
import sys
import re
import logging
import os
import time
import random
import ast
import uuid
import multiprocessing as mp
from multiprocessing import Process


def config_logs():
    log_format = "%(levelname)s %(asctime)s - %(message)s"
    logging.basicConfig(filename='../logs/logs.txt',
                        level=logging.DEBUG,
                        format=log_format,
                        filemode='w')


def config_parser():
    config = ConfigParser()
    config.read('configuration.ini')

    parser = argparse.ArgumentParser(prog='Data Generator', description='Generate dataset from given schema.')

    parser.add_argument('-p', '--path_to_files', type=str, default=config['DEFAULT']['path_to_files'],
                        help='The path where files should be saved "current" means current directory.'
                             'Default - current | Possible (current or any directory')

    parser.add_argument('-f', '--files_count', type=int, default=config['DEFAULT']['files_count'],
                        help='How many files should be generated. If 0 all output will be printed in console'
                             'Default - 0 | Possible (any positive integer)')

    parser.add_argument('-b', '--base_file_name', type=str, default=config['DEFAULT']['base_file_name'],
                        help='Base name of each file. If "files_count" > 1 base name will be continued with prefix'
                             'Default = dataset | Possible (any word or phrase without spaces)')

    parser.add_argument('-x', '--file_prefix', default=config['DEFAULT']['file_prefix'],
                        choices=['count', 'uuid'],
                        help='Prefix placed after basename when files_count is more than 1'
                             'Default - count | Possible (count or uuid)')

    parser.add_argument('-d', '--data_schema', type=str, default=config['DEFAULT']['data_schema'],
                        help='Schema of generated data. Should be in JSON notation like "{\"something\": "something\"'
                             'Default - written schema | Possible (path to JSON file or written schema in console')

    parser.add_argument('-l', '--data_lines', type=int, default=config['DEFAULT']['data_lines'],
                        help='Number of lines in each JSON file. Default - 10 | Possible (any positive integer')

    parser.add_argument('-c', '--clear_path', action='store_false',
                        help='Use this argument to clear all files in directory, witch starts with the base_file_name'
                             'Default - True | If you dont want to clear, write "-c" in console')

    parser.add_argument('-m', '--multiprocessing', type=int, default=config['DEFAULT']['multiprocessing'],
                        help='How many processes should be used to create files.'
                             'Default = 1 | Possible (any positive integer but wil be used only as many as nuber of '
                             'CPU cores.')

    args = parser.parse_args()

    return args


def identify_schema_source(inputs):
    try:
        schema = json.loads(inputs)
        if not isinstance(schema, dict):
            schema = json.loads(schema)
    except json.decoder.JSONDecodeError:
        if os.path.exists(inputs):
            logging.warning('schema given as path to file')
            with open(f'{inputs}', 'r') as f:
                schema = json.load(f)
                if not isinstance(schema, dict):
                    schema = json.loads(schema)
        else:
            sys.exit('Path to file with JSON data schema doesnt exist')
    return schema


def process_schema(dic):
    """Check correctness of a given data schema"""
    possible_types = ['timestamp', 'int', 'str', ]
    pattern_rand_1 = '^[0-9].*'
    pattern_rand_2 = '^[a-z].*'
    pattern_rand_3 = '\[.*\]'
    pattern_rand_4 = 'rand\([0-9]*, ?[0-9]*\)'
    final_dict = {}
    for key in dic:
        if ':' in dic[key]:
            divider = dic[key].index(':')
            left = dic[key][:divider]
            right = dic[key][divider + 1:]
            if left not in possible_types:
                sys.exit(f'forbidden type of data to generate -> {left}')
            if re.match(pattern_rand_2, right.lower()) or re.match(pattern_rand_1, right.lower()):
                pass
            elif re.match(pattern_rand_3, right.lower()):
                pass
            elif right == '':
                pass
            else:
                sys.exit(f'forbidden right side of notation -> {right}')

        else:
            sys.exit('forbidden format of input. Value must be like "type of data to generate:what to generate"')

        """Generating data line"""
        if dic[key].lower().startswith('timestamp'):
            if not dic[key].lower().endswith(':'):
                pass
            final_dict[key] = time.time()

        elif dic[key].lower().startswith('str:'):
            if dic[key][4:].lower() == 'rand':
                final_dict[key] = str(uuid.uuid4())
            elif re.match(pattern_rand_3, dic[key][4:]):
                final_dict[key] = random.choice(ast.literal_eval(dic[key][4:]))
            else:
                final_dict[key] = dic[key][4:]

        elif dic[key].lower().startswith('int:'):
            if re.match(pattern_rand_3, dic[key][4:].lower()):
                final_dict[key] = random.choice(ast.literal_eval(dic[key][4:]))
                try:
                    int(final_dict[key])
                except ValueError:
                    sys.exit(f'list to draw must contains only digits -> {final_dict[key]}')

            elif dic[key][4:].lower() == 'rand':
                final_dict[key] = random.randint(0, 10000)

            elif re.match(pattern_rand_4, dic[key][4:]):
                comma = dic[key].index(',')
                p1 = dic[key].index('(')
                p2 = dic[key].index(')')
                floor = dic[key][p1 + 1:comma]
                ceiling = dic[key][comma + 1:p2]
                if not isinstance(int(floor), int) or not isinstance(int(ceiling), int):
                    sys.exit(f'given scope has to be an "int" -> {floor} or {ceiling}')
                final_dict[key] = random.randint(int(floor), int(ceiling))

            else:
                final_dict[key] = dic[key][4:]
                try:
                    final_dict[key] = int(final_dict[key])
                except ValueError:
                    pass
                if not isinstance(final_dict[key], int):
                    sys.exit(f'given age has to be an "int" -> {final_dict[key]}')
    return final_dict


def process_attributes(attrs_dict):
    if attrs_dict['path_to_files'].lower() == 'current':
        saving_path = os.getcwd()
    elif os.path.exists(attrs_dict['path_to_files']):
        saving_path = attrs_dict['path_to_files']
    elif not os.path.exists(attrs_dict['path_to_files']):
        sys.exit(f'Path {attrs_dict["path_to_files"]} does not exist')

    if attrs_dict['files_count'] < 0:
        sys.exit('I cant create negative number of files')
    elif attrs_dict['files_count'] == 0:
        how_to_display = 'console'
        set_prefix = False
    elif attrs_dict['files_count'] == 1:
        how_to_display = 'files'
        set_prefix = False
    elif attrs_dict['files_count'] > 1:
        how_to_display = 'files'
        set_prefix = attrs_dict['file_prefix']

    if attrs_dict['multiprocessing'] <= 0:
        sys.exit('I cant run negative or none number of processes')
    elif attrs_dict['multiprocessing'] == 1:
        number_of_processes = 1
    else:
        number_of_processes = min(attrs_dict['multiprocessing'], os.cpu_count())

    return [saving_path, [how_to_display, attrs_dict['files_count']], attrs_dict['base_file_name'], set_prefix,
            attrs_dict['data_lines'], attrs_dict['clear_path'], number_of_processes]


def clearing_path(path, flag, filename):
    if flag:
        for file in os.listdir(path):
            if file.startswith(filename):
                os.remove(path + '/' + file)
    else:
        logging.warning('cleaning flag is off. Path will not be cleaned')
        return False


def generate_dataset_basic(attributes, data):
    """setting way of display"""
    if attributes[1][0] == 'console':
        for _ in range(attributes[4]):
            data_line = process_schema(data)
            print(data_line)
    else:
        if not attributes[3]:
            with open(f'{attributes[0]}/{attributes[2]}.txt', 'w', encoding='utf-8') as f:
                for _ in range(attributes[4]):
                    line = json.dumps(process_schema(data))
                    f.write(f'{line}\n')

        elif attributes[3].lower() == 'count':
            for i in range(attributes[1][1]):
                with open(f'{attributes[0]}/{attributes[2]}_{i}.txt', 'w', encoding='utf-8') as f:
                    for _ in range(attributes[4]):
                        line = json.dumps(process_schema(data))
                        f.write(f'{line}\n')

        elif attributes[3].lower() == 'uuid':
            for _ in range(attributes[1][1]):
                with open(f'{attributes[0]}/{attributes[2]}_{str(uuid.uuid4())}.txt', 'w', encoding='utf-8') as f:
                    for _ in range(attributes[4]):
                        line = json.dumps(process_schema(data))
                        f.write(f'{line}\n')
        else:
            sys.exit(f'forbidden prefix for files -> {attributes[3]}')


def generate_dataset_parallel(attributes, data, tasks):
    while True:
        try:
            name = tasks.get_nowait()
        except queue.Empty:
            print('empty queue')
            break

        if not attributes[3]:
            sys.exit('Creating one file with few processes is no sense')

        else:
            with open(f'{attributes[0]}/{attributes[2]}_{name}.txt', 'w', encoding='utf-8') as f:
                for _ in range(attributes[4]):
                    line = json.dumps(process_schema(data))
                    f.write(f'{line}\n')


def main():
    config_logs()
    logging.debug('configuring argparser and uploading default options...')
    args = config_parser()
    logging.info('parser configured')

    logging.debug('identifying schema source...')
    schema = identify_schema_source(args.data_schema)
    logging.info('schema source identified')

    for item in schema:
        if schema[item].lower().startswith('timestamp'):
            if not schema[item].lower().endswith(':'):
                logging.warning(f'timestamp does not support any values. {item} will be set as current time.')

    logging.debug('processing given attributes...')
    proc_at = process_attributes(vars(args))
    logging.info('attributes processed')

    logging.debug('starting clearing path...')
    clearing_path(proc_at[0], proc_at[5], proc_at[2])
    logging.info('clearing finished.')

    if proc_at[6] == 1:
        start = time.time()
        logging.debug('checking correctness and generating the data in a single process...')
        generate_dataset_basic(proc_at, schema)
        logging.info('data generated')
        stop = time.time()
        print(stop-start)
    else:
        start = time.time()
        if proc_at[1][0] == 'console':
            sys.exit('Printing data to console with many processes is no sense')

        logging.debug(f'checking correctness and generating the data in {proc_at[6]} processes...')

        mp.set_start_method('fork')
        work_to_do = mp.JoinableQueue()
        if proc_at[3] == 'uuid':
            [work_to_do.put(str(uuid.uuid4())) for _ in range(proc_at[1][1])]
        elif proc_at[3] == 'count':
            [work_to_do.put(i) for i in range(proc_at[1][1])]
        else:
            sys.exit(f'forbidden prefix for files -> {proc_at[3]}')
        process_pool = [Process(target=generate_dataset_parallel,
                                args=(proc_at, schema, work_to_do)) for _ in range(proc_at[6])]
        [p.start() for p in process_pool]
        [p.join() for p in process_pool]
        [p.close() for p in process_pool]
        logging.info('data generated')
        stop = time.time()
        print(stop-start)


if __name__ == '__main__':
    main()

