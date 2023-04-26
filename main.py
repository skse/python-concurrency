import argparse
import csv
import pathlib
import random
import string
import tempfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from typing import Optional, List, Tuple
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

from utils import time_it, try_open_file_manager


def _generate_xml_content(
        index: int,
        nesting: Optional[int]
) -> Tuple[int, bytes]:
    """
    Populates XML template with random data.
    :param index: represents output file's name
    :param nesting: defines how nested the internal XML structure will be. CPU-intensive op
    :return: tuple consisting of the file index & bytes string representing its content (populated XML)
    """
    root = Element('root')
    SubElement(root, "var", attrib={'name': 'id', 'value': str(uuid4())})
    SubElement(root, "var", attrib={'name': 'level', 'value': str(random.randint(1, 100))})
    objects = SubElement(root, "objects")
    for _ in range(nesting or random.randint(1, 10)):
        SubElement(objects, "object", attrib={'name': ''.join(random.choices(string.ascii_uppercase, k=20))})
    return index, tostring(root)


def _blocking_write(tmp_path: pathlib.Path, meta: Tuple[int, bytes]):
    name, content = meta
    path = tmp_path / f'{name}.xml'
    path.write_bytes(content)


@time_it
def store_xml(
    tmp_path: pathlib.Path,
    xml_count: int,
    xml_nesting: Optional[int],
    thread_pool: ThreadPoolExecutor,
    process_pool: ProcessPoolExecutor
):
    """
    Concurrently creates XML content & stores it in local filesystem.
    :param tmp_path: temp dir to save files to
    :param xml_count: Number of XML files to be created. I/O bound param.
    :param xml_nesting: Number of objects nested within each XML file. CPU bound param.
    :param thread_pool: thread pool instance
    :param process_pool: process pool instance
    """

    data = process_pool.map(partial(_generate_xml_content, nesting=xml_nesting), (i for i in range(xml_count)))
    list(thread_pool.map(partial(_blocking_write, tmp_path), data))

    assert len(list(tmp_path.iterdir())) == xml_count  # sanity check


def _blocking_read(path: pathlib.Path) -> bytes:
    return path.read_bytes()


def _parse_xml(content: bytes) -> Tuple[Tuple[str, str], List[Tuple[str, str]]]:
    """
    Read XML content & calculate associated meta.
    :param content: XML content string
    :return: data required for populating the result CSVs
    """
    doc = fromstring(content)

    doc_id = doc.find(".//var[@name='id']").attrib['value']
    doc_level = doc.find(".//var[@name='level']").attrib['value']
    doc_object_names = [tag.attrib['name'] for tag in doc.findall('.//object[@name]')]

    return (doc_id, doc_level), [(doc_id, object_name) for object_name in doc_object_names]


@time_it
def store_csv(tmp_path: pathlib.Path, thread_pool: ThreadPoolExecutor, process_pool: ProcessPoolExecutor):
    """
    Concurrently fetches all XML files from given dir, calculates some meta based on their contents
    & stores it in two CSV files.
    :param tmp_path: temp dir to save files to
    :param thread_pool: thread pool instance
    :param process_pool: process pool instance
    """
    first_csv_data, second_csv_data = [], []
    
    bytes_list = thread_pool.map(_blocking_read, tmp_path.iterdir())
    for pair in process_pool.map(_parse_xml, bytes_list):
        first_csv_data.append(pair[0])
        second_csv_data.extend(pair[1])

    #  number of CSV files is known & constant, so it seems like an overkill to write them concurrently
    path = tmp_path / 'first.csv'
    with path.open("w", encoding="utf-8", newline='') as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(first_csv_data)

    path = tmp_path / 'second.csv'
    with path.open("w", encoding="utf-8", newline='') as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(second_csv_data)


def run_context(args: argparse.Namespace):
    xml_count = int(args.xml_count)
    xml_nesting = int(args.xml_nesting) if args.xml_nesting else None

    thread_pool = ThreadPoolExecutor()
    process_pool = ProcessPoolExecutor()

    tempdir = tempfile.TemporaryDirectory()
    
    try:
        tmp_path = pathlib.Path(tempdir.name)

        store_xml(tmp_path, xml_count, xml_nesting, thread_pool, process_pool)
        store_csv(tmp_path, thread_pool, process_pool)

        assert len(list(tmp_path.iterdir())) == xml_count + 2  # sanity check
    except Exception as e:
        print(f'Something went totally wrong: {e}')
    finally:
        thread_pool.shutdown()
        process_pool.shutdown()
        if args.keep_output:
            print(f"You can view the output directory at {tempdir.name}")
            try_open_file_manager(tempdir.name)
            input('Press enter to delete temp files...')
        tempdir.cleanup()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a script demonstrating I/O & CPU bound process load.')
    parser.add_argument(
        '-c',
        dest='xml_count',
        action='store',
        default=50,
        help='How many XML files will be crated (default: 50)'
    )
    parser.add_argument(
        '-n',
        dest='xml_nesting',
        action='store',
        default=None,
        help='How nested each XML will be (default: random [1..10])'
    )
    parser.add_argument(
        '--view',
        dest='keep_output',
        action='store_const',
        default=False,
        const=True,
        help='Stop script execution to view files in the output directory (default: False)'
    )

    run_context(parser.parse_args())
