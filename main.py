import argparse
import csv
import itertools
import pathlib
import random
import string
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from typing import Optional, List, Tuple
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, fromstring, tostring

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


def _blocking_write(zf: zipfile.ZipFile, item: Tuple[int, bytes]):
    name, content = item
    zf.writestr(f'{name}.xml', content)


def _zip(path: pathlib.Path, chunk):
    path = path / f'{chunk[0][0]}.zip'
    with zipfile.ZipFile(path, 'w') as z:
        for item in chunk:
            _blocking_write(z, item)  # sadly, ZipFile doesn't support concurrent writes out of the box


@time_it
def store_zip(
    tmp_path: pathlib.Path,
    zip_count: int,
    xml_count: int,
    xml_nesting: Optional[int],
    thread_pool: ThreadPoolExecutor,
    process_pool: ProcessPoolExecutor
):
    """
    Concurrently creates XML content & stores it via zip containers in local filesystem.
    :param tmp_path: temp dir to save files to
    :param zip_count: number of zip files to be created.
    :param xml_count: number of XML files to be created.
    :param xml_nesting: number of objects nested within each XML file.
    :param thread_pool: thread pool instance
    :param process_pool: process pool instance
    """
    # tested vs plain .map() via python main.py -z 50 -c 20 -n 1000
    contents = process_pool.map(
        partial(_generate_xml_content, nesting=xml_nesting),
        [i for i in range(zip_count * xml_count)]
    )

    chunks = (list(itertools.islice(contents, xml_count)) for _ in range(zip_count))
    # tested vs plain .map() via python main.py -z 1000 -c 1 -n 1000
    list(thread_pool.map(partial(_zip, tmp_path), chunks))

    assert len(list(tmp_path.iterdir())) == zip_count  # sanity check


def _concurrent_read(path: pathlib.Path) -> List[bytes]:
    """
    Looks like https://github.com/python/cpython/pull/26974 has been backported to Python 3.9 and on,
    so we can try using concurrent reads
    :param path: zip file path
    :return: list of bytes content
    """
    with zipfile.ZipFile(path, 'r') as zf:
        return list(map(zf.read, zf.namelist()))


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
    Concurrently fetches all XML files from zips in given dir and calculates some meta based on their content
    & stores it in two CSV files.
    :param tmp_path: temp dir to save files to
    :param thread_pool: thread pool instance
    :param process_pool: process pool instance
    """
    first_csv_data, second_csv_data = [], []

    # tested vs plain .map() via python main.py -z 4000 -c 1 -n 10
    nested_bytes_iter = thread_pool.map(_concurrent_read, tmp_path.iterdir())

    # tested vs plain .map() via python main.py -z 1 -c 1000 -n 1000
    for pair in process_pool.map(_parse_xml, itertools.chain(*nested_bytes_iter)):
        first_csv_data.append(pair[0])
        second_csv_data.extend(pair[1])

    #  number of CSV files is known & constant, so it seems like an overkill to write these two concurrently
    path = tmp_path / 'first.csv'
    with path.open("w", encoding="utf-8", newline='') as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(first_csv_data)

    path = tmp_path / 'second.csv'
    with path.open("w", encoding="utf-8", newline='') as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(second_csv_data)


def run_context(args: argparse.Namespace):
    zip_count = args.zip_count
    xml_count = args.xml_count
    xml_nesting = int(args.xml_nesting) if args.xml_nesting else None

    thread_pool = ThreadPoolExecutor()
    process_pool = ProcessPoolExecutor()

    tempdir = tempfile.TemporaryDirectory()

    try:
        tmp_path = pathlib.Path(tempdir.name)

        store_zip(tmp_path, zip_count, xml_count, xml_nesting, thread_pool, process_pool)
        store_csv(tmp_path, thread_pool, process_pool)

        assert len(list(tmp_path.iterdir())) == zip_count + 2  # sanity check
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
    parser = argparse.ArgumentParser(description='Run a script demonstrating I/O & CPU-bounded process execution time.')
    parser.add_argument(
        '-z',
        dest='zip_count',
        action='store',
        default=50,
        type=int,
        help='How many zip files will be created (default: 50). I/O & CPU-bounded param'
    )
    parser.add_argument(
        '-c',
        dest='xml_count',
        action='store',
        default=100,
        type=int,
        help='How many XML files will be created per zip file (default: 100). I/O-bounded param'
    )
    parser.add_argument(
        '-n',
        dest='xml_nesting',
        action='store',
        default=None,
        help='How nested each XML will be (default: random [1..10]). CPU-bounded param'
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
