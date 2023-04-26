import csv
import pathlib
import random
import string
import tempfile
from typing import Optional, Tuple, List
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

from utils import time_it


XML_FILES_COUNT = 1000  # Affects overall I/O bound
XML_FILE_NESTING = 1000  # Affects overall CPU bound


def _generate_xml_content(
        count: int = XML_FILES_COUNT,
        nesting: Optional[int] = XML_FILE_NESTING
) -> Tuple[int, bytes]:
    for i in range(count):
        root = Element('root')
        SubElement(root, "var", attrib={'name': 'id', 'value': str(uuid4())})
        SubElement(root, "var", attrib={'name': 'level', 'value': str(random.randint(1, 100))})
        objects = SubElement(root, "objects")
        for _ in range(nesting or random.randint(1, 10)):
            SubElement(objects, "object", attrib={'name': ''.join(random.choices(string.ascii_uppercase, k=20))})
        yield i, tostring(root)


@time_it
def store_xml(tmp_path: pathlib.Path):
    for index, content in _generate_xml_content():
        path = tmp_path / f'{index}.xml'
        path.write_bytes(content)

    assert len(list(tmp_path.iterdir())) == XML_FILES_COUNT


def _parse_xml(tmp_path: pathlib.Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    id_and_level, id_and_object_name = [], []
    for file in tmp_path.iterdir():
        content = file.read_bytes()
        doc = fromstring(content)
        doc_id = doc.find(".//var[@name='id']").attrib['value']
        doc_level = doc.find(".//var[@name='level']").attrib['value']
        doc_object_names = [tag.attrib['name'] for tag in doc.findall('.//object[@name]')]

        id_and_level.append((doc_id, doc_level))
        id_and_object_name.extend([(doc_id, name) for name in doc_object_names])

    return id_and_level, id_and_object_name


@time_it
def store_csv(tmp_path: pathlib.Path):
    first_csv_data, second_csv_data = _parse_xml(tmp_path)

    path = tmp_path / 'first.csv'
    with path.open("w", encoding="utf-8") as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(first_csv_data)

    path = tmp_path / 'second.csv'
    with path.open("w", encoding="utf-8") as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(second_csv_data)


def run_context():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)

        store_xml(tmp_path)
        store_csv(tmp_path)

        assert len(list(tmp_path.iterdir())) == XML_FILES_COUNT + 2


if __name__ == '__main__':
    run_context()
