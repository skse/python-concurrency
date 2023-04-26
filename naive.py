import csv
import pathlib
import random
import string
import tempfile
import zipfile
from typing import Optional, Tuple, List
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, fromstring, ElementTree

from utils import time_it


ZIP_FILES_COUNT = 50  # both I/O and CPU bounded
XML_FILES_COUNT = 20  # I/O bounded
XML_FILE_NESTING = 1000  # CPU bounded


def _generate_xml_content(
    index: int = XML_FILES_COUNT,
    nesting: Optional[int] = XML_FILE_NESTING
) -> Tuple[int, ElementTree]:
    root = Element('root')
    tree = ElementTree(root)
    SubElement(root, "var", attrib={'name': 'id', 'value': str(uuid4())})
    SubElement(root, "var", attrib={'name': 'level', 'value': str(random.randint(1, 100))})
    objects = SubElement(root, "objects")
    for _ in range(nesting or random.randint(1, 10)):
        SubElement(objects, "object", attrib={'name': ''.join(random.choices(string.ascii_uppercase, k=20))})
    return index, tree


@time_it
def store_zip(tmp_path: pathlib.Path):
    contents = [_generate_xml_content(i) for i in range(ZIP_FILES_COUNT * XML_FILES_COUNT)]

    chunks = [contents[i:i + XML_FILES_COUNT] for i in range(0, len(contents), XML_FILES_COUNT)]
    for chunk in chunks:
        path = tmp_path / f'{chunk[0][0]}.zip'
        with zipfile.ZipFile(path, 'w') as z:
            for item in chunk:
                name, tree = item
                with z.open(f'{name}.xml', 'w') as f:
                    tree.write(f, encoding='UTF-8', xml_declaration=True)

    assert len(list(tmp_path.iterdir())) == ZIP_FILES_COUNT


def _parse_xml(tmp_path: pathlib.Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    id_and_level, id_and_object_name = [], []
    for file in tmp_path.iterdir():
        with zipfile.ZipFile(file, 'r') as zf:
            for content in list(path.read_bytes() for path in zipfile.Path(zf).iterdir()):
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

        store_zip(tmp_path)
        store_csv(tmp_path)

        assert len(list(tmp_path.iterdir())) == ZIP_FILES_COUNT + 2


if __name__ == '__main__':
    run_context()
