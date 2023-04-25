'''
Написать программу на Python, которая делает следующие действия:

1. Создает 50 zip-архивов, в каждом 100 xml файлов со случайными данными следующей структуры:

<root>
    <var name=’id’ value=’<случайное уникальное строковое значение>’/>
    <var name=’level’ value=’<случайное число от 1 до 100>’/>
        <objects>
            <object name=’<случайное строковое значение>’/>
            <object name=’<случайное строковое значение>’/>
            …
        </objects>
</root>

В тэге objects случайное число (от 1 до 10) вложенных тэгов object.

2. Обрабатывает директорию с полученными zip архивами, разбирает вложенные xml файлы и формирует 2 csv файла:
    Первый: id, level - по одной строке на каждый xml файл
    Второй: id, object_name - по отдельной строке для каждого тэга object (получится от 1 до 10 строк на каждый xml файл)

Очень желательно сделать так, чтобы задание 2 эффективно использовало ресурсы многоядерного процессора.
'''


import csv
import pathlib
import random
import string
import tempfile
import time
from typing import Optional, Tuple, List
from uuid import uuid4
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring


def generate_xml_content(
        count: int = 50,
        level: Optional[int] = None,
        nesting: Optional[int] = None
) -> Tuple[int, bytes]:
    for i in range(count):
        root = Element('root')
        SubElement(root, "var", attrib={'name': 'id', 'value': str(uuid4())})
        SubElement(root, "var", attrib={'name': 'level', 'value': str(level or random.randint(1, 100))})
        objects = SubElement(root, "objects")
        for _ in range(nesting or random.randint(1, 10)):
            SubElement(objects, "object", attrib={'name': ''.join(random.choices(string.ascii_uppercase, k=20))})
        yield i, tostring(root)


def store_xml(tmp_path: pathlib.Path):
    for index, content in generate_xml_content():
        path = tmp_path / f'{index}.xml'
        path.write_bytes(content)


def parse_xml(tmp_path: pathlib.Path) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
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


def store_csv(tmp_path: pathlib.Path, id_and_level: List[Tuple[str, str]], id_and_object_name: List[Tuple[str, str]]):
    path = tmp_path / 'fisrt.csv'
    with path.open("w", encoding="utf-8") as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(id_and_level)

    path = tmp_path / 'second.csv'
    with path.open("w", encoding="utf-8") as file:
        csv.writer(file, delimiter=',', quoting=csv.QUOTE_ALL).writerows(id_and_object_name)


def run_context():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = pathlib.Path(tmp)

        store_xml(tmp_path)

        first_csv_data, second_csv_data = parse_xml(tmp_path)

        store_csv(tmp_path, first_csv_data, second_csv_data)


if __name__ == '__main__':
    start = time.time()
    run_context()
    end = time.time()
    print(f'Execution time is: {end - start:.3f} s')
