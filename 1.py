# Загрузить списки людей из файлов small_data и big_data в Excel на разные листы
# На листе small_data записи должны быть отсортированы по фамилии, на листе big_data по имени.
import json
import pandas as pd
from io import TextIOWrapper  # для красивой аннотации

# константы
PATH_EXCEL: str = 'Aibim.Test work/data_json.xlsx'
PATH_JSON: str = 'Aibim.Test work\\JSON\\'


def json_sorter(json_data: list, elem: int = 0, tag: str = "Name") -> list:
    '''Сортировка json_data по значению elem ключа tag'''
    return sorted(json_data, key=lambda x: x[tag].split()[elem])


def tag_returner(temp_json: TextIOWrapper, tag: str) -> list:
    '''Возвращает list с элементами из temp_json по ключу tag'''
    return [i[tag] for i in temp_json]


def data_sheet(temp_name: TextIOWrapper) -> dict:
    """Формирует лист в Excel"""
    return {
        "ID": tag_returner(temp_name, "ID"),
        "Name": tag_returner(temp_name, "Name"),
        "Age": tag_returner(temp_name, "Age")
    }


# вытягиваем из json-а данные, попутно сортируя
with open(PATH_JSON+'small_data_persons.json', encoding='utf8') as inf:
    temp_small: list = json_sorter(json.load(inf), 1)
with open(PATH_JSON+'big_data_persons.json', encoding='utf8') as inf:
    temp_big: list = json_sorter(json.load(inf), 0)

# завозим наши json-данные в DataFrames
df_small: pd.DataFrame = pd.DataFrame(data_sheet(temp_small))
df_big: pd.DataFrame = pd.DataFrame(data_sheet(temp_big))

# список листов: их названия и содержимое
income_sheets: dict = {'Small_sheet': df_small, 'Big_sheet': df_big}

# запись в файл .xlsx
with pd.ExcelWriter(PATH_EXCEL, engine='xlsxwriter') as writer:
    for sheet_name, sheed_data in income_sheets.items():
        income_sheets[sheet_name].to_excel(
            writer, sheet_name=sheet_name, index=False)
# missions 1.3 и 1.4 completed
