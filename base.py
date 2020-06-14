import json
import re
from typing import Dict, List
import pandas as pd
from dataclasses import dataclass


def json_extracter(path: str, elem: int = 0, tag: str = "Name", JSON: str = "JSON\\") -> List[str]:
    """Extract data from json

    Args:
        path (str): path to file
        elem (int, optional): Key[elem] to sort. Defaults to 0.
        tag (str, optional): Key to sort. Defaults to "Name".

    Returns:
        List[str]: Sorted json by tag[elem]
    """
    def sorter(info: List[str]) -> List[str]:
        """Sorter to json"""
        return sorted(info, key=lambda x: x[tag].split()[elem])

    with open(JSON+path, encoding='utf8') as inf:
        return sorter(json.load(inf))


def exc_writer(name_file: str, sheets: Dict[str, pd.DataFrame],
               ext: str = '.xlsx', engine: str = 'xlsxwriter') -> None:
    """Write data to ext format file

    Args:
        name_file (str): name to file
        sheets (Dict[str, pd.DataFrame]): data to write in file
        ext(str, optional): file extension
        engine(str, optional): engine to writer

    Returns:
        None
    """
    with pd.ExcelWriter(name_file+ext, engine=engine) as writer:
        for sheet_name, sheed_data in sheets.items():
            sheets[sheet_name].to_excel(writer, sheet_name, index=False)
    print('Success!')


@dataclass
class DF_processor:
    """Class for operations with dfs
    """
    sm: pd.DataFrame
    big: pd.DataFrame

    def __post_init__(self):
        # dataframe(sm, big) merging
        self.df: pd.DataFrame = pd.concat(
            [self.sm, self.big], ignore_index=True)

    def only_in_small(self) -> pd.DataFrame:
        """People, which only in sm, and not in big

        Returns:
            pd.DataFrame
        """
        return self.sm[self.sm["Name"].isin(
            set(self.sm.Name) - set(self.big.Name))]

    def namesakes(self, dif: int = 10) -> pd.DataFrame:
        """Namesakes whose age difference = dif

        Args:
            dif (int, optional): age difference. Defaults to 10.

        Returns:
            pd.DataFrame:
        """
        # func to get surname
        def surname(x): return x[1]["Name"].split()[0]
        df_names: pd.DataFrame = pd.DataFrame()  # empty df
        surn: Dict[str, List[str]] = dict()

        # write in surn {surname: age}
        for row in self.df.iterrows():
            if surname(row) not in surn:
                surn[surname(row)] = [row[1]["Age"]]
            else:
                surn[surname(row)].append(row[1]["Age"])

        for row in self.df.iterrows():
            if surname(row) in surn:
                for j in surn[surname(row)]:
                    if abs(int(j) - int(row[1]["Age"])) == dif:
                        df_names = df_names.append(row[1], ignore_index=True)

        return df_names

    def english_leter_in(self) -> pd.DataFrame:
        """People, whose name/surname has еnglish letter in

        Returns:
            pd.DataFrame
        """
        return pd.DataFrame([row[1] for row in self.df.iterrows()
                             if re.search(r'[a-zA-Z]', row[1]["Name"])])


temp_small: List[str] = json_extracter('small_data_persons.json', 0)
temp_big: List[str] = json_extracter('big_data_persons.json', 1)

# завозим наши json-данные в DataFrames
df_small: pd.DataFrame = pd.DataFrame(temp_small)
df_big: pd.DataFrame = pd.DataFrame(temp_big)

df_cls = DF_processor(df_small, df_big)

if __name__ == "__main__":
    # список листов: их названия и содержимое
    sheets: Dict[str, pd.DataFrame] = {
        'small_data': df_small,
        'big_data': df_big,
        'missing names': df_cls.only_in_small(),
        'english_leter_in': df_cls.english_leter_in(),
        'namesakes': df_cls.namesakes()
    }

    exc_writer('base', sheets)
