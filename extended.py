import pandas as pd
from collections import Counter
from base import exc_write
from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class DF_processor:
    """Main class with all content
    """
    pers: pd.DataFrame
    cont: pd.DataFrame

    @staticmethod
    def to_datetime(df_con: pd.DataFrame) -> None:
        """Convert rows to datetime

        Args:
            df_con (pd.DataFrame): df to convertation
        """
        df_con.From = pd.to_datetime(df_con.From, dayfirst=True)
        df_con.To = pd.to_datetime(df_con.To, dayfirst=True)

    def __post_init__(self):
        DF_processor.to_datetime(self.cont)

    def get_cont_amount(self, min_coun: int = 5) -> pd.DataFrame:
        """People contacts amout

        Args:
            min_coun (int, optional): Contact is considered, if it happened >min_coun. Defaults to 5.

        Returns:
            pd.DataFrame: DataFrame with people and the contacts they made
        """
        counter: Counter = Counter()
        for i, row in self.cont.iterrows():
            delta_seconds = (row.To - row.From).total_seconds()
            if delta_seconds >= min_coun*60:
                counter.update([row.Member1_ID, row.Member2_ID])

        contacts_am = pd.DataFrame(counter.items(),
                                   columns=['ID', 'Contacts'])

        contacts: pd.DataFrame = pd.merge(self.pers, contacts_am,
                                          on=['ID'])
        contacts.sort_values('Contacts', ascending=False, inplace=True)

        self.contacts_df: pd.DataFrame = contacts
        return contacts

    def get_cont_duration(self) -> pd.DataFrame:
        """DF of people sorted in reverse order by total duration
contact with other people

        Returns:
            pd.DataFrame: DF with people
        """
        dur: Dict[pd.Timestamp, pd.Timedelta] = {}

        def isin_dict(elem: List[pd.Timestamp], value: pd.Timedelta) -> None:
            """if elem in dur: dur[e]+= value, else: dur[e] = value

            Args:
                elem (List[pd.Timestamp]): elements, which search in dur and doing magic with them ))
                value (pd.Timedelta): value to assignment in dur[e]
            """
            for e in elem:
                if e in dur:
                    dur[e] += value
                else:
                    dur[e] = value

        for i, row in self.cont.iterrows():
            delt = row.To - row.From
            isin_dict([row.Member1_ID, row.Member2_ID], delt)

        dur_f = pd.DataFrame(dur.items(), columns=['ID', 'Contact duration'])
        df_dur = pd.merge(self.pers, dur_f, on=['ID'])

        df_dur.sort_values('Contact duration', inplace=True, ascending=False)
        df_dur['Contact duration'] = df_dur['Contact duration'].astype(str)

        return df_dur

    def get_age_groups(self, step: int = 10) -> pd.DataFrame:
        """Age groups of people with age difference = step , and amount of contact they made

        Returns:
            pd.DataFrame
        """
        def dict_compress() -> Dict[range, int]:
            dic: Dict[range, int] = {}
            for i in range(0, 141, step):
                # самый долгоживущий человек - 146 лет
                dic.update({range(i, i+step): 0})
            return dic

        def search(dic: Dict[range, int]) -> Optional[range]:
            """search in dic.keys() range, in which it is"""
            for el in dic.keys():
                if row["Age"] in el:
                    return el
            return None

        def rang_field(x: range) -> List[str]:
            """Return range start and end"""
            return [str(x.start), str(x.stop - 1)]

        age_gr = dict_compress()
        for i, row in self.contacts_df.iterrows():
            age_gr[search(age_gr)] += row.Contacts

        age_gr = {('-'.join(rang_field(key))):
                  age_gr[key] for key in age_gr.keys()}

        df_age = pd.DataFrame(age_gr.items(),
                              columns=['Age group', 'Contacts amount'])
        df_age.sort_values('Contacts amount', inplace=True, ascending=False)

        return df_age


if __name__ == "__main__":

    sm_persons = pd.read_json('JSON/small_data_persons.json')
    sm_contacts = pd.read_json('JSON/small_data_contracts.json')

    big_persons = pd.read_json('JSON/big_data_persons.json')
    big_contacts = pd.read_json('JSON/big_data_contracts.json')

    df_cls_sm = DF_processor(sm_persons, sm_contacts)
    df_cls_big = DF_processor(big_persons, big_contacts)

    sheets_sm = {
        'contacts_amount': df_cls_sm.get_cont_amount(),
        'contact_duration': df_cls_sm.get_cont_duration(),
        'age_groups': df_cls_sm.get_age_groups()
    }

    sheets_big = {
        'contacts_amount': df_cls_big.get_cont_amount(),
        'contact_duration': df_cls_big.get_cont_duration(),
        'age_groups': df_cls_big.get_age_groups()
    }

    exc_write('extended_sm', sheets_sm)
    exc_write('extended_big', sheets_big)
