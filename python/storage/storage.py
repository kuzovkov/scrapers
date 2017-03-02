#coding=utf-8

import time
from pprint import pprint
import os


class Storage:

    dbase = None #объект crud базы данных

    def __init__(self, dbase):
        self.dbase = dbase

    def _quote(self, s):
        return ''.join(["'", s, "'"])

    '''
    запись в таблицу данных
    @param unique_keys
        {
          "url": 1,
          "title": 2,
          "description": 5,
        }
    @param data список словарей {field1:value1, ...}
    '''
    def save(self, unique_keys, data):
        old_data = self.dbase.get('data')
        data_to_write = []
        for row in data:
            if len(old_data) > 0:
                record_exists = False
                for old_row in old_data:
                    key_match = True
                    for key, index in unique_keys.items():
                        if row[key] != old_row[index]:
                            key_match = False
                            break
                    if key_match:
                        record_exists = True
                if record_exists:
                    continue
            data_to_write.append(row.values())
        res = self.dbase.insert('data', data_to_write)
        return res

    '''
    Удаление данных из таблицы
    @param table имя таблицы
    '''
    def delete(self, table=None):
        if table is None:
            table = 'data'
        self.dbase.delete(table)


