# -*- coding: utf-8 -*-
"""
Created on Wed Jan 18 12:44:36 2017

@author: User
"""

import os
import shutil

from collections import Counter

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql

primary_key, foreign_key = read_sql()

path_csv_good = os.path.join(os.path.dirname(path_sarah), 'csv_good')

tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

def non_empty_tables():
    tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))
    for table in tables_on_disk:
        path_file = os.path.join(path_sarah, table + '.csv')
        path_good = os.path.join(path_csv_good, table + '.csv')
        tab = read_table(table)
        if len(tab) > 0:
            shutil.copy(path_file, path_good)


## est-ce que 

if __name__ == '__main__':
    non_empty_tables()
    print(len(tables_on_disk))
    print(len(os.listdir(path_csv_good)))