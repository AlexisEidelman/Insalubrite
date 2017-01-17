# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 14:49:42 2017

@author: sgmap
"""
import os
import pandas as pd

from insalubrite.config_insal import path_sarah, path_sql_sarah


def read_table(name):
    tab = pd.read_csv(os.path.join(path_sarah, name + '.csv'),
                      sep = '\t', na_values='\\N')
    for col in tab.columns:
        if all(tab[col].isin(['f','t'])):
            tab[col] = tab[col] == 't'
        # travaille sur les dates"
        if tab[col].dtype == 'O' and tab[col].str[4].isnull().sum() == 0:
            if all(tab[col].str[4] == '-'):
                if any(~tab[col].str[:2].isin(['19','20'])):
                    assert all(tab[col].str[:2].isin(['19','20','00', '10']))
                    tab.loc[tab[col].str[:2].isin(['00','10']), col] = \
                        '20' + tab.loc[tab[col].str[:2] == '00', col].str[2:]
                tab[col].str[:2]
                tab[col] = pd.to_datetime(tab[col])

    return tab


def read_sql():
    ''' return a dict'''
    primary_key = dict()
    foreign_key = list()

    with open(path_sql_sarah) as sql:
        for line in sql.readlines():
            if line.startswith('--'):
                continue
            if 'create table' in line:
                name_table = line.strip().split(' ')[2].lower()
            if 'primary key' in line:
                key = line.strip().split(' ')[2][1:-1].lower()
                if key[-1] == ')':
                    key = key[:-1]
            if ');' in line:
                primary_key[name_table] = key

            # alter
        #        alter table ADRBAD
        #        add constraint FK72CF84F676DF198E
        #        foreign key (PARCELLE_ID)
        #        references PARCELLE_CADASTRALE;
            if 'alter table' in line:
                name_table_alter = line.strip().split(' ')[2].lower()
            if 'foreign key' in line:
                key_alter = line.strip().split(' ')[2][1:-1].lower()
            if 'references' in line:
                reference_alter = line.strip().split(' ')[1][:-1].lower()
                foreign_key += [(name_table_alter,key_alter, reference_alter)]

    return primary_key, foreign_key



if __name__ == '__main__':
    bbb = read_sql()
#    tab = read_table('affaire')
#    print(tab.head())
#    print(tab.dtypes)
