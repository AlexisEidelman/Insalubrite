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
                      sep = '\t', na_values='\\N',
                      parse_dates=True,
                      date_parser=pd.to_datetime)
    for col in tab.columns:
        if all(tab[col].isin(['f','t'])):
            tab[col] = tab[col] == 't'
        # travaille sur les dates"
        if tab[col].dtype == 'O':
            if all(tab[col].str[4] == '-'):
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
                name_table = line.strip().split(' ')[2]
            if 'primary key' in line:
                key = line.strip().split(' ')[2][1:-1]
                if key[-1] == ')':
                    key = key[:-1]
                primary_key[name_table] = key

            # alter
        #        alter table ADRBAD 
        #        add constraint FK72CF84F676DF198E 
        #        foreign key (PARCELLE_ID) 
        #        references PARCELLE_CADASTRALE;
            if 'alter table' in line:
                name_table_alter = line.strip().split(' ')[2]
            if 'foreign key' in line:
                key_alter = line.strip().split(' ')[2][1:-1]
            if 'references' in line:
                reference_alter = line.strip().split(' ')[1][:-1]
                foreign_key += [(name_table_alter,key_alter, reference_alter)]
                print(name_table_alter,key_alter, reference_alter)


    return primary_key, foreign_key   
    
    

if __name__ == '__main__':
    bbb = read_sql()
#    tab = read_table('affaire')
#    print(tab.head())
#    print(tab.dtypes)
