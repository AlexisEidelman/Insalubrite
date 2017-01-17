# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 14:49:42 2017

@author: sgmap
"""
import os
import pandas as pd

from insalubrite.config_insal import path_sarah


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
    

if __name__ == '__main__':
    tab = read_table('affaire')
    print(tab.head())
    print(tab.dtypes)
