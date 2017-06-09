# -*- coding: utf-8 -*-
"""
Created on Fri Jun  9 09:10:51 2017

@author: User
"""
import pandas as pd

from insalubrite.create_base_travail import sarah_data
from data import build_output

data = sarah_data()
test = build_output(data, niveau_de_gravite=True)

def analyse_temporelle(table):
    date = pd.to_datetime(table['date_creation'])
    # Analyse dans le temps
    # =>  on est bien pour 2009
    table.groupby([date.dt.year])['est_insalubre'].count()
    print(table.groupby([date.dt.year])['est_insalubre'].mean().loc[2006:])
    table.groupby([date.dt.month])['est_insalubre'].count()
    table.groupby([date.dt.month])['est_insalubre'].mean()

date = pd.to_datetime(data['date_creation'])
data = data[date.dt.year.isin(range(2009,2017))]


## data 
data['output'].value_counts(normalize=True)