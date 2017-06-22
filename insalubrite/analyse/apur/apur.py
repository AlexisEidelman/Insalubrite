# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 16:20:46 2017

@author: User
"""

import os
import pandas as pd
    
from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban

path_result = os.path.dirname(path_apur)
path_2014 = os.path.join(path_result,
                           "VP_STH_Observatoire 2013-2014 actualisé au 11072014.xlsx")
tab14 = pd.read_excel(path_2014, sheetname = "base2013")


path_2015 = os.path.join(path_result,
                           "VP_STH_observatoire 2014-2015 fichier final.xlsx")
tab15 = pd.read_excel(path_2015, sheetname = "bilan général")

tab15.loc[:, 'code_postal'] = (75000 + tab15['Arron.']).astype(str)


tab15.loc[:, 'Adresse'] = \
    tab15['num'].astype(str) + \
    tab15['let'].fillna('').replace('b', ' bis') + \
    ', ' + \
    tab15['typv'] + ' ' + \
    tab15['libv'] + ', Paris'

tab_ban = merge_df_to_ban(
    tab15,
    os.path.join(path_output, 'result_apur_15.csv'),
    ['Adresse', 'code_postal'],
    name_postcode = 'code_postal'
    )
#TODO: regarder les erreurs de match


if __name__ == '__main__':
    from insalubrite.analyse.data import get_data
    sarah = get_data('batiment')
    test = tab_ban.merge(sarah)