# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 11:24:43 2017

@author: User
"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban


path_pp = os.path.join(path_apur + '2015', '40 PP_APUR 250215.xls')
pp = pd.read_excel(path_pp, sheetname = 1)
del pp['Numéro de dossier']

pp['arr'] = pp['Adresse complete2'].str.split('\(').str[1].str[:-1]
assert all(pp['arr'] == pp['Arrondissement'])
pp['libelle'] = pp['Adresse complete2'].str.split('\(').str[0]

pp['codepostal'] = pp['Arrondissement'].str.split(' ').str[0]
pp['codepostal'] = pp['codepostal'].astype(int) + 75000

    
pp_ban = merge_df_to_ban(
    pp,
    os.path.join(path_output, 'pp_temp.csv'),
    ['libelle', 'codepostal'],
    name_postcode = 'codepostal'
    )
#TODO: regarder les erreurs de match
    
pp_ban['dossier prefecture'] = pp_ban['Type de dossier']
pp_ban.drop(['Adresse complete2', 'Arrondissement',
             'Type de dossier', 'arr'], axis=1, inplace=True)

path_pp = os.path.join(path_output, 'pp.csv')
pp_ban.to_csv(path_pp, index=False, encoding='utf8')
