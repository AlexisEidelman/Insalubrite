# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 15:50:12 2017

"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban


path_eau_2014 = os.path.join(path_apur + '2014', '10 eau APUR 2014.xlsx')
eau_2014 = pd.read_excel(path_eau_2014)
eau_2014.rename(columns={'Libellé type de branchement':
    'Libellé qualité client payeur'}, inplace=True)
eau_2014['eau_annee_source'] = 2014

path_eau_2015 = os.path.join(path_apur + '2015', '10 Eau-de-Paris_APUR 2014.xlsx')
eau_2015 = pd.read_excel(path_eau_2015)
eau_2015['eau_annee_source'] = 2015

eau = eau_2014.append(eau_2015)
eau.reset_index(inplace=True)

# on a que 38 adresses commune
# eau[['Nom rue', 'N° dans la rue', 'Complément du N° dans la rue']].duplicated().sum()

eau['libelle'] = eau['N° dans la rue'].astype(str) + ' ' + \
    eau['Complément du N° dans la rue'].fillna('') + ' ' + \
    eau['Type de voie'] + ' ' + \
    eau['Nom rue'] + ', Paris'

eau['libelle'] = eau['libelle'].str.replace('  ', ' ')

eau['codepostal'] = eau['Nom commune'].str.split('PARIS ').str[1]
eau['codepostal'] = eau['codepostal'].str.split('EME ARRONDISSEMENT').str[0]
eau['codepostal'] = eau['codepostal'].str.split('ER ARRONDISSEMENT').str[0]
eau['codepostal'] = 75000 + eau['codepostal'].astype(int)

eau_ban = merge_df_to_ban(
    eau,
    os.path.join(path_output, 'eau_temp.csv'),
    ['libelle', 'codepostal'],
    name_postcode = 'codepostal'
    )

#TODO: regarder les erreurs de match

path_eau = os.path.join(path_output, 'eau.csv')
eau_ban.to_csv(path_eau, index=False, encoding='utf8')
