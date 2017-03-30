# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 16:08:50 2017

Saturnisme

NOTE: par rapport aux fichiers initiaux, les docs ont été
converti en xlsx (plus simple avec pandas)
et les noms de colonnes simplifiés
"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban


path_sat_2014 = os.path.join(path_apur + '2014',
                             '22 Sat_diag positifs APUR 2013.xlsx')
sat_2014 = pd.read_excel(path_sat_2014)
sat_2014['Adresse'] = \
    sat_2014['Numero voie'].astype(str) + ', ' + \
    sat_2014['Nom\nVoie'] + ', Paris'
sat_2014['sat_annee_source'] = 2014
sat_2014.drop(['Numero voie', 'Nom\nVoie'], axis=1, inplace=True)

path_sat_2015 = os.path.join(path_apur + '2015',
                             '21 diag positifs du 31 déc 2013 au 4 février 2015.xlsx')
sat_2015 = pd.read_excel(path_sat_2015)
sat_2015['Adresse'] = sat_2015['Adresse'] + ', Paris'
sat_2015['sat_annee_source'] = 2015

sat = sat_2014.append(sat_2015)
sat.reset_index(inplace=True)

sat_ban = merge_df_to_ban(
    sat,
    os.path.join(path_output, 'sat_temp.csv'),
    ['Adresse', 'Code postal'],
    name_postcode = 'Code postal'
    )

#TODO: regarder les erreurs de match

path_sat = os.path.join(path_output, 'sat.csv')
sat_ban.to_csv(path_sat, index=False)