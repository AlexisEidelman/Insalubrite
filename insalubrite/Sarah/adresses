#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 15:08:38 2017
Récupère les tables finales envoyées par final_table.py
Sauvegarde les adresses seules

@author: kevin
"""
import pandas as pd
import os

from insalubrite.match_to_ban import merge_df_to_ban
from insalubrite.config_insal import path_output
path_result_adrbad = os.path.join(path_output, 'result_adrbad.csv')
path_result_adrsimple = os.path.join(path_output, 'result_adrsimple.csv')

result_adrbad = pd.read_csv(path_result_adrbad)
adresses_final = result_adrbad[['codeinsee', 'codepostal', 'nomcommune',
               'numero', 'suffixe1', 'nom_typo', 'affaire_id',
               'code_cadastre']]
#adresses_final['suffixe1'].fillna('', inplace=True)
#
#adresses_final['libelle'] = adresses_final['numero'].astype(str) + ' ' + \
#    adresses_final['suffixe1'] + ' ' + \
#    adresses_final['nom_typo'] + ', Paris'
#adresses_final['libelle'] = adresses_final['libelle'].str.replace('  ', ' ')


#pour adresse sarah
adresses_final = merge_df_to_ban(
    adresses_final,
    os.path.join(path_output, 'temp.csv'),
    ['libelle', 'codepostal'],
    name_postcode = 'codepostal'
    )

result_adrsimple = pd.read_csv(path_result_adrsimple)
adresses_simple_final = merge_df_to_ban(
    result_adrsimple,
    os.path.join(path_output, 'temp_simple.csv'),
    ['libelle_adresse', 'numero_adresse1', 'codepostal_adresse'],
    name_postcode = 'codepostal_adresse'
    )


path_csv_adresses = os.path.join(path_output, 'adresses_ban.csv')
path_csv_adresses_simple = os.path.join(path_output, 'adresses_simple_ban.csv')
adresses_final.to_csv(path_csv_adresses, index=False, encoding='utf8')
adresses_simple_final.to_csv(path_csv_adresses_simple, index=False, \
                            encoding='utf8')
