# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 17:35:58 2017

Outil pour envoyer à la BAN et trouver l'identifiant
"""

import requests

from io import StringIO
import pandas as pd


def csv_to_ban(path_csv, name_postcode=''):
    ''' On part du principe qu'on a le code postal'''
    data = {}
    if name_postcode:
        data = {
            'postcode': name_postcode
            }
    r = requests.post('http://devapi-adresse.data.gouv.fr/search/csv/',
                      files = {'data': open(path_csv)},
                      json = data)
    print(r.status_code, r.reason)
    return pd.read_csv(StringIO(r.content.decode('UTF-8')))


def merge_df_to_ban(tab, path_csv, var_to_send,
                             name_postcode, encode_utf8=False):
    '''retourne un DataFrame tab augmenté via
    https://adresse.data.gouv.fr/api-gestion'''
    tab[var_to_send].to_csv(
        path_csv, index=False, encoding='utf8'
        )
    tab_ban = csv_to_ban(path_csv, name_postcode)
    tab_ban = tab_ban[['result_label', 'result_score', 'result_id']]
    tab_ban.set_index(tab.index, inplace=True)

    tab.rename(columns = {
        'result_label':'adresse_ban',
        'result_score': 'score_matching_adresse',
        'result_id': 'id_adresse'
        },
        inplace = True)

    return tab.join(tab_ban)


def look_for_unmatched(tab_with_ban):
    ''' #TODO '''
    tab = tab_with_ban
    tab[(tab['result_score'] < 0.7) | (tab['result_score'].isnull())]

