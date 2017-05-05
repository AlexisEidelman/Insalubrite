# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 17:35:58 2017

Outil pour envoyer à la BAN et trouver l'identifiant
"""

import requests

from io import StringIO
import numpy as np
import pandas as pd

def csv_to_ban(path_csv, name_postcode=''):
    ''' On part du principe qu'on a le code postal'''
    print("appel à l'api de  adresse.data.gouv.fr, cette opération peut prendre du temps")
    
    data = {}
    if name_postcode:
        data = {
            'encoding': 'utf-8',
            'delimiter': ',',
            'postcode': name_postcode
            }
    r = requests.post('http://api-adresse.data.gouv.fr/search/csv/',
                      files = {'data': open(path_csv, encoding='utf8')},
                      data = data)
    print(r.status_code, r.reason)
    return pd.read_csv(StringIO(r.content.decode('UTF-8')))


def send_one_table(tab, path_csv, name_postcode):
    tab.to_csv(
        path_csv, index=False, encoding='utf8'
        )
    tab_ban = csv_to_ban(path_csv, name_postcode)
    tab_ban = tab_ban[['result_label', 'result_score', 'result_id', 'result_type']]
    tab_ban.set_index(tab.index, inplace=True)   
    tab_ban.rename(columns = {
        'result_label':'adresse_ban',
        'result_score': 'adresse_ban_score',
        'result_type': 'adresse_ban_type',
        'result_id': 'adresse_ban_id',
        },
        inplace = True)
    return tab.join(tab_ban)



def merge_df_to_ban(tab, path_csv, var_to_send,
                             name_postcode):
    '''retourne un DataFrame tab augmenté via
    https://adresse.data.gouv.fr/api-gestion'''
    tab[name_postcode] = tab[name_postcode].astype(int)
    tab_to_ban = tab[var_to_send]
    
    tab_ban = pd.DataFrame()
    select = np.arange(len(tab))//10000
    if select.max() > 1:
        print("On sépare l'appel à l'API en", select.max() ,'parties')
        for k, part_tab in tab_to_ban.groupby(select):
            part_tab_ban = send_one_table(part_tab, path_csv, name_postcode)
            tab_ban = tab_ban.append(part_tab_ban)
    else:
        tab_ban = send_one_table(tab_to_ban, path_csv, name_postcode)

    assert len(tab_ban) == len(tab)
    return tab.merge(tab_ban)


def look_for_unmatched(tab_with_ban):
    ''' #TODO '''
    tab = tab_with_ban
    tab[(tab['result_score'] < 0.7) | (tab['result_score'].isnull())]

