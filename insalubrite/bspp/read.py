# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 15:22:25 2017

Charge la base des inspections de bâtiments de la BSPP

Le programme charge la base puis :
    1 - travaille sur le libellé
    2 - travaille sur la date
    3 - travaille sur l'adresse en utilisant l'api de geocodage 
        https://adresse.data.gouv.fr/api-gestion/

"""

import os
import pandas as pd

from insalubrite.config_insal import path_bspp

path_csv_bspp = os.path.join(path_bspp, 'ETALAB.csv')
tab = pd.read_csv(path_csv_bspp)


### Corrige le libellé

#tab.groupby(['Abrege_Motif','Libelle_Motif']).size()
# => on a plusieurs label pour un abrege_motif. C'est des coquilles que l'on
# corrige.

# on devrait corriger dans l'autre sens mais c'est plus simple de retirer
# les accents pour avoir un libellé unique
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace('é','e')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace('É','E')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace('è','e')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace('ç','c')

tab['Hors_mission_bspp'] = tab['Libelle_Motif'].str.contains('\(hors mission')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.split(' \(hors mission').str[0]
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.strip()

tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace(', nettoyage des', ' de')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace(', butane, propane',
    ' (butane ou propane)')
tab['Libelle_Motif'] = tab['Libelle_Motif'].str.replace(' de monoxyde',
    ' monoxyde')

#tab.groupby(['Abrege_Motif','Libelle_Motif']).size()
#tab.groupby('Abrege_Motif')['Libelle_Motif'].unique()
del tab['Abrege_Motif']


### Travail sur la date

# on séléctionne la date et l'heure d'intervention
tab['Heure_intervention'] = tab['Groupe_Horaire_Modification_Statut'].str[11:13]
tab['Date_intervention'] = tab['Groupe_Horaire_Modification_Statut'].str[:10]
tab['Date_intervention'] = pd.to_datetime(tab['Date_intervention'], format="%d/%m/%Y")
del tab['Groupe_Horaire_Modification_Statut']


### Travaille sur l'adresse

adresse = tab['Libelle_Adresse']

pattern_code_postal = r'[0-9]{5}'

# adresse.str.count(pattern_code_postal).value_counts()
## 0 => 1042; 3 => 27, 2 => 15, 5 => 3
pb_0_code_postal = adresse[adresse.str.count(pattern_code_postal) == 0]
#TODO: exploiter ces 1000 cas qui ont une ville la plupart du temps

pd.options.display.max_colwidth = 150
pb_trop_code_postal = adresse[adresse.str.count(pattern_code_postal) > 1]
pb_trop_code_postal
# => on a l'adresse dans une seconde partie de l liste
# c'est toujours des grands établissements => a priori pas insalubre
pd.options.display.max_colwidth = 50

tab_bonne_adresse = tab[adresse.str.count(pattern_code_postal) == 1]
tab = tab_bonne_adresse
adresse = tab['Libelle_Adresse']
tab['code_postal'] = adresse.str.findall(pattern_code_postal).str[0]
tab['voie'] = adresse.str.split(pattern_code_postal).str[0]
tab['voie'] = tab['voie'].str.strip()
tab['ville'] = adresse.str.split(pattern_code_postal).str[1]
tab.loc[tab['ville'].str.contains('ARRONDISS'), 'ville'] = 'PARIS'
tab['ville'] = tab['ville'].str.strip()
tab['ville'].value_counts()


import requests
from io import StringIO

def use_api_adresse(tab):
    '''retourne un DataFrame augmenté via 
    https://adresse.data.gouv.fr/api-gestion'''
    
    path_csv_temp = os.path.join(path_bspp, 'temp.csv')
    tab[['voie','ville', 'code_postal']].to_csv(
        path_csv_temp, index=False, encoding='utf8'
        ) 
    
    data = {
        'postcode': 'code_postal'    
        }

    r = requests.post('http://api-adresse.data.gouv.fr/search/csv/',
                      files = {'data': open(path_csv_temp)},
                      json = data)
    print(r.status_code, r.reason)
    
    return pd.read_csv(StringIO(r.content.decode('UTF-8')))

tab_paris = tab[tab['ville'] == 'PARIS']
tab_paris_adresse = use_api_adresse(tab_paris)
tab_paris_adresse = tab_paris_adresse[['result_label', 'result_score', 'result_id']]
tab_paris_adresse.set_index(tab_paris.index, inplace=True)

tab_paris = tab_paris.join(tab_paris_adresse)

path_csv_paris = os.path.join(path_bspp, 'paris_ban.csv')
tab_paris.to_csv(path_csv_paris)

    
#http --timeout 600 -f POST http://api-adresse.data.gouv.fr/search/csv/ 