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

path_csv_adresse_bspp = os.path.join(path_bspp, 'adresses.csv')

tab[['voie', 'ville', 'code_postal']].to_csv(path_csv_adresse_bspp,
    index=False)
    
tab[['voie', 'ville', 'code_postal']].iloc[:500].to_csv(path_csv_adresse_bspp,
    index=False, encoding='utf8')

import requests

data = {
    'data': path_csv_adresse_bspp,
    'columns': 'voie',
    'postcode': 'code_postal'    
    }
r = requests.post('http://api-adresse.data.gouv.fr/search/csv/',
                  files = {'data': path_csv_adresse_bspp},
                  data = data, timeout=600)
print(r.status_code, r.reason)


BAN_URL = "http://api-adresse.data.gouv.fr/search"
requests.get(BAN_URL + '?q=' + '12 rue poliveau')
#http --timeout 600 -f POST http://api-adresse.data.gouv.fr/search/csv/ 