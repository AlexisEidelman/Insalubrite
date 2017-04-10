# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 10:08:58 2017

Ce programme crée la base de travail. Il se sert du travail
interne au dossier SARAH avec la base des adresses et le
résultat des inspections

La base doit contenir :
    A : les données sur le resulat de la visite (issu de SARAH)
    B : les données sur le bâtiments (son identifiant au moins)
    C :



Ensuite, il va chercher les éléments des autres sources
pour enrichir la base. Pour l'instant, on utilise:

    - BSPP

"""


import os
import pandas as pd

from insalubrite.config_insal import path_bspp, path_output

path_affaires = os.path.join(path_output, 'compterenduinsalubre_v0.csv')
if not os.path.exists(path_affaires):
    import insalubrite.Sarah.affaires
adresses_sarah = pd.read_csv(path_affaires)
adresses_sarah.to_csv(path_affaires, encoding='utf8', index=False)
sarah = adresses_sarah
sarah.rename(columns={'id_adresse': 'result_id'}, inplace=True)

###########################
###         BSPP        ###
###########################


path_bspp = os.path.join(path_bspp, 'paris_ban.csv')
if not os.path.exists(path_bspp):
    import insalubrite.bspp.read
bspp = pd.read_csv(path_bspp)

### Fusion des données
test = sarah.merge(bspp, on='result_id', how='outer', indicator=True)
test._merge.value_counts()



########################################
###      Parcelle   et demandeurs    ###
########################################

# on rejoint les deux car parcelle et demandeurs sont au niveau parcelle
# cadastrale
from insalubrite.Apur.parcelles import read_parcelle
parcelle = read_parcelle(2015)
parcelle.rename(columns={'C_CAINSEE': 'codeinsee'}, inplace=True)

path_dem = os.path.join(path_output, 'demandeurs.csv')
demandeurs = pd.read_csv(path_dem)
code = demandeurs['ASP']
demandeurs['codeinsee'] = code.str[:3].astype(int) + 75100
assert all(demandeurs['codeinsee'].isin(parcelle.codeinsee))
demandeurs['C_SEC'] = code.str[4:6]
demandeurs['N_PC'] = code.str[7:].astype(int)

parcelle = parcelle.merge(demandeurs,
                          on=['codeinsee', 'C_SEC', 'N_PC'],
                          how='outer', indicator=True)
# right_only      350

#prépare adresse_sarah_pour le match
code = sarah['code_cadastre']
assert all(code.str[:5].astype(int) == sarah['codeinsee'])
sarah['C_SEC'] = code.str[6:8]
sarah['N_PC'] = code.str[8:].astype(int)

sarah = sarah.merge(parcelle, on=['codeinsee', 'C_SEC', 'N_PC'],
                   how='left', indicator=True)
sarah._merge.value_counts()
# => 134 non matché, est-ce une question de mise à jour ? test[test._merge == 'left_only']
sarah.drop(['codeinsee', 'C_SEC', 'N_PC', 'code_cadastre', '_merge'],
           axis=1, inplace=True)

###########################
###      eau      ###
###########################

path_eau = os.path.join(path_output, 'eau.csv')
if not os.path.exists(path_eau):
    import insalubrite.Apur.eau
eau = pd.read_csv(path_eau)
sarah = sarah.merge(eau[['result_id', 'eau_annee_source']],
                   how='left')
sarah['eau_annee_source'].value_counts(dropna=False)
# on rate des adresses de eau  #TODO: étudier
# TODO: récupérer la date pour vérifier qu'on est avant la visite

###########################
###      saturnisme     ###
###########################

# question métier : si le saturnisme est décelé après une première
# viste d'insalubrité, alors le serpent se mord la queue :
# on utilise le résultat pour prédire le résultat
path_sat = os.path.join(path_output, 'sat.csv')
if not os.path.exists(path_sat):
    import insalubrite.Apur.saturnisme
sat = pd.read_csv(path_sat)
sarah = sarah.merge(sat[['result_id', 'sat_annee_source']],
                   how='left')
sarah['sat_annee_source'].value_counts(dropna=False)
# on rate des adresses de sat  #TODO: étudier
# TODO: récupérer la date
# TODO: récupérer la date pour vérifier qu'on est avant la visite


