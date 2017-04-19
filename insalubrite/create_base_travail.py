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

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.Sarah.bien_id import adresse_via_bien_id
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import path_bspp, path_output


def create_sarah_table():
    hyg = read_table('affhygiene')

    cr = read_table('cr_visite')
    hyg = hyg[hyg.affaire_id.isin(cr.affaire_id)]
    infraction = read_table('infraction')[['affaire_id', 'infractiontype_id']]

    infractiontype = read_table('infractiontype')
    infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
    infractiontype.rename(columns={'id': 'infractiontype_id',
                                   'libelle': 'type_infraction'}, inplace=True)

    infraction = infraction.merge(infractiontype, on='infractiontype_id', how='left')

    affaire = hyg.merge(infraction, on = 'affaire_id', how="left")


    hyg_bien_id = adresse_via_bien_id(affaire)

    adresse = adresses()[['adresse_id', 'typeadresse', 'libelle']]
    sarah = hyg_bien_id.merge(adresse, on = 'adresse_id',
                              how = 'left')
    sarah_adresse = sarah[sarah.adresse_id.notnull()]

    sarah_adresse = merge_df_to_ban(sarah_adresse,
                             os.path.join(path_output, 'temp.csv'),
                             ['libelle', 'codepostal'],
                             name_postcode = 'codepostal')

    sarah = sarah_adresse.append(sarah[sarah.adresse_id.isnull()])
    return sarah


path_affaires = os.path.join(path_output, 'affaires_avec_adresse.csv')
if os.path.exists(path_affaires):
    adresses_sarah = pd.read_csv(path_affaires)
else:
    adresses_sarah = create_sarah_table()
    adresses_sarah.to_csv(path_affaires, encoding='utf8', index=False)

sarah = adresses_sarah.copy()
sarah.rename(columns={'adresse_ban_id': 'result_id'}, inplace=True)
#assert all(sarah.isnull().sum() == 0)

# NOTE:
# on retire les 520 affaires sans parcelle cadastrale sur 46 000
sarah = sarah[sarah['code_cadastre'].notnull()]
# TODO: analyser le biais créée


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

pb = parcelle[parcelle['_merge'] == 'right_only']
del parcelle['_merge']

#prépare adresse_sarah_pour le match
code = sarah['code_cadastre']
assert all(code.str[:5].astype(float) == sarah['codeinsee'])
sarah['C_SEC'] = code.str[6:8]
sarah['N_PC'] = code.str[8:].astype(int)

sarah_parcelle = sarah.merge(parcelle, on=['codeinsee', 'C_SEC', 'N_PC'],
                   how='left', indicator=True)
sarah_parcelle._merge.value_counts()
# 68 match raté

# =>  non matché, est-ce une question de mise à jour ? sarah_parcelle[sarah_parcelle._merge == 'left_only']
sarah_parcelle.drop(['codeinsee', 'C_SEC', 'N_PC', 'code_cadastre', '_merge'],
           axis=1, inplace=True)


# ici : on a finit avec le niveau parcelle
# on continue avec le niveau logement mais c'est beaucoup moins bien défini
# à cause de bien_id et de immeuble qui n'ont pas souvent de adresse_id
# voir si le signalement ne doit pas être pris en compte pour en avoir plus

sarah_adresse = sarah[sarah.adresse_id.notnull()]
sarah = sarah_adresse

###########################
###         BSPP        ###
###########################

path_csv_bspp = os.path.join(path_bspp, 'paris_ban.csv')
if not os.path.exists(path_csv_bspp):
    import insalubrite.bspp.read
bspp = pd.read_csv(path_csv_bspp)

### Fusion des données
bspp = bspp[bspp.result_id.isin(sarah.result_id)]

# simplification => on ne tient pas compte de la date.
# on utilise un nombre d'intervention par type
bspp = pd.crosstab(bspp.result_id, bspp.Libelle_Motif)
bspp_columns =  bspp.columns

sarah_bspp = sarah.merge(bspp,
                   left_on='result_id',
                   right_index=True,
                   how='outer',
                   indicator='match_bspp')
assert all(sarah_bspp['match_bspp'] != 'right_only')
del sarah_bspp['match_bspp']

sarah_bspp[bspp_columns] = sarah_bspp[bspp_columns].fillna(0)
sarah = sarah_bspp

###########################
###      eau      ###
###########################

path_eau = os.path.join(path_output, 'eau.csv')
if not os.path.exists(path_eau):
    import insalubrite.Apur.eau
eau = pd.read_csv(path_eau)
sarah_eau = sarah.merge(eau[['result_id', 'eau_annee_source']],
                   how='outer',
                   on='result_id',
                   indicator='match_eau')
sarah_eau['match_eau'].value_counts()

sarah_eau['eau_annee_source'].value_counts(dropna=False)
# on rate des adresses de eau  #TODO: étudier
pb = sarah_eau[sarah_eau['match_eau'] == 'right_only']
sarah = sarah_eau[sarah_eau['match_eau'] != 'right_only']
#TODO: quelques nouveaux cas parce que des fusions

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
sat['Type_saturnisme'] = sat['Type'] # rename moche
# Tous les cas, sont positifs, on a besoin d'en avoir un par result_id
sat = sat[~sat['result_id'].duplicated(keep='last')]

sarah_sat = sarah.merge(sat[['result_id', 'sat_annee_source', 'Type_saturnisme']],
                        on='result_id',
                        how='outer',
                        indicator='match_sat')

sarah = sarah_sat[sarah_sat['match_sat'] != "right_only"]
sarah['sat_annee_source'].value_counts(dropna=False)
# on rate des adresses de sat  #TODO: étudier
# TODO: récupérer la date
# TODO: récupérer la date pour vérifier qu'on est avant la visite


