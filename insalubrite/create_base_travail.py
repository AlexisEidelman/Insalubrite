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
from insalubrite.Sarah.adresse_de_l_affaire import add_adresse_id
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import path_bspp, path_output


def create_sarah_table():
    hyg = read_table('affhygiene')

    # on garde les affaires ayant déjà eu au moins une visite
    cr = read_table('cr_visite')
    hyg = hyg[hyg.affaire_id.isin(cr.affaire_id)]

    
    infraction = read_table('infraction')[['affaire_id', 'infractiontype_id']]
    # on ne garde qu'une infraction maximum par affaire
    # en conservant la "plus grave"
    infraction['infractiontype_id'].replace([30, 31, 32], [-30, -31, -32], inplace=True)
    infraction = infraction.groupby('affaire_id')['infractiontype_id'].max().reset_index()
    infraction['infractiontype_id'].replace([-30, -31, -32], [30, 31, 32], inplace=True)

    infractiontype = read_table('infractiontype')
    infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
    infractiontype.rename(columns={'id': 'infractiontype_id',
                                   'libelle': 'type_infraction'}, inplace=True)

    infraction = infraction.merge(infractiontype, on='infractiontype_id', how='left')


    affaire = hyg.merge(infraction, on = 'affaire_id', how="left")


    affaire_with_adresse = add_adresse_id(affaire)

    adresse = adresses()[['adresse_id', 'typeadresse', 
        'libelle', 'codepostal', 'codeinsee', 'code_cadastre']]
    sarah = affaire_with_adresse.merge(adresse, on = 'adresse_id',
                                       how = 'left')
    sarah_adresse = sarah[sarah.adresse_id.notnull()]

    sarah_adresse = merge_df_to_ban(sarah_adresse,
                             os.path.join(path_output, 'temp.csv'),
                             ['libelle', 'codepostal'],
                             name_postcode = 'codepostal')

    sarah = sarah_adresse.append(sarah[sarah.adresse_id.isnull()])
    return sarah



########################################
###      Parcelle   et demandeurs    ###
########################################

# on rejoint les deux car parcelle et demandeurs sont au niveau parcelle
# cadastrale


def infos_parcelles():
    from insalubrite.Apur.parcelles import read_parcelle
    parcelle = read_parcelle(2015)
    parcelle.rename(columns={'C_CAINSEE': 'codeinsee'}, inplace=True)
    # TODO: attention, N_SQ_PC le numéro séquentiel est
    # déterminé par année avec réaffectation des numéros d'une année
    # sur l'autre. C'est très piège, on retire tout de suite.
    parcelle.drop(['N_SQ_PC', 'N_SQ_PD', 'OBJECTID', 'B_GRAPH'], axis=1,
                  inplace=True)

    # C_PDNIV0, C_PDNIV1 et C_PDNIV2, correspondent au libellé
    #                  L_PDNIV0, L_PDNIV1 et L_PDNIV2
    # ces éléments sont imbriqués, les niveaux deux correspondent à un groupe
    #                  de niveau 1 qui correspond à un groupe de niveau 0
    parcelle.drop(['C_PDNIV0', 'C_PDNIV1', 'C_PDNIV2'], axis=1,
                  inplace=True)

    path_dem = os.path.join(path_output, 'demandeurs.csv')
    demandeurs = pd.read_csv(path_dem)
    # on ne garde qu'une valeur par ASP, celle la plus récente
    demandeurs = demandeurs[~demandeurs['ASP'].duplicated(keep='first')]

    # hotel meublé
    path_hm = os.path.join(path_output, 'hm.csv')
    if not os.path.exists(path_hm):
        import insalubrite.Apur.hotel_meuble
    hm = pd.read_csv(path_hm)

    augmentation = hm.merge(demandeurs, on='ASP', how='outer')

    code = augmentation['ASP'] # Arrondissement Section Parcelle
    augmentation['codeinsee'] = code.str[:3].astype(int) + 75100
    assert all(augmentation['codeinsee'].isin(parcelle.codeinsee))
    augmentation['C_SEC'] = code.str[4:6]
    augmentation['N_PC'] = code.str[7:].astype(int)

    parcelle_augmentee = parcelle.merge(augmentation,
                              on=['codeinsee', 'C_SEC', 'N_PC'],
                              how='outer', indicator=True)

    pb = parcelle_augmentee[parcelle_augmentee['_merge'] == 'right_only']

    parcelle_augmentee = parcelle_augmentee[parcelle_augmentee['_merge'] != 'right_only']
    del parcelle_augmentee['_merge']

    parcelle_augmentee['hotel meublé'].fillna(False, inplace=True)
    return parcelle_augmentee



def add_infos_parcelles(table):
    assert 'code_cadastre' in table.columns
    assert table['code_cadastre'].notnull().all()
    parcelle = infos_parcelles()

    #prépare table pour le match
    code = table['code_cadastre']
    assert all(code.str[:5].astype(float) == table['codeinsee'])
    table['C_SEC'] = code.str[6:8]
    table['N_PC'] = code.str[8:].astype(int)

    table_parcelle = table.merge(parcelle, on=['codeinsee', 'C_SEC', 'N_PC'],
                       how='left', indicator=True)
    table_parcelle._merge.value_counts()
    # 68 match raté
    # =>  non matché, est-ce une question de mise à jour ? table_parcelle[table_parcelle._merge == 'left_only']

    table_parcelle.drop(['codeinsee', 'C_SEC', 'N_PC', 'code_cadastre', '_merge'],
               axis=1, inplace=True)
    return table_parcelle



###########################
###         BSPP        ###
###########################

# l'APur a pris en compte deux fois au moins la même année.
# quand il y a un engin

# il y a un effet, à force de chercher, le STH trouve.


def add_bspp(table):
    path_csv_bspp = os.path.join(path_bspp, 'paris_ban.csv')
    if not os.path.exists(path_csv_bspp):
        import insalubrite.bspp.read
    bspp = pd.read_csv(path_csv_bspp)

    ### Fusion des données
    bspp = bspp[bspp.adresse_ban_id.isin(table.adresse_ban_id)]

    # simplification => on ne tient pas compte de la date.
    # on utilise un nombre d'intervention par type
    bspp = pd.crosstab(bspp.adresse_ban_id, bspp.Libelle_Motif)
    bspp_columns =  bspp.columns

    table_bspp = table.merge(bspp,
                       left_on='adresse_ban_id',
                       right_index=True,
                       how='outer',
                       indicator='match_bspp')
    assert all(table_bspp['match_bspp'] != 'right_only')
    del table_bspp['match_bspp']

    table_bspp[bspp_columns] = table_bspp[bspp_columns].fillna(0)
    return table_bspp


###########################
###      eau      ###
###########################

# facture d'eau impayé collective
# 120 jours après émissions.
# attention, des syndic jouent avec les règles.
# ça pourrit l'analyse. Voir comment ça va avec les autres.
# éliminer les faux.

def add_eau(table):
    path_eau = os.path.join(path_output, 'eau.csv')
    if not os.path.exists(path_eau):
        import insalubrite.Apur.eau
    eau = pd.read_csv(path_eau)
    table_eau = table.merge(eau[['adresse_ban_id', 'eau_annee_source']],
                       how='outer',
                       on='adresse_ban_id',
                       indicator='match_eau')
    table_eau['match_eau'].value_counts()

    table_eau['eau_annee_source'].value_counts(dropna=False)
    # on rate des adresses de eau  #TODO: étudier
    pb = table_eau[table_eau['match_eau'] == 'right_only']
    #TODO: quelques nouveaux cas parce que des fusions
    # TODO: récupérer la date pour vérifier qu'on est avant la visite
    table_eau = table_eau[table_eau['match_eau'] != 'right_only']
    del table_eau['match_eau']
    return table_eau

###########################
###      saturnisme     ###
###########################

def add_saturnisme(table):
    # question métier : si le saturnisme est décelé après une première
    # viste d'insalubrité, alors le serpent se mord la queue :
    # on utilise le résultat pour prédire le résultat
    path_sat = os.path.join(path_output, 'sat.csv')
    if not os.path.exists(path_sat):
        import insalubrite.Apur.saturnisme
    sat = pd.read_csv(path_sat)
    sat['Type_saturnisme'] = sat['Type'] # rename moche
    # Tous les cas, sont positifs, on a besoin d'en avoir un par adresse_ban_id
    sat = sat[~sat['adresse_ban_id'].duplicated(keep='last')]

    table_sat = table.merge(sat[['adresse_ban_id', 'sat_annee_source', 'Type_saturnisme']],
                            on='adresse_ban_id',
                            how='outer',
                            indicator='match_sat')

    table_sat['sat_annee_source'].value_counts(dropna=False)
    # on rate des adresses de sat  #TODO: étudier
    # TODO: récupérer la date
    # TODO: récupérer la date pour vérifier qu'on est avant la visite

    table_sat = table_sat[table_sat['match_sat'] != "right_only"]
    del table_sat['match_sat']
    return table_sat



if __name__ == '__main__':
    path_affaires = os.path.join(path_output, 'sarah_adresse.csv')
    if os.path.exists(path_affaires):
        data_sarah = pd.read_csv(path_affaires)
    else:
        data_sarah = create_sarah_table()
        data_sarah.to_csv(path_affaires, encoding='utf8', index=False)

    sarah = data_sarah.copy()
    # on retire les 520 affaires sans parcelle cadastrale sur 46 000
    sarah = sarah[sarah['code_cadastre'] != 'inconnu_car_source_adrsimple'] # TODO: analyser le biais créée
    sarah_augmentee_parcelle = sarah[['affaire_id', 'code_cadastre',
                                      'codeinsee',
                                      'infractiontype_id', 'titre']]
    sarah_augmentee_parcelle = add_infos_parcelles(sarah_augmentee_parcelle)

    path_output_parcelle = os.path.join(path_output, 'niveau_parcelles.csv')
    sarah_augmentee_parcelle.to_csv(path_output_parcelle, index=False,
                                    encoding="utf8")

    # ici : on a finit avec le niveau parcelle
    # on continue avec le niveau logement mais c'est beaucoup moins bien défini
    # à cause de bien_id et de immeuble qui n'ont pas souvent de adresse_id
    # voir si le signalement ne doit pas être pris en compte pour en avoir plus

    sarah_adresse = sarah[sarah['adresse_id'].notnull()]
    sarah_adresse = sarah_adresse[['adresse_ban_id', 'affaire_id',
                                   'infractiontype_id', 'titre',
                                   'code_cadastre']]
    sarah_augmentee_adresses = add_bspp(sarah_adresse)
    sarah_augmentee_adresses = add_eau(sarah_augmentee_adresses)
    sarah_augmentee_adresses = add_saturnisme(sarah_augmentee_adresses)

    path_output_adresse = os.path.join(path_output, 'niveau_adresses.csv')
    sarah_augmentee_adresses.to_csv(path_output_adresse, index=False,
                                    encoding="utf8")


