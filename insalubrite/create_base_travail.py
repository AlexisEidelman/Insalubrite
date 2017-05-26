# -*- coding: utf-8 -*-
'''
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
'''


import os
import importlib
import pandas as pd
import numpy as np

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.Sarah.adresse_de_l_affaire import add_adresse_id
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import path_bspp, path_output


def _read_or_generate_data(path_csv, module, force=False):
    if not os.path.exists(path_csv) or force:
        print('**** Load :', module)
        importlib.import_module(module)
    return pd.read_csv(path_csv)



def table_affaire():
    """  retourne la table avec date, adresse_id et affaire_id """
    cr = read_table('cr_visite')[['date_creation','affaire_id']]
    assert all(cr.date_creation.notnull())

    # ne garde que la date (pas l'heure)
    cr['date_creation'] = cr['date_creation'].dt.date
    # on garde une seule date par affaire (la premiere)
    cr = cr.groupby('affaire_id')['date_creation'].min().reset_index()

    hyg = read_table('affhygiene')[['affaire_id', 'bien_id']]
    # on garde les affaires ayant déjà eu au moins une visite
    hyg = hyg.merge(cr,
                    on = 'affaire_id',
                    how = 'right')
    return hyg



def create_sarah_table(cols_from_bien_id=None):
    hyg = table_affaire()

    #### ajoute l'infraction
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
    #### fin de l'ajout infraction


    ###ajout adresses correspondantes
    def affaire_avec_adresse(affaire, _cols_from_bien_id=None):
        affaire_with_adresse = add_adresse_id(affaire, _cols_from_bien_id)
        affaire_with_adresse.drop(['adresse_id_sign', 'adresse_id_bien',
                                   'localhabite_id'],
                                  axis=1,
                                  inplace=True
                                  )
        affaire_with_adresse = affaire_with_adresse[
                                    affaire_with_adresse.adresse_id.notnull()]

        adresse = adresses()[['adresse_id', 'typeadresse',
            'libelle', 'codepostal', 'codeinsee', 'code_cadastre']]

        sarah = affaire_with_adresse.merge(adresse, on = 'adresse_id',
                                           how = 'left')

        # on prend le code cadastre de l'adresse_id ou l'ancien (celui de bien_id)
        # quand on n'a pas d'adresse
        sarah['code_cadastre'] = sarah['code_cadastre_y']
        # quelques incohérence (516 sur 34122)
        pb = sarah.loc[sarah['code_cadastre'].notnull(), 'code_cadastre'] != \
            sarah.loc[sarah['code_cadastre'].notnull(), 'code_cadastre_x']

        sarah.loc[sarah['code_cadastre'].isnull(), 'code_cadastre'] = \
            sarah.loc[sarah['code_cadastre'].isnull(), 'code_cadastre_x']
        sarah.drop(['code_cadastre_x', 'code_cadastre_y'], axis=1, inplace=True)


        # match ban
        match_possible = sarah['codepostal'].notnull() & sarah['libelle'].notnull()
        sarah_adresse = sarah[match_possible]
        sarah_adresse = merge_df_to_ban(sarah_adresse,
                                 os.path.join(path_output, 'temp.csv'),
                                 ['libelle', 'codepostal'],
                                 name_postcode = 'codepostal')
        sarah = sarah_adresse.append(sarah[~match_possible])
        return sarah

    sarah = affaire_avec_adresse(affaire, cols_from_bien_id)
    ###fin ajout adresses correspondantes
    return sarah


def sarah_data(force=False, cols_from_bien_id=None):
    path_sarah = os.path.join(path_output, 'sarah_adresse.csv')
    if not os.path.exists(path_sarah) or force:
        print('**** Load : Sarah',)
        sarah_data = create_sarah_table(cols_from_bien_id)
        sarah_data.to_csv(path_sarah, encoding='utf8', index=False)
    else:
        sarah_data = pd.read_csv(path_sarah)
    return sarah_data

########################################
###      Parcelle   et demandeurs    ###
########################################

# on rejoint les deux car parcelle et demandeurs sont au niveau parcelle
# cadastrale


def infos_parcelles():
    from insalubrite.Apur.parcelles import read_parcelle
    parcelle = read_parcelle(2015)

    # demandeur
    path_dem = os.path.join(path_output, 'demandeurs.csv')
    demandeurs = _read_or_generate_data(path_dem, 'insalubrite.Apur.demandeurs')
    # on ne garde qu'une valeur par ASP, celle la plus récente
    demandeurs = demandeurs[~demandeurs['ASP'].duplicated(keep='first')]

    # hotel meublé
    path_hm = os.path.join(path_output, 'hm.csv')
    if not os.path.exists(path_hm):
        import insalubrite.Apur.hotel_meuble
    hm = pd.read_csv(path_hm)

    augmentation = hm.merge(demandeurs, on='ASP', how='outer')

    parcelle_augmentee = parcelle.merge(augmentation,
                              on=['ASP'],
                              how='outer', indicator=True)

    pb = parcelle_augmentee[parcelle_augmentee['_merge'] == 'right_only']
    # 292 erreurs

    parcelle_augmentee = parcelle_augmentee[parcelle_augmentee['_merge'] != 'right_only']
    del parcelle_augmentee['_merge']

    parcelle_augmentee['hotel meublé'].fillna(False, inplace=True)

    # si on n'a pas de demandeurs alros, on a pas fusionné avec parcelles,
    # il faut mettre à O
    return parcelle_augmentee.fillna(0)



def add_infos_parcelles(table):
    assert 'code_cadastre' in table.columns
    assert table['code_cadastre'].notnull().all()

    parcelle = infos_parcelles()

    #prépare table pour le match
    code = table['code_cadastre']
    table.loc[:,'ASP'] = '0' + code.str[3:5] + '-' + code.str[6:8] + '-' + code.str[8:]

    table_parcelle = table.merge(parcelle, on=['ASP'],
                       how='left', indicator=True)
    table_parcelle._merge.value_counts()
    table_parcelle['codeinsee'] = table_parcelle['codeinsee_y']
    table_parcelle.loc[table_parcelle['codeinsee'].isnull(), 'codeinsee'] = \
        table_parcelle.loc[table_parcelle['codeinsee'].isnull(), 'codeinsee_x']
    table_parcelle.drop(['codeinsee_x', 'codeinsee_y'], axis=1, inplace=True)

    # 188 match raté
    # =>  non matché, est-ce une question de mise à jour ? table_parcelle[table_parcelle._merge == 'left_only']

    table_parcelle.drop(['_merge', 'ASP',
                         #, 'code_cadastre',
                         ],
               axis=1, inplace=True)
    return table_parcelle



###########################
###         BSPP        ###
###########################

# l'APur a pris en compte deux fois au moins la même année.
# quand il y a un engin

# il y a un effet, à force de chercher, le STH trouve.

def add_bspp(table, force=False):
    bspp = _read_or_generate_data(
        os.path.join(path_bspp, 'paris_ban.csv'),
        'insalubrite.bspp.read',
        force=force,
        )

    # trouver les intervention par affaire
    merge_bspp = table[['affaire_id','adresse_ban_id','date_creation']].merge(bspp,
                       how='inner',
                       on='adresse_ban_id',
    #                   indicator='match_bspp',
                       )
    merge_bspp['date_creation'] = pd.to_datetime(merge_bspp['date_creation'])
    merge_bspp['Date_intervention'] = pd.to_datetime(merge_bspp['Date_intervention'])
    select_on_date = merge_bspp['Date_intervention'] <  \
            merge_bspp['date_creation']

    merge_bspp = merge_bspp[select_on_date]

    bspp_by_affaire = pd.crosstab(merge_bspp.affaire_id, merge_bspp.Libelle_Motif)

    bspp_by_affaire_columns =  bspp_by_affaire.columns

    table_bspp = table.merge(bspp_by_affaire,
                       left_on='affaire_id',
                       right_index=True,
                       how='left',
    #                   indicator='match_bspp',
                       )


    #Travail sur les valeurs manquantes
    table_bspp[bspp_by_affaire_columns] = table_bspp[bspp_by_affaire_columns].fillna(0)
    return table_bspp


###########################
###      eau      ###
###########################

# facture d'eau impayé collective
# 120 jours après émissions.
# attention, des syndic jouent avec les règles.
# ça pourrit l'analyse. Voir comment ça va avec les autres.
# éliminer les faux.

def add_eau(table, force=False):
    eau = _read_or_generate_data(
        os.path.join(path_output, 'eau.csv'),
        'insalubrite.Apur.eau',
        force=force,
        )
    eau = eau[~eau['adresse_ban_id'].duplicated(keep='last')]

    table_eau = table.merge(eau[['adresse_ban_id', 'eau_annee_source']],
                       how='left',
                       on='adresse_ban_id',
    #                   indicator='match_eau',
                       )
    # on rate des adresses de eau  #TODO: étudier

    #TODO: quelques nouveaux cas parce que des fusions
    table_eau['date_creation'] = pd.to_datetime(table_eau['date_creation'])
    select_on_date = table_eau['eau_annee_source'] <  \
            table_eau['date_creation'].dt.year

    table_eau.loc[~select_on_date,'eau_annee_source'] = np.nan
    table_eau['eau'] = table_eau['eau_annee_source'].notnull()
    del table_eau['eau_annee_source']
    #table_eau.drop('date_creation', axis = 1, inplace = True)
    return table_eau

###########################
###      saturnisme     ###
###########################

def add_saturnisme(table, force=False):
    # question métier : si le saturnisme est décelé après une première
    # viste d'insalubrité, alors le serpent se mord la queue :
    # on utilise le résultat pour prédire le résultat
    sat = _read_or_generate_data(
        os.path.join(path_output, 'sat.csv'),
        'insalubrite.Apur.saturnisme',
        force=force,
        )
    sat.rename(columns = {'Type':'Type_saturnisme',
                          'Date de réalisation':'realisation_saturnisme'},
               inplace = True)

    # Tous les cas, sont positifs, on a besoin d'en avoir un par adresse_ban_id
    sat = sat[~sat['adresse_ban_id'].duplicated(keep='last')]

    table_sat = table.merge(sat[['adresse_ban_id','sat_annee_source',
                                 'realisation_saturnisme','Type_saturnisme']],
                            on='adresse_ban_id',
                            how='left',
    #                        indicator='match_sat',
                            )

    # on rate des adresses de sat  #TODO: étudier

    table_sat['date_creation'] = pd.to_datetime(table_sat['date_creation'])
    table_sat['realisation_saturnisme'] = pd.to_datetime(table_sat['realisation_saturnisme'])
    select_on_date = table_sat['realisation_saturnisme'] <  \
            table_sat['date_creation']

    table_sat.loc[~select_on_date,['sat_annee_source','realisation_saturnisme',
                  'Type_saturnisme']] = np.nan

    table_sat['Type_saturnisme'].fillna('pas de saturnisme connu', inplace=True)
    del table_sat['sat_annee_source']
    return table_sat

############################
### Préfecture de Police ###
###########################

def add_pp(table, force=False):
    pp = _read_or_generate_data(
        os.path.join(path_output, 'pp.csv'),
        'insalubrite.Apur.PP',
        force=force,
        )
    # Tous les cas, sont positifs, on a besoin d'en avoir un par adresse_ban_id
    pp = pp[~pp['adresse_ban_id'].duplicated(keep='last')]

    table_pp = table.merge(pp[['adresse_ban_id', 'dossier prefecture']],
                            on='adresse_ban_id',
                            how='outer',
                            indicator='match_pp')

    table_pp['dossier prefecture'].value_counts(dropna=False)
    table_pp['match_pp']
    # on rate des adresses de pp celle où il n'y a pas eu d'affaire
    # TODO: récupérer la date
    # TODO: récupérer la date pour vérifier qu'on est avant la visite
    table_pp = table_pp[table_pp['match_pp'] != "right_only"]
    del table_pp['match_pp']
    table_pp['dossier prefecture'].fillna('Pas de dossier', inplace=True)
    return table_pp


def add_infos_niveau_adresse(tab, force_all=False,
                             force_bspp=False,
                             force_eau=False,
                             force_saturnisme=False,
                             force_pp=False):
    tab1 = add_bspp(tab, force_all or force_bspp)
    assert len(tab1) == len(tab)
    tab2 = add_eau(tab1, force_all or force_eau)
    assert len(tab2) == len(tab)
    tab3 = add_saturnisme(tab2, force_all or force_saturnisme)
    assert len(tab3) == len(tab)
    tab4 = add_pp(tab3, force_all or force_pp)
    assert len(tab4) == len(tab)
    return tab4


if __name__ == '__main__':
    force_all = False
    
    # colonne_en_plus, c'est les colonnes associée à des adresses
    # que l'on va chercher dans sarah, elles ne sont pas forcément
    # exploitable opérationnellement puisqu'on ne les a peut-être pas
    # avant la visite.
    colonnes_en_plus = ['possedecaves','mode_entree_batiment',
                        'hauteur_facade', 'copropriete']
    var_sarah_to_keep = ['affaire_id', 'code_cadastre', 'date_creation',
                            'codeinsee','infractiontype_id', 'titre']
#    # explications :
#        'adresse_ban_score', # on ne garde que l'adresse en clair
#        #'affaire_id', # On garde affaire_id pour des matchs évenuels plus tard (c'est l'index en fait)
#        'articles', #'infractiontype_id'  on garde par simplicité mais on devrait garder que 'titre',
#        'bien_id', 'bien_id_provenance', # interne à Sarah   

    
    sarah = sarah_data(force_all, cols_from_bien_id=colonnes_en_plus)
    # on retire les 520 affaires sans parcelle cadastrale sur 46 000
    sarah = sarah[sarah['code_cadastre'] != 'inconnu_car_source_adrsimple']
    sarah = sarah[sarah['code_cadastre'].notnull()] # TODO: analyser le biais créée
    
    sarah_parcelle = sarah[var_sarah_to_keep]
    
    sarah_augmentee_parcelle = add_infos_parcelles(sarah_parcelle)

    path_output_parcelle = os.path.join(path_output, 'niveau_parcelles.csv')
    sarah_augmentee_parcelle.to_csv(path_output_parcelle, index=False,
                                    encoding="utf8")

    # ici : on a finit avec le niveau parcelle
    # on continue avec le niveau logement mais c'est beaucoup moins bien défini
    # à cause de bien_id et de immeuble qui n'ont pas souvent de adresse_id
    # voir si le signalement ne doit pas être pris en compte pour en avoir plus

    sarah_adresse = sarah[sarah['adresse_id'].notnull()]
    sarah_adresse = sarah_adresse[var_sarah_to_keep + ['adresse_ban_id'] + 
                                   colonnes_en_plus]

    sarah_final = add_infos_niveau_adresse(sarah_adresse,
                             force_all,
                             force_bspp=False,
                             force_eau=False,
                             force_saturnisme=False,
                             force_pp=False)

    path_output_adresse = os.path.join(path_output, 'niveau_adresses.csv')
    sarah_final.to_csv(path_output_adresse, index=False,
                                    encoding="utf8")

