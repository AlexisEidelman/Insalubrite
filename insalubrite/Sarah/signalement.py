# -*- coding: utf-8 -*-
"""
Relie l'affaire à l'adresse

Au depart, on passait par le signalement de l'affaire qui était
relié à une adresse.
Problème : beaucoup d'affaires n'ont pas de signalement.


Finalement, la comprhénsion de la variable bien_id directement
reliée à affygiène fait prendre une autre piste plus précise et
plus compléte puisqu'on n'a plus le problème de signalement

"""

import os
import pandas as pd

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table

from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.Sarah.affaires_insalubres import affaires_insalubres
from insalubrite.match_to_ban import merge_df_to_ban


def adresses_via_signalement(table,
                             liste_var_signalement=None,
                             prevent_duplicates=True):
    '''
    Trouve l'adresse d'une affaire en utilisant le signalement
    La fonction utilise une table contenant une variable affaire_id
    et
    retrounes la table avec l'adresse_id correspondant à chaque affaire
    repérée par affaire_id
    en éliminant les affaire_id qui ne sont pas dans signalement_affaire
    '''
    assert 'affaire_id' in table.columns


    # signalements
    signalement_affaire = read_table('signalement_affaire')

    signalement = read_table('signalement')
    var_to_keep = ['id', 'adresse_id']
    if liste_var_signalement is not None:
        var_to_keep += liste_var_signalement

    signalement = signalement[var_to_keep]
    ##Rename 'id' column of signalement table
    signalement.rename(columns = {'id':'signalement_id'}, inplace = True)
    table_signalement = pd.merge(signalement_affaire, signalement,
                                 on='signalement_id',
                                 how='left')

    # est-ce que pour une affaire on a une seule adresse
    table_signalement.groupby(['affaire_id'])['adresse_id'].nunique().value_counts()
    # => non
    # l'étude de ces cas, montre que l'on peut utiliser une seule des adresse_id
    # adresse[adresse.adresse_id.isin(table_signalement.loc[table_signalement.affaire_id == 12795, 'adresse_id'])]
    if prevent_duplicates:    
        table_signalement.drop_duplicates(['affaire_id'], inplace=True)


    assert table_signalement.affaire_id.value_counts().max() == 1
    if 'signalement_id' in table.columns:
        table.rename(columns = {'signalement_id':'signalement_id_orig'},
                                inplace = True)
    if 'libelle' in table_signalement.columns:
        table_signalement.rename(columns = {'libelle':'libelle_table'}, inplace = True)

    table_avec_signalement = pd.merge(table, table_signalement,
                                         on='affaire_id',
                                         how='left',
                                         indicator='merge_signalement')
    #    len(table.affaire_id) # => 37322
    #    len(lien_signalement_affaire.affaire_id) ## => 30871
    #    len(result1.affaire_id) ## => 30692
    return table_avec_signalement


if __name__ == '__main__':
    ### éude des adresses
    #adresse = read_table('adresse')
    # les adresses sont ou bien dans adrbad ou bien dans adrsimple

    # Merge tables
    compterenduinsalubre = affaires_insalubres()
    adresses_affaires = adresses_via_signalement(compterenduinsalubre)
    adresses_final = merge_df_to_ban(adresses_affaires,
                                     os.path.join(path_output, 'temp.csv'),
                                     ['libelle', 'codepostal'],
                                     name_postcode = 'codepostal')