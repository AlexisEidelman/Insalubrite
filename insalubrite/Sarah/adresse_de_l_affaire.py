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
from insalubrite.match_to_ban import merge_df_to_ban
from insalubrite.Sarah.affaires_insalubres import affaires_insalubres

def adresses_via_signalement(table,
                             liste_var_signalement=None):
    '''
    Trouve l'adresse d'une affaire en utilisant le signalement
    La fonction utilise une table contenant une variable affaire_id
    et
    retrounes la table avec l'adresse_id correspondant à chaque affaire
    repérée par affaire_id
    en éliminant les affaire_id qui ne sont pas dans signalement_affaire
    '''
    assert 'affaire_id' in table.columns

    signalement_affaire = read_table('signalement_affaire')
    if 'signalement_id' in table.columns:
        table.rename(columns = {'signalement_id':'signalement_id_orig'},
                                inplace = True)
    table_signalement_affaire = pd.merge(table, signalement_affaire,
                                         on='affaire_id',
                                         how='left',
                                         indicator='merge_signalement')
    #    len(table.affaire_id) # => 37322
    #    len(lien_signalement_affaire.affaire_id) ## => 30871
    #    len(result1.affaire_id) ## => 30692

    # étape 2 : signalement
    signalement = read_table('signalement')
    var_to_keep = ['id', 'adresse_id']
    if liste_var_signalement is not None:
        var_to_keep += liste_var_signalement

    signalement = signalement[var_to_keep]
    ##Rename 'id' column of signalement table
    signalement.rename(columns = {'id':'signalement_id'}, inplace = True)
    table_signalement = pd.merge(table_signalement_affaire, signalement,
                                 on='signalement_id',
                                 how='left')

    adresse = adresses()

    ## étape 3.4 : fusionne table et adresse
    if 'libelle' in table_signalement.columns:
        table_signalement.rename(columns = {'libelle':'libelle_table'}, inplace = True)
    table_adresses = table_signalement.merge(adresse[['adresse_id', 'libelle',
                                                      'codepostal', 'code_cadastre']],
                                             on='adresse_id',
                                             how = 'left')
    len(table_signalement) # => 38534
    len(adresse)  # => 241767
    len(table_adresses)  # => 38534
    return table_adresses


def adresses_par_affaires(table, liste_var_signalement=None):

    ## étape 3.5 : envoie à l'API
    print("appel à l'api de  adresse.data.gouv.fr, cette opération peut prendre du temps")
    table_ban = merge_df_to_ban(
        table_adresses,
        os.path.join(path_output, 'temp.csv'),
        ['libelle', 'codepostal'],
        name_postcode = 'codepostal'
        )
    return table_ban


if __name__ == '__main__':
    compterenduinsalubre = affaires_insalubres()
    adresses_final = adresse_par_affaires(compterenduinsalubre)
    adresses_affaires = adresses_via_signalement(compterenduinsalubre)
    adresses_final = merge_df_to_ban(adresses_affaires,
                                     os.path.join(path_output, 'temp.csv'),
                                     ['libelle', 'codepostal'],
                                     name_postcode = 'codepostal')

