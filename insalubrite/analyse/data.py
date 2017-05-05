# -*- coding: utf-8 -*-
"""

Rassemble les données générées dans la création de base
pour avoir des données sur lesquelles produire des statistiques et autres infos.

Pour être clair: on prend ici les données sarah_adresse, niveau_parcelles et
niveau_adresses comme données. On ne les modifie pas.

"""

import os
import pandas as pd

from insalubrite.config_insal import path_bspp, path_output

path_affaires = os.path.join(path_output, 'sarah_adresse.csv')
adresses_sarah = pd.read_csv(path_affaires)

path_parcelles = os.path.join(path_output, 'niveau_parcelles.csv')
parcelles = pd.read_csv(path_parcelles)
assert parcelles['code_cadastre'].isnull().sum() == 0

path_adresses = os.path.join(path_output, 'niveau_adresses.csv')
adresse = pd.read_csv(path_adresses)

### étape 1  
# on rassemble toutes les infos
tab = adresses_sarah.merge(adresse, how='left').merge(parcelles, how='left')
# On a toutes les affaires (avec une visite) y compris les non matchées


# on supprime les variables inutiles pour l'analyse
tab.drop(
    [
    'adresse_ban_id', 'adresse_ban_score', # on ne garde que l'adresse en clair
    'adresse_id', 'typeadresse',
    # 'affaire_id', On garde affaire_id pour des matchs évenuels plus tard (c'est l'index en fait)
    'articles', 'type_infraction', #'infractiontype_id'  on garde par simplicité mais on devrait garder que 'titre', 
    'bien_id', 'bien_id_provenance', # interne à Sarah
    'codeinsee', 'codeinsee_x', 'codeinsee_y',# recoupe codepostal
    'libelle', # = adresse_ban
    '---',
    ],
    axis=1, inplace=True)



# Plusieurs niveau de séléction 
# on ne garde que quand le match ban est bon
tab = tab[tab['adresse_ban_type'] == 'housenumber']
del tab['adresse_ban_type'] 
# =>  72 lignes en moins


def build_output(tab, name_output = 'output', libre_est_insalubre = True,
                niveau_de_gravite = False):

    assert 'infractiontype_id' in tab.columns
    infractiontype_id = tab['infractiontype_id']
    
    output = infractiontype_id.isnull()
    if libre_est_insalubre:
        output = output | (infractiontype_id == 30)

    if niveau_de_gravite:
        output = 1*output
        cond_gravite = infractiontype_id.isin(range(23,29))
        output[cond_gravite] = 2

    # si titre est dans     
    if 'titre' in tab.columns:
        del tab['infractiontype_id']
        
    tab[name_output] = output
    return tab

tab = build_output(parcelles, name_output='est_insalubre')