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

path_affaires = os.path.join(path_output, 'niveau_adresses.csv')
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
    # 'adresse_ban_id',
    'adresse_ban_score', # on ne garde que l'adresse en clair
    'adresse_id', 'typeadresse',
    #'affaire_id', # On garde affaire_id pour des matchs évenuels plus tard (c'est l'index en fait)
    'articles', 'type_infraction', #'infractiontype_id'  on garde par simplicité mais on devrait garder que 'titre',
    'bien_id', 'bien_id_provenance', # interne à Sarah
    'codeinsee_x', 'codeinsee_y',# recoupe codepostal
    'libelle', # = adresse_ban

    ],
    axis=1, inplace=True, errors='ignore')



# Plusieurs niveau de séléction
# on ne garde que quand le match ban est bon
tab = tab[tab['adresse_ban_id'].notnull()]
#del tab['adresse_ban_id']
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

tab = build_output(tab, name_output='est_insalubre')


# faire les trois niveaux de table
niveau_parcelles = tab.groupby('code_cadastre').sum()
# TODO: ce n'est pas bon parce qu'il peut y avoir plusieurs affaire dans une
# parcelle, on veut sommer le deman


# logistique:
import math
import sklearn.linear_model as lin
logistic = lin.LogisticRegression()

X = tab[[col for col in tab.columns
        if col not in ['adresse_ban_id', 'affaire_id',
                       'titre', 'code_cadastre',
                       'date_creation',
                       'L_PD',
                       'realisation_saturnisme',
                       "codeinsee",
                       'L_PDNIV2', 'L_PDNIV1']]]

# on supprime 188 lignes qui sont pas matché
X = X[X.M2_SHAB.notnull()]
X = X[X['codeinsee'].notnull()] #TODO: pourquoi on a ça ?
Y = X['est_insalubre']
del X['est_insalubre']
from sklearn.preprocessing import LabelEncoder
le = LabelEncoder()

list_encoded = list()
for name, col in X.select_dtypes(['object']).iteritems():
    print(name)
    list_encoded.append(name)
    X['l_' + name] = le.fit_transform(col)
X.drop(list_encoded, axis=1, inplace=True)

logistic.fit(X.iloc[:10000,:], Y.iloc[:10000])

counter=0
for col in X:
    print(col, math.exp(logistic.coef_[0][counter]))
    counter +=1