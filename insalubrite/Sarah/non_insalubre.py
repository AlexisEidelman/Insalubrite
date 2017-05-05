# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 09:54:00 2017

Ce programme étudier les pistes pour déterminer les bâtiments non insalubres

Idée 1: les cr_visites sans histoinfraction
Idée 2: les cr_visites sans histoinfraction
Idée 3: les affaires sans infraction

"""

import pandas as pd

from insalubrite.Sarah.read import read_table
from insalubrite.config_insal import path_output


### Idée 1: cr_visites sans histoinfraction

cr = read_table('cr_visite')
cr = cr[['id', 'affaire_id']]

infraction_histo = read_table('infractionhisto')
infraction_histo = infraction_histo[['date_historisation',
                                               'compterenduvisite_id',
                                               'infraction_id',
                                               'infractiontype_id',
                                               'titre', 'libelle', 'articles',
                                    ]]
                                               
# il y a un problème de cohérence entre l'infraction type et articles, titre et
#libelle
# TODO: mettre les reprises (les premières lignes) de côtés
#infractiontype = read_table('infractiontype')
#infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
#infractiontype.rename(columns={'id': 'infractiontype_id'}, inplace=True)
#
#infraction_histo = infraction_histo.merge(infractiontype,
#                                                    on='infractiontype_id',
#                                                    how='left')

cr = cr.merge(infraction_histo, 
                             left_on = 'id',
                             right_on = 'compterenduvisite_id',
                             how = 'outer',
                             indicator=True
                             )

cr._merge.value_counts()
idee1 = cr[cr['_merge'] == 'left_only']


### Idée 2: infractionhisto.infractiontype_id == 30

# pourquoi cette idée : pas d'article, le titre et le lib Libre
# hypothèse, les bâtiments insalubre sont ceux pour lesquels on a
# une infraction autre que 30 - Libre
idee2 = cr[cr.infractiontype_id == 30]

# ce n'est pas vrai, sur les anciennes affaires.
# si l'idée 2 est la bonne, alors normalement, on n'a pas d'autres infractions
ids_infraction_30 = idee2.id
tous_les_cr_avec_infraction_30 = cr[cr.id.isin(ids_infraction_30)]
test = tous_les_cr_avec_infraction_30
test = test[test['infractiontype_id'] != 30]
test.infractiontype_id.value_counts()
# => il y a d'autres infraction lié à libre. Est-ce que 30 veut dire
# Autres motifs (comme dans champs libre) ?


### Idée 3: classement
classement = read_table('classement')[['commentaire', 'compte_rendu_id']]
# Les commentaires montre que parfois les travaux ont été réalisés.
# => il y a eu besoin de faire des travaux, 
# => le bâtiments était insalubre lors d'une visite précédente

### Idée 4 : affaire avec infraction
affaire = read_table('affhygiene')
# on garde les affaires qui on donné lieu à une visite 
# sinon comment savoir si c'est insalubre
cr = read_table('cr_visite')
affaire = affaire[affaire.affaire_id.isin(cr.affaire_id)]
infraction = read_table('infraction')

test = affaire.merge(infraction, on = 'affaire_id', how="left", indicator=True)
# TODO: demander ce que sont ces infractions sans affaire_id

