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
from insalubrite.Sarah.adresse_de_l_affaire import signalement_des_affaires
from insalubrite.config_insal import path_output


### Idée 1: cr_visites sans histoinfraction

def idee1():
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

def idee2():
    '''mauvaise idée, il me semble '''
    # pourquoi cette idée : pas d'article, le titre et le lib Libre
    # hypothèse, les bâtiments insalubre sont ceux pour lesquels on a
    # une infraction autre que 30 - Libre
    cr = read_table('cr_visite')
    cr = cr[['id', 'affaire_id']]

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


### Idée 3: les affaires une visite mais pas dans infraction
affaire = read_table('affhygiene')
affaire = signalement_des_affaires(affaire, ['date_reception'])
affaire['annee'] = affaire['date_reception'].fillna(0).astype(str).str[:4].astype(int)


infraction = read_table('infraction')

toutes_les_affaires = affaire.merge(infraction,
                                    on='affaire_id',
                                    how='outer',
                                    indicator=True,
                                    )
# toutes_les_affaires.groupby('_merge')['annee'].describe()
# => il y des affaires qui n'ont pas fait de visite mais pas seulement

#both          30164
#left_only     19090
#right_only      858

cr = read_table('cr_visite')
cr = cr[['id', 'affaire_id']]
toutes_les_affaires['avec_visite'] = toutes_les_affaires.affaire_id.isin(
    cr.affaire_id)
affaires_avec_visite = toutes_les_affaires[toutes_les_affaires.affaire_id.isin(
    cr.affaire_id)]

affaire_infraction = toutes_les_affaires[
    toutes_les_affaires['_merge'].isin(['both', 'right_only'])
    ]

infraction_histo = read_table('infractionhisto')
cr = cr.merge(infraction_histo,
              left_on = 'id',
              right_on = 'compterenduvisite_id',
              how = 'outer',
              indicator=True
             )
cr_infactionhisto = cr[cr._merge == 'both']
cr_infactionhisto.affaire_id.nunique()
