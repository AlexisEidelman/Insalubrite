#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 13:53:35 2017

@author: kevin
"""
import pandas as pd
import os

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresse_de_l_affaire import adresse_par_affaires
from insalubrite.config_insal import path_output

cr_visite_brut = read_table('cr_visite')

## Vaine tentative de drop_duplicates
#    all_but_id = [x for x in cr_visite_brut.columns
#        if x not in ['id', 'date_creation']
#        ]
#    cr_visite  = cr_visite_brut.drop_duplicates(subset=all_but_id) #à ne pas faire
#     parce que l'on perd des id et que l'on rate un merge plus loin

cr_visite  = cr_visite_brut[['id', 'affaire_id', 'date']]
len(cr_visite) ##=>49548 visites
cr_visite.affaire_id.value_counts()
cr_visite.affaire_id.nunique() #34122 affaires distinctes
                               # avec n>=1 visites chacune


###  on a des problème de date
pb_de_date = ['1000-10-16 00:00:00',
              '0029-06-29 00:00:00',
              '1010-04-06 00:00:00']

# on regarde si date et date_creation correspondent
# et c'est pareil dans la moitié des cas, très proche dans les autres
# => on peut se dire qu'en cas de problème on prend date_creation à la
# place de date
#    date = cr_visite_brut['date'].str[:10]
#    date_creation = cr_visite_brut['date_creation'].astype(str).str[:10]
#    (date == date_creation).value_counts()

cr_visite.loc[cr_visite['date'].isin(pb_de_date), 'date'] = \
    cr_visite_brut.loc[cr_visite['date'].isin(pb_de_date), 'date_creation']
cr_visite.loc[cr_visite['date'].isnull(), 'date'] = \
    cr_visite_brut.loc[cr_visite['date'].isnull(), 'date_creation']

cr_visite['date'] = pd.to_datetime(cr_visite['date'])


# une affaire entraîne peut avoir plusieurs visites dans la journée
#assert not all(cr_visite.groupby(['affaire_id', 'date']).size() == 1)
#cr_visite_brut[cr_visite.affaire_id == 14665]


#####################
#####Infraction ####
####################

#infractiontype_id et titre
infraction_brut = read_table('infraction')
infraction_brut.groupby(['infractiontype_id','titre']).apply(len)
#On va garder infractiontype
infraction = infraction_brut[['affaire_id', 'id', 'infractiontype_id']]
len(set(infraction.affaire_id)) #19090 affaires ont relevé des infractions
sum(infraction.affaire_id.isin(cr_visite.affaire_id))/len(infraction) #=>97,23%
#97,23% des visites qui chacune ont mis en exergue un type d'infraction.
#Trouver les articles enfreints dans l'attribut 'articles'

##Visites suite auxquelles on n'a pas relevé d'infraction
#l = set(cr_visite.affaire_id) - set(infraction.affaire_id)
#aff_without_infraction = cr_visite[cr_visite.affaire_id.isin(l)]


####################
#Infraction histo##
##################

infractionhisto = read_table('infractionhisto')

# étudie le lien entre infractionhisto et infraction
len(infraction_brut)  # 31022
len(infractionhisto)  # 49614
assert infractionhisto.infraction_id.isin(infraction.id).all()
# il y a des infractionhisto qui renvoie à la même infraction
# infractionhisto.infraction_id.value_counts()
# exemple infractionhisto[infractionhisto.infraction_id == 9419]
# => même type infraction, date hstorisation différentes

# on regarde le lien avec le compte_rendu de visite

#La plupart des affaires dans infractionhisto ont des valeurs articles = NaN
#infractionhisto[infractionhisto.articles.isnull()].head()
#Cela correspond-il a des affaires sans infractions relevées?
# => oui dans l'immense majorité des cas (infractiontype_id == 30)



infractionhisto.compterenduvisite_id.value_counts()
# pourquoi on a plusieurs infratction reliée à une même visite ?
# exemple infractionhisto[infractionhisto.compterenduvisite_id == 37955]
# TODO: voir avec le SI d'où ça peut venir

# => il peut y avoir plusieurs infractions constatée sur une même visite



# on retire les Reprise qui sont dures à exploiter
# TODO: vérifier le sens de ces reprises et la pertinence de les enlever
reprise = (infractionhisto['libelle'] == 'Reprise') & \
    (infractionhisto['titre'] == 'Reprise')
infractionhisto = infractionhisto[~reprise]

# il y a un problème de cohérence entre l'infraction type et articles, titre et
#libelle
# TODO: mettre les reprises (les premières lignes) de côtés
infractiontype = read_table('infractiontype')
infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
infractiontype.rename(columns={'id': 'infractiontype_id'}, inplace=True)

infractionhisto_avant_merge = infractionhisto[['id', 'date_historisation',
                                               'compterenduvisite_id',
                                               'infraction_id',
                                               'infractiontype_id']]

# pour tester :
# infractionhisto.merge(infractiontype, on='infractiontype_id')
# test = infractionhisto.merge(infractiontype, on='infractiontype_id')

infractionhisto = infractionhisto_avant_merge.merge(infractiontype,
                                                    on='infractiontype_id',
                                                    how='left')

# hypothèse, les bâtiments insalubre sont ceux pour lesquels on a
# une infraction autre que 30 - Libre
insalubre = infractionhisto[infractionhisto.infractiontype_id != 30]

#Plusieurs infractions par visite
insalubre.groupby(['compterenduvisite_id']).size()
insalubre.groupby(['compterenduvisite_id','infraction_id']).size()
#Choix: on choisit de garder une seule infraction par visite: la première
insalubre_first_infraction = insalubre.groupby(['compterenduvisite_id']).first()
insalubre_first_infraction.reset_index(inplace = True)
insalubre_first_infraction.groupby(['compterenduvisite_id']).size()
#insalubre_first_infraction.groupby(['compterenduvisite_id','infraction_id']).size()

#
#cr_visite[cr_visite.affaire_id==43110]
#infraction_brut[infraction.affaire_id == 43110].articles
##l'affaire 43110 qui a entraîné deux visites, 2 articles enfreints...
#infraction['infraction_id'] = infraction['id']
#del infraction['id']
##Peut-on utiliser compterenduvisite_id?
#sum(infractionhisto.compterenduvisite_id.isin(\
#cr_visite.affaire_id))/len(infractionhisto) ##61.3%
##Je ne vois pas comment utiliser infractionhisto

####################################
###Merge cr_visite et infraction###
###################################


del insalubre_first_infraction['id']
compte_rendu_insalubre = cr_visite.merge(insalubre_first_infraction,
                                         left_on = 'id',
                                         right_on = 'compterenduvisite_id',
                                         how = 'right')

compte_rendu_insalubre.loc[(compte_rendu_insalubre.affaire_id == 18828) &\
             (compte_rendu_insalubre.date =='2012-09-10 00:00:00')]

compte_rendu_insalubre.drop(['libelle', 'articles'], axis=1, inplace=1)

path_affaires = os.path.join(path_output, 'compterenduinsalubre_v0.csv')
compte_rendu_insalubre.to_csv(path_affaires, encoding='utf8', index=False)

path_affaires = os.path.join(path_output, 'cr_avec_adresse_v0.csv')
affaires = adresse_par_affaires(compte_rendu_insalubre)
affaires.to_csv(path_affaires, encoding='utf8', index=False)