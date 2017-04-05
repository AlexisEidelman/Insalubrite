#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  4 13:53:35 2017

@author: kevin
"""

import pandas as pd
import os

from insalubrite.Sarah.read import read_table
from insalubrite.config_insal import path_output

cr_visite_brut = read_table('cr_visite')
cr_visite  = cr_visite_brut[['affaire_id', 'date']].drop_duplicates()
len(cr_visite) ##=>49548 visites
cr_visite.affaire_id.value_counts()
cr_visite.affaire_id.nunique() #34122 affaires distinctes
                              #avec n>=1 visites chacune
cr_visite.date.nunique() ##=>4073 dates
# TODO: corriger pour pd.to_datetime(cr_visite.date) tourne


# à une même date on peut avoir plusieurs visites
# Par exemple le 2010-04-14 on a 84 visites
cr_visite[cr_visite.date == '2010-04-14 00:00:00'].affaire_id.value_counts()

# à une date donnée une affaire entraîne une seule visite
assert all(cr_visite.groupby(['affaire_id', 'date']).size() == 1)

#####################
#####Infraction ####
####################

#infractiontype_id et titre
infraction_brut = read_table('infraction')
infraction_brut.infractiontype_id.value_counts()
infraction_brut[infraction_brut.infractiontype_id == 30].titre.value_counts()
infraction_brut[infraction_brut.infractiontype_id == 29].titre.value_counts()
##un infractiontype_id correspond en général à plusieurs titres
infraction_brut.titre.value_counts()
#Top 3: 'humidité de condensation', 'Reprise', 'infiltrations EU'
infraction_brut[infraction_brut.titre == \
'infiltrations EU'].infractiontype_id.value_counts()
##un titre correspond à plusieurs infractiontype_id

#On va garder l'attribut titre plutôt que articles car il est plus explicite
infraction = infraction_brut[['affaire_id', 'id', 'titre']]
len(set(infraction.affaire_id)) #19090 affaires ont relevé des infractions
sum(infraction.affaire_id.isin(cr_visite.affaire_id))/len(infraction) #=>97,23%
#97,23% des visites qui chacune ont mis en exergue un type d'infraction.
#Trouver les articles enfreints dans l'attribut 'articles'

#Visites suite auxquelles on n'a pas relevé d'infraction
l = set(cr_visite.affaire_id) - set(infraction.affaire_id)
aff_without_infraction = cr_visite[cr_visite.affaire_id.isin(l)]


####################
#Infraction histo##
##################

infractionhisto = read_table('infractionhisto')
infractionhisto.infraction_id.isin(infraction.id).all() #True
infraction.id.isin(infractionhisto.infraction_id).all() #False
#TODO: Pourquoi les id de infraction ne sont_ils pas tous dans infractionhisto?
infractionhisto.titre.isin(infraction.titre).all() #False
infractionhisto.articles.value_counts(dropna = False)
#La plupart des affaires dans infractionhisto ont des valeurs articles = NaN
#Cela correspond-il a des affaires sans infractions relevées?
infractionhisto[pd.isnull(infractionhisto.articles)].head()
infractionhisto[pd.isnull(\
infractionhisto.articles)].compterenduvisite_id.value_counts()
cr_visite[cr_visite.affaire_id==43110]
read_table('infraction')[infraction.affaire_id == 43110].articles
#l'affaire 43110 qui a entraîné deux visites, 2 articles enfreints...
infraction['infraction_id'] = infraction['id']
del infraction['id']
#Peut-on utiliser compterenduvisite_id?
sum(infractionhisto.compterenduvisite_id.isin(\
cr_visite.affaire_id))/len(infractionhisto) ##61.3%
#Je ne vois pas comment utiliser infractionhisto

####################################
###Merge cr_visite et infraction###
###################################

#On fait une jointure externe pour conserver les affaires sans infraction
# TODO: pourquoi par 'left' ?
affaires = cr_visite.merge(infraction, on = ['affaire_id'],
                          how='outer')
#Ca marche:
aff_without_infraction.affaire_id.isin(affaires.affaire_id).all()
#On garde bien toutes les visites: infraction ou non

len(affaires) ##=>70 000
#affaire a beaucoup plus de lignes que de nombre de visite : pourquoi ?
cr_visite[cr_visite.affaire_id == 18828]
#Une affaire avec 5 visites
len(affaires[affaires.affaire_id == 18828]) ##=>30
#aboutit à 30 entrées dans la table affaire
#C'est normal car pour une visite caractérisée par un affaire_id et une date
#on devra multiplier par le nombre d'infractions relevées: ici 6
# TODO: non, faire le merge mieux
affaires.loc[(affaires.affaire_id == 18828) &\
             (affaires.date =='2012-09-10 00:00:00')]

path_affaires = os.path.join(path_output, 'affaires.csv')
affaires.to_csv(path_affaires)