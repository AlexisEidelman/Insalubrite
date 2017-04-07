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

# étudie le lien entre infractionhisto et infraction
len(infraction_brut)  # 31022
len(infractionhisto)  # 49614
assert infractionhisto.infraction_id.isin(infraction.id).all()
# il y a des infractionhisto qui renvoie à la même infraction
# infractionhisto.infraction_id.value_counts()
# exemple infractionhisto[infractionhisto.infraction_id == 9419]
# => même type infraction, date hstorisation différentes

# on regarde le lien avec le compte_rendu de visite

infraction.id.isin(infractionhisto.infraction_id).all() #False
#TODO: Pourquoi les id de infraction ne sont_ils pas tous dans infractionhisto?
infractionhisto.titre.isin(infraction.titre).all() #False
infractionhisto.articles.value_counts(dropna = False)


#La plupart des affaires dans infractionhisto ont des valeurs articles = NaN
#infractionhisto[infractionhisto.articles.isnull()].head()
#Cela correspond-il a des affaires sans infractions relevées?
# => oui dans l'immense majorité des cas (infractiontype_id == 30)



infractionhisto.compterenduvisite_id.value_counts()
# pourquoi on a plusieurs infratction reliée à une même visite ?
# exemple infractionhisto[infractionhisto.compterenduvisite_id == 37955]
# TODO: voir avec le SI d'où ça peut venir

# => il peut y avoir plusieurs infractions constatée sur une même visite



## infractionhisto pointe vers cr_visite qui pointe vers affaire
## infractionhisto pointe vers infraction qui pointe vers affaire
# TODO: verifier la cohérence


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

#On fait une jointure externe pour conserver les affaires sans infraction
# TODO: pourquoi par 'left' ?
#affaires = cr_visite.merge(infraction, on = ['affaire_id'],
#                          how='outer')
compte_rendu_insalubre = cr_visite.merge(insalubre_first_infraction, 
                                         left_on = 'affaire_id',
                                         right_on = 'compterenduvisite_id',
                                         how = 'left')
##Ca marche:
#aff_without_infraction.affaire_id.isin(compte_rendu_insalubre.affaire_id).all()
##On garde bien toutes les visites: infraction ou non
#
#len(compte_rendu_insalubre) ##=>70 000
##affaire a beaucoup plus de lignes que de nombre de visite : pourquoi ?
#cr_visite[cr_visite.affaire_id == 18828]
##Une affaire avec 5 visites
#len(compte_rendu_insalubre[compte_rendu_insalubre.affaire_id == 18828]) ##=>30
##aboutit à 30 entrées dans la table affaire
##C'est normal car pour une visite caractérisée par un affaire_id et une date
##on devra multiplier par le nombre d'infractions relevées: ici 6
## TODO: non, faire le merge mieux
compte_rendu_insalubre.loc[(compte_rendu_insalubre.affaire_id == 18828) &\
             (compte_rendu_insalubre.date =='2012-09-10 00:00:00')]


#### On part du simple
# si pour une affaire, il y a un CR de visite avec une infration, alors
# on considère qu'il y a un problème.



path_affaires = os.path.join(path_output, 'compterenduinsalubre.csv')
compte_rendu_insalubre.to_csv(path_affaires, encoding='utf8')