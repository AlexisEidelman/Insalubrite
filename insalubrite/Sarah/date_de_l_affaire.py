# -*- coding: utf-8 -*-
"""
 Quelle date de l'affaire conserver?

"""
import os
import pandas as pd

from insalubrite.Sarah.read import read_table


hyg = read_table('affhygiene')
cr = read_table('cr_visite')
hyg = hyg[hyg.affaire_id.isin(cr.affaire_id)]
cr.date.value_counts(dropna=False)
len(cr)
cr.date_creation.value_counts(dropna=False)
# date création est le seul champ de date complètement rempli
assert cr.date_creation.notnull().sum() == len(cr)
#convert to datetime objects
cr['date_creation'] = cr['date_creation'].dt.strftime(date_format = '%d-%m-%Y')
cr['date_creation'] = pd.to_datetime(cr['date_creation'])

#cr.loc[cr.date_diag_plomb.notnull(),'date_diag_plomb'] = \
#pd.to_datetime(cr.loc[cr.date_diag_plomb.notnull(),'date_diag_plomb'].str[:10])

# quelles dates garder?
cr.date_diag_plomb.value_counts(dropna=False)
cr.date_signalement_pref_police.value_counts(dropna=False)
cr.date_signalement_saturn.value_counts(dropna=False)
# on ne gardera pas ces dates car beaucoup de valeurs non renseignées
cr.groupby(['date_signalement_saturn','date_diag_plomb']).size().sort_values(ascending = False)
# date signalement saturnisme et date diagnostic plomb sont pareilles dans la 
# plupart des cas
(cr.date_signalement_saturn == cr.date_diag_plomb).sum()/cr['date_signalement_saturn'].notnull().sum()
cr.groupby(['date_creation','date']).size().sort_values(ascending = False)
cr.groupby(['date','date_creation']).size().sort_values(ascending = False)