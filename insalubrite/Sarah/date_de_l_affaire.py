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

# quelles dates garder
cr.date_diag_plomb.value_counts(dropna=False)
cr.date_signalement_pref_police.value_counts(dropna=False)
cr.date_signalement_saturn.value_counts(dropna=False)
# on ne gardera pas ces dates
cr.groupby(['date_creation','date']).size().sort_values(ascending = False)
cr.groupby(['date','date_creation']).size().sort_values(ascending = False)