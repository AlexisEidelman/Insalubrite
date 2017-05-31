#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
   Commence l'analyse des tables générées par data.py
"""


import os
import pandas as pd
from insalubrite.analyse.data import get_data

data = get_data("batiment", libre_est_salubre=True, niveau_de_gravite=False)

# Test pour trouver lesquelles des variables numériques sont pertinentes
# pour décrire l'insalubrité
data_numeric = data._get_numeric_data()
test = data_numeric.groupby('est_insalubre').mean()
test2 = data_numeric.groupby('est_insalubre').std()


##################
###Stats desc ####
##################

#Combien d'affaires
print("{} affaires d'hygiène".format(data.affaire_id.nunique()))

#La répartition des affaires par adresse, par parcelles
print("{} parcelles cadastrales".format(data.code_cadastre.nunique()))
data.groupby(['code_cadastre']).size().sort_values(ascending = True)
print("{} adresses différentes".format(data.adresse_ban_id.nunique()))
data.groupby(['adresse_ban_id']).size().sort_values(ascending = True)
print("{} arrondissements".format(data.codeinsee.nunique()))
data.codeinsee.value_counts()


#Carte par iris

#Les bâtiments avec une affaire sont-ils particuliers?