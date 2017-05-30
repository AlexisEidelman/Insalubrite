#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" 
   Commence l'analyse des tables générées par data.py
"""


import os
import pandas as pd
from insalubrite.analyse.data import get_data

data = get_data("batiment", libre_est_salubre=True, niveau_de_gravite=False)

def test_pertinence_variables(table):
    variables = table.columns
    result = []
    for variable in variables:
        test_var = table.groupby('output')['variable'].mean()
        result.append(test_var)
        return result

test_pertinence_variables(data)

##################
###Stats desc ####
##################

#Combien d'affaires
data.affaire_id.nunique()

#La répartition des affaires par adresse, par parcelles

#Carte par iris

#Les bâtiments avec une affaire sont-ils particuliers?