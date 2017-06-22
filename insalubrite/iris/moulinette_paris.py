#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 18:01:35 2017

@author: kevin

Utilise les données récupérée via OpenMoulinette pour ne sélectionnée que 
celle qui traite Paris.

"""

import os
import pandas as pd

from insalubrite.config_insal import path_moulinette, path_output

path_moulinette_iris = os.path.join(path_moulinette, 'output.csv')
path_moulinette_paris = os.path.join(path_moulinette, 'iris_paris.csv')

def moulinette_paris():
    if not os.path.exists(path_moulinette_paris):
        iris = pd.read_csv(path_moulinette_iris, sep=';', encoding='utf8')
        print(iris.shape)
        
        iris_paris = iris[iris['DEP'] == 75]
        iris_paris.to_csv(path_moulinette_paris, 
                          sep=';', index=False, encoding='utf-8')
    else:
        iris_paris = pd.read_csv(path_moulinette_paris, sep=';', encoding='utf8')
        
    return iris_paris