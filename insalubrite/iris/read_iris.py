#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 19 18:01:35 2017

@author: kevin
"""

import os
import pandas as pd

path = '/home/kevin/Desktop/open-moulinette-master/insee/data'
path_moulinette_iris = os.path.join(path, 'output.csv')
path_moulinette_paris = os.path.join(path, 'iris_paris.csv')

if not os.path.exists(path_moulinette_paris):
    iris = pd.read_csv(path_moulinette_iris, sep=';', encoding='utf8')
    print(iris.shape)
    
    iris_paris = iris[iris['DEP'] == 75]
    iris_paris.to_csv(path_moulinette_paris, 
                      sep=';', index=False, encoding='utf-8')

else:
    iris_paris = pd.read_csv(path_moulinette_paris, sep=';', encoding='utf8')