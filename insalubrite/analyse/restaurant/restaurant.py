# -*- coding: utf-8 -*-
"""
Created on Fri May 26 09:03:26 2017

Dans les visites d'insalubrité, on trouve une info qui 
a l'air en lien avec des restaurants, on regarde ce que ça donne 
si on croise cette info avec les resultats des contrôles sanitaires
des restaurants
"""


### étape 1 : repérer les affaires concernées
import os
import pandas as pd

from insalubrite.config_insal import path_bspp, path_output

path_affaires = os.path.join(path_output, 'niveau_adresses.csv')
adresses_sarah = pd.read_csv(path_affaires)

affaire = adresses_sarah[adresses_sarah['titre'] == 'eaux usées et bac à graisse']

#=> il n'y en a que 16 on laisse tomber
