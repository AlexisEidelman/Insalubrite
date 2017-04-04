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

cr_visite = read_table('cr_visite')
cr_visite  = cr_visite[['affaire_id', 'date']].drop_duplicates()
len(set(cr_visite.affaire_id)) #34122 affaires différentes avec visites

#Infraction ou pas
infraction = read_table('infraction')
infraction = infraction[['affaire_id', 'articles']]
len(set(infraction.affaire_id)) #19090 affaires ont relevé des infractions
sum(infraction.affaire_id.isin(cr_visite.affaire_id))/len(infraction) #=>97,23%
#97,23% des affaires ayant induits chacune plusieurs visites qui chacune ont 
#mis en exergue un type d'infraction. Trouver les articles enfreints 
#dans l'attribut 'articles'

#Visites uite auxquelles on n'a pas relevé d'infraction
l = set(cr_visite.affaire_id) - set(infraction.affaire_id)
aff_without_infraction = cr_visite[cr_visite.affaire_id.isin(l)]

#On fait une jointure externe pour conserver les affaires sans infraction
affaires = cr_visite.merge(infraction, left_on = 'affaire_id',
                            right_on = 'affaire_id', how='outer')
#Ca marche:
aff_without_infraction.affaire_id
affaires[affaires.affaire_id == 22777]
#Par exemple affaire_id = 22777 qui est dans aff_without_infraction apparaît 
#dans affaires avec l'attribut articles valant NaN

path_affaires = os.path.join(path_output, 'affaires.csv')
affaires.to_csv(path_affaires)