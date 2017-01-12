# -*- coding: utf-8 -*-
"""
A expertiser
"""

import os
import pandas as pd

year = str(2015)
path = "M:\data\Apur\_prevention_" + year

# parcelle cadastralle
tab_file = os.path.join(path, '00 PARCELLE_CADASTRALE_STAT_' + year + '.xlsx')
tab = pd.read_excel(tab_file)

for col in tab.columns:
    try:
        a = tab[col].astype(float)
    except:
        serie = tab[col]
        if serie.nunique() < 5:
            print(col, '\n')
            print(serie.value_counts())
        else:
            if len(serie) == serie.nunique():
                print('cette variable est un identifiant ', col)


logement = [x for x in tab.columns if 'NB_LG' in x]
piece = [x for x in tab.columns if 'NB_PIEC' in x]
nb_la = [x for x in tab.columns if 'NB_LA' in x]
restant = [x for x in tab.columns if x not in logement + piece + nb_la]

# geoloc des parcelles
tab_file = os.path.join(path, '00 SRU2014 avec geoloc.xls')
geoloc = pd.read_excel(tab_file)

var_communes = [x for x in tab.columns if x in geoloc.columns]

# eau de paris
tab_file = os.path.join(path, '10 Eau-de-Paris_APUR 2014.xlsx')
eau = pd.read_excel(tab_file)

# mise en demeure
tab_file = os.path.join(path, '30 mises en demeure STH 2014.xls')
demeure = pd.read_excel(tab_file)
# parse la localisaion
local = demeure[u'Localisation        ']
# le premier terme c'est la caractéristique
# à la fin, après ":, ", c'est les adresses
type_loc = local.str.split(' :').str[0]
type_loc.value_counts()
type_of_location = type_loc.unique().tolist()

adresses = local.str.split(':,').str[-1]
