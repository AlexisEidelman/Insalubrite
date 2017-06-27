# -*- coding: utf-8 -*-
"""
Created on Tue Jun 27 16:41:00 2017

@author: User
"""

import geopandas as gpd
from shapely.geometry import Point
from data import get_data, build_output
import preprocess

#tab_ini = get_data('batiment') 

import os
import pandas as pd
  
from insalubrite.config_insal import path_bspp, path_output

path_parcelles = os.path.join(path_output, 'niveau_parcelles.csv')
parcelles = pd.read_csv(path_parcelles)
assert parcelles['code_cadastre'].isnull().sum() == 0
parcelles = build_output(parcelles, name_output='est_insalubre',
                         libre_est_salubre=True,
                         )


path_adresses = os.path.join(path_output, 'niveau_adresses.csv')
adresse = pd.read_csv(path_adresses)

geo = adresse[adresse.longitude.notnull() & adresse.latitude.notnull()]
assert all(geo[['longitude', 'latitude']].isnull().sum() == 0)
geo['geometry'] = geo.apply(lambda x: Point(x.longitude, x.latitude), axis=1)

geo.head().plot()

# TODO: calculer le ratio d'insalubre par code iris
# puis fuisionner avec iris_paris
# puis colorplether ça.