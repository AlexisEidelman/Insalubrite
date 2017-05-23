# -*- coding: utf-8 -*-
"""
Created on Tue May 23 14:43:01 2017

@author: User
"""

import os
import pandas as pd
import numpy as np

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.Sarah.adresse_de_l_affaire import add_adresse_id
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import path_bspp, path_output

from insalubrite.create_base_travail import table_affaire


affaire = table_affaire()


affaire_with_adresse = add_adresse_id(affaire)
affaire_with_adresse.drop(['adresse_id_sign', 'adresse_id_bien',
                           'localhabite_id'],
                          axis=1,
                          inplace=True
                          )
affaire_with_adresse = affaire_with_adresse[
                            affaire_with_adresse.adresse_id.notnull()]

adresse = adresses()[['adresse_id', 'typeadresse',
    'libelle', 'codepostal', 'codeinsee', 'code_cadastre']]

sarah = affaire_with_adresse.merge(adresse, on = 'adresse_id',
                                   how = 'left')

# on prend le code cadastre de l'adresse_id ou l'ancien (celui de bien_id)
# quand on n'a pas d'adresse
sarah['code_cadastre'] = sarah['code_cadastre_y']
# quelques incohérence (516 sur 34122)
pb = sarah.loc[sarah['code_cadastre'].notnull(), 'code_cadastre'] != \
    sarah.loc[sarah['code_cadastre'].notnull(), 'code_cadastre_x']

sarah.loc[sarah['code_cadastre'].isnull(), 'code_cadastre'] = \
    sarah.loc[sarah['code_cadastre'].isnull(), 'code_cadastre_x']
sarah.drop(['code_cadastre_x', 'code_cadastre_y'], axis=1, inplace=True)

match_possible = sarah['codepostal'].notnull() & sarah['libelle'].notnull()
sarah_adresse = sarah[match_possible]
sarah_adresse = merge_df_to_ban(sarah_adresse,
                         os.path.join(path_output, 'temp.csv'),
                         ['libelle', 'codepostal'],
                         name_postcode = 'codepostal',
                         var_ban_to_keep=['result_label', 'result_score',
                                          'result_id', 'result_type',
                                          'latitude', 'longitude']
                         )

xxx
# Latitude et longitude sont en WP84
pyproj.transform(
    pyproj.Proj(init='epsg:4326'),
    pyproj.Proj(init='epsg:2154'), #lambert
    48.8662,
    2.34521,
    )


# contours iris lui concerne la GEOLOC.
import os
import fiona
import pyproj
import geopandas as gpd

path = "D:\data\SARAH\iris\CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10"
path = os.path.join(path, 'CONTOURS-IRIS', '1_DONNEES_LIVRAISON_2015',
                    "CONTOURS-IRIS_2-1_SHP_LAMB93_FE-2015"
                    )
path2 = os.path.join(path, 'CONTOURS-IRIS.shp')

#iris = gpd.read_file(path2)
#test = fiona.open(path2)
#iris_paris.to_file(os.path.join(path, 'CONTOURS-IRIS_Paris.shp'))

iris = gpd.read_file(os.path.join(path, 'CONTOURS-IRIS_Paris.shp'))
iris_paris = iris[iris.INSEE_COM.str.startswith('75')]


# On utilise les données GEOFLA
from fiona.crs import from_string, from_epsg
from_epsg(2154)
tes
from shapely.ops import transform




