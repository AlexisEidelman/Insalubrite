# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 16:56:18 2017

Les infos sont au niveau parcelle cadastralle

"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban


path_dem_2014 = os.path.join(path_apur + '2014',
                             '70 demandeurs_2014.xlsx')
dem_2014 = pd.read_excel(path_dem_2014, sheetname='bd')


path_dem_2015 = os.path.join(path_apur + '2015',
                             '70 demandeurs_2015.xlsx')
dem_2015 = pd.read_excel(path_dem_2015, sheetname='don')
dem_2015 = dem_2015.iloc[:-1] #la derniere ligne c'est le total
#

assert all(dem_2015.columns == dem_2014.columns)
dem = dem_2014.append(dem_2015)

explications = pd.read_excel(path_dem_2015, sheetname='t_occ')

traduction = explications.set_index("Code du mode d'occupation")["Libellé du mode d'occupation"].to_dict()
dem.rename(columns=traduction, inplace=True)

# on ajoute les valeurs non précisé au mode : Locataire dans le privé
dem['Locataire dans le privé'] += dem['---']
del dem['---']
dem.fillna(0, inplace=True)

path_dem = os.path.join(path_output, 'demandeurs.csv')

dem.to_csv(path_dem, index=False, encoding='utf8')

if __name__ == "__main__":
    ''' fait des test '''
    dem_brute = dem.drop(['ASP', 'TOT_PROP', 'TOT_LOC', 'TOTAL_DEM'], axis=1)
    dem[['TOT_PROP', 'TOT_LOC', 'TOTAL_DEM']]
    assert all(dem['TOT_PROP'] + dem['TOT_LOC'] == dem['TOTAL_DEM'])
    assert all(dem_brute.sum(axis=1) == dem['TOTAL_DEM'])
    assert all(dem['Propriétaire'] == dem['TOT_PROP'])