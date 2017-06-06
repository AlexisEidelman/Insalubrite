# -*- coding: utf-8 -*-
"""
"""

from data import get_data 
import mca

import matplotlib.pyplot as plt


from sklearn.cluster import KMeans
from sklearn.preprocessing import scale


tab = get_data('batiment')
X = tab[[col for col in tab.columns
        if col not in ['adresse_ban_id', 'affaire_id',
                        'titre', 'code_cadastre',
                       'date_creation',
                       'realisation_saturnisme',
                       "codeinsee",
                       'L_PD',
                       'L_PDNIV2', 'L_PDNIV1']]]

# on supprime 188 lignes qui sont pas matché
Y = X['est_insalubre']
del X['est_insalubre']


tab_quali = X.select_dtypes(['object', 'bool'])
for name, col in tab_quali.select_dtypes(['bool']).iteritems():
    print(name)
    tab_quali.loc[:, name] = 'eau' + tab_quali[name].astype(str)
        

ncols = 8
mca_ben = mca.mca(tab_quali, cols=tab_quali.columns.tolist())
mca_ind = mca.mca(tab_quali, cols=tab_quali.columns.tolist(), benzecri=False)

