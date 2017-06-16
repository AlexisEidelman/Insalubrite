# -*- coding: utf-8 -*-
"""
Created on Tue Apr 25 14:07:53 2017

@author: User
"""

import pandas as pd
from data import get_data
from preprocess import preprocess_data
from sklearn.decomposition import PCA
from statsmodels.multivariate.pca import PCA as sPCA

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



tab_afc = tab_afc.join(categorielles)

spca = sPCA(tab_en_attendant, max_iter = 10)
spca_standard = sPCA(tab_en_attendant, standardize=True)



#### PCA with scikit-learn

pca = PCA(n_components=4)
pca.fit(tab_afc)
  
out = pca.transform(tab_afc)
pca.explained_variance_ratio_

#### PCA with scikit-learn and normalization
from sklearn.preprocessing import Normalizer
from sklearn.preprocessing import normalize, scale
#from sklearn.pipeline import Pipeline

norm = Normalizer(copy=True, norm='l2')
#tab_norm = norm.fit_transform(tab_en_attendant)
tab_norm = scale(tab_afc)
pca = PCA(n_components=4)
pca.fit(tab_norm)
  
out = pca.transform(tab_norm)
plt.plot(out[:,0], out[:,1], ".")