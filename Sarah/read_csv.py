# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 18:24:13 2016

@author: aeidelman
"""

import os
import pandas as pd

path = 'D:\data\SARAH\data'

# l'export depuis PostgreSQL créait des public.nom_de_la_table
for file in os.listdir(path):
    if file[:7] == 'public.':
        os.rename(
            os.path.join(path, file),
            os.path.join(path, file[7:])
            )

path_signalement = os.path.join(path, 'signalement.csv')
signal = pd.read_csv(path_signalement, sep=';')

import pdb
to_remove = ['prenom', 'mail']

for col in to_remove:
    del signal[col]

for col in signal.columns:
    print(signal[col])
    print(signal[col].value_counts())
    print(col)
    pdb.set_trace()
    

# on garde contact_nom car il y a certaines modalités intéressante

# on ne garde pas le téléphone, que l'indicatif, au cas où
signal['contact_telephone'] = signal['contact_telephone'].str[:2]


# pas grave pour l'analyse mais dommage
# nature me pose probleme 
#le value counts fait :
# 1    35553
# 0      438
# 2       89
#Name: nature, dtype: int64
# pareil pour partie_commune
#

# drop duplicate ? 
S14090003    2
S11080391    2
S16010011    2
S16010024    2
S15120242    2
S12090207    2
S15090134    2
S16010227    2