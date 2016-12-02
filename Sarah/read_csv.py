# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 18:24:13 2016

@author: aeidelman
"""

import os
import pandas as pd

path = 'D:\data\SARAH\data'
path = '/home/sgmap/data/SARAH/data'

path_worked = os.path.join(path[:-5], 'worked')

# l'export depuis PostgreSQL créait des public.nom_de_la_table
for file in os.listdir(path):
    if file[:7] == 'public.':
        os.rename(
            os.path.join(path, file),
            os.path.join(path, file[7:])
            )



def read_table(name):
    path_file = os.path.join(path, name + '.csv')
    tab = pd.read_csv(
        path_file,
        sep=';',
        )
    return tab

tables = dict()
for file in os.listdir(path):
    real_name = file[:-4]
    tables[real_name] = read_table(real_name)


def merge(name1, var1, name2, var2):
    ''' fusionne la table 1 selon var1
        avec la tab2 selon var2 
        puis supprimer tab2 et 
        retire var1 qui ne servait qu'au mergre
        de tab1
    '''
    tab1 = tables[name1]
    tab2 = tables[name2]
    tab1 = tab1.merge(
        tab2,
        left_on=var1,
        right_on=var2,
        suffixes=('','_y'))
    
    if all(tab2.columns == ['id', 'libelle']):
        tab1[var1] = tab1['libelle']
        del tab1['libelle']

    if var2 + '_y' in tab1.columns:
        del tab1[var2 + '_y']
    else:
        del tab1[var2]
    
    del tables[name2]
    tables[name1] = tab1

merge('voieprivee', 'statut',
      'statutvoieprivee', 'id')

merge(
    'voieprivee',
    'id',
    'voieprivee_parcelle',
    'voieprivee_id'
    )

tab1 = tables['voieprivee']


tiers = tables['tiers']
tiersref = tables['tiersreference']
tiers_table = [x for x in tables.keys() if 'tiers' in x]
tiers_complexe = tables['tierscomplexe']
tables['typetiers']
tiers_complexe.merge(tables['typetiers'],
                     right_on='id',
                     left_on='type_tiers_id',
                     how='left')
del tables['typetiers']

for k,v in tables.items():
    print(k, len(v))



xxxx
def utilisateur():
    ''' TODO: use utilisateur_type '''
    # on n'a pas besoin de la table utilisateur
    del tables['utilisateur']
    tab = tables['responsable_affaire_historique']
    tab = tab.merge(
        tables['utilisateur_role'],
        left_on='utilisateur_modificateur',
        right_on='utilisateur_id'
        )
    tab.rename(columns={'role_id':'role_utilisateur'},
               inplace=True)
    del tab['utilisateur_id']
    tab = tab.merge(
        tables['utilisateur_role'],
        left_on='responsable_id',
        right_on='utilisateur_id'
        )
    tab.rename(columns={'role_id':'role_responsable'},
               inplace=True)
    del tab['utilisateur_id']


tables['responsable_affaire_historique'] = tab


def affaire():
    tables['affaire'].head()
    # TODO: affaire_statut : affaire_id == id 
    # puis merge avec statut
    
    # merge avec la table responsable


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
#S14090003    2
#S11080391    2
#S16010011    2
#S16010024    2
#S15120242    2
#S12090207    2
#S15090134    2
#S16010227    2