# -*- coding: utf-8 -*-
"""
Created on Wed Mar 29 10:08:58 2017

Ce programme crée la base de travail. Il se sert du travail
interne au dossier SARAH avec la base des adresses et le 
résultat des inspections

La base doit contenir :
    A : les données sur le resulat de la visite (issu de SARAH)
    B : les données sur le bâtiments (son identifiant au moins)
    C : 



Ensuite, il va chercher les éléments des autres sources 
pour enrichir la base. Pour l'instant, on utilise:

    - BSPP

"""


import os
import pandas as pd

from insalubrite.config_insal import path_bspp, path_output

path_sarah = os.path.join(path_output, 'adresses_ban.csv')
if not os.path.exists(path_sarah):
    import sarah.adresses_de_l_affaire
adresses_sarah = pd.read_csv(path_sarah)


### charge la base bspp ###

path_bspp = os.path.join(path_bspp, 'paris_ban.csv')
if not os.path.exists(path_bspp):
    import bspp.read
bspp = pd.read_csv(path_bspp)


### Fusion des données 
test = adresses_sarah.merge(bspp, on='result_id', how='outer', indicator=True)
test._merge.value_counts()

