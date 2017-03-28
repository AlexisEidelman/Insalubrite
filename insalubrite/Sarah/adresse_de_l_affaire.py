# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:15:44 2017


"""
from insalubrite.Sarah.read import read_table
import pandas as pd

affhygiene = read_table('affhygiene')

lien_signalement_affaire = read_table('signalement_affaire')

sign = read_table('signalement')

sign.adresse_id

### éude des adresses 
adrbad = read_table('adrbad')
del adrbad['id_bad'] #id_bad est 

adrsimple = read_table('adrsimple')

adresse = read_table('adresse')

#TODO: est-ce que les adresses sont ou bien dans adrbad ou bien dans adrsimple ?



# lien vers la parcelle, la voie et l'adresse
voie = read_table('voie')
adrbad.suffixe3.value_counts()

#Merge tables
result1 = pd.merge(affhygiene, lien_signalement_affaire, on='affaire_id')
##len(affhygiene.affaire_id)
##37322
##len(lien_signalement_affaire.affaire_id)
##30871
##len(result1.affaire_id)
##30692
sign = read_table('signalement')
##Rename 'id' column of signalement table
sign['signalement_id'] = sign['id']
del sign['id']

result2 = pd.merge(result1, sign, on='signalement_id')
##len(result1.signalement_id)
##30692
##len(sign.signalement_id)
##36080
##len(result2.signalement_id)
##30692
result = pd.merge(result2, adrsimple, on='adresse_id')
##len(result2.adresse_id)
##30692
##len(adrsimple.adresse_id)
##95461
##len(result.adresse_id)
##239

## 68 features: can we drop some of them?
result  = result[['affaire_id', 'bien_id','designation','libelle_adresse','ville_adresse',
                 'observations','codepostal_adresse']]
                 
path = '/home/kevin/Desktop/data_etalab/Insalubrite/adresse.csv'
result.to_csv(path)

path_to_good_adress = '/home/kevin/Downloads/adresse.geocoded.csv'
result_bis = pd.read_csv(path_to_good_adress)
result_bis = result_bis[['affaire_id', 'bien_id', 'observations','designation','result_label']]                 
result_bis                