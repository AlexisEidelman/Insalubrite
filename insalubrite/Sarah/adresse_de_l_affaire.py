# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:15:44 2017

TODO: faire le lien entre adrbad, voie, parcelle
TODO: faire la séléction des variable au fur et à mesure

"""
import pandas as pd

from insalubrite.Sarah.read import read_table


### éude des adresses 


adresse = read_table('adresse')
# est-ce que les adresses sont ou bien dans adrbad ou bien dans adrsimple ?
# vu la tête de adresse on peut supposer que oui
adrsimple = read_table('adrsimple')

# lien vers la parcelle, la voie et l'adresse
voie = read_table('voie')


# Merge tables
affhygiene = read_table('affhygiene')
lien_signalement_affaire = read_table('signalement_affaire')
result1 = pd.merge(affhygiene, lien_signalement_affaire, on='affaire_id')
##len(affhygiene.affaire_id)
##37322
##len(lien_signalement_affaire.affaire_id)
##30871
##len(result1.affaire_id)
##30692
signalement = read_table('signalement')
signalement = signalement[['id', 'observations', 'adresse_id']]
##Rename 'id' column of signalement table
signalement['signalement_id'] = signalement['id']
del signalement['id']

result2 = pd.merge(result1, signalement, on='signalement_id')
##len(result1.signalement_id)
##30692
##len(sign.signalement_id)
##36080
##len(result2.signalement_id)
##30692

adrbad = read_table('adrbad')
del adrbad['id_bad'] #id_bad est 
result = pd.merge(result2, adrbad, on='adresse_id')
len(result2) # => 30692
len(adrbad)  # => 146306
len(result)  # => 30453
# Les 239 qui ne sont pas matché avec adrbad sont marché avec adrsimple
#TODO: faire le matching avec adrsimple pour les 239 cas

## 68 features: can we drop some of them?
result  = result[['affaire_id', 'bien_id','designation','libelle_adresse','ville_adresse',
                 'observations','codepostal_adresse']]
                 
path = '/home/kevin/Desktop/data_etalab/Insalubrite/adresse.csv'
result.to_csv(path)

path_to_good_adress = '/home/kevin/Downloads/adresse.geocoded.csv'
result_bis = pd.read_csv(path_to_good_adress)
result_bis = result_bis[['affaire_id', 'bien_id', 'observations','designation','result_label']]                 
result_bis                