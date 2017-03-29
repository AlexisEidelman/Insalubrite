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
len(affhygiene.affaire_id) # => 37322
len(lien_signalement_affaire.affaire_id) ## => 30871
len(result1.affaire_id) ## => 30692
signalement = read_table('signalement')
signalement = signalement[['id', 'observations', 'adresse_id']]
##Rename 'id' column of signalement table
signalement['signalement_id'] = signalement['id']
del signalement['id']

result2 = pd.merge(result1, signalement, on='signalement_id')
len(result1.signalement_id) ## => 30692
len(signalement.signalement_id) ## => 36080
len(result2.signalement_id) ## => 30692

# Link with adresses
adrbad = read_table('adrbad')
#del adrbad['id_bad'] #id_bad est 
adrbad = adrbad[['adresse_id','parcelle_id','voie_id']]
result_adrbad = pd.merge(result2, adrbad, on='adresse_id')
len(result2) # => 30692
len(adrbad)  # => 146306
len(result_adrbad)  # => 30453

voie = read_table('voie')
voie['voie_id'] = voie['id']
del voie['id']
voie = voie[['voie_id','code_ville','libelle','nom_typo','type_voie']]


parcelle_cadastrale = read_table('parcelle_cadastrale')
parcelle_cadastrale['parcelle_id'] = parcelle_cadastrale['id']
del parcelle_cadastrale['id']
parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id','code_cadastre']]
#Merge parcelle_cadastrale with ilot on ilot_id
arrondissement = read_table('arrondissement')
arrondissement = arrondissement[['id', 'codeinsee', 'codepostal', 'nomcommune']]
arrondissement['nsq_ca'] = arrondissement['id']
del arrondissement['id']
quartier_admin = read_table('quartier_admin')
result = pd.merge(arrondissement, quartier_admin, on = 'nsq_ca')
result['nqu'] = result['nsq_qu']
del result['nsq_qu']
ilot = read_table('ilot')
ilot['ilot_id'] = ilot['nsq_ia']
del ilot['nsq_ia']
result = pd.merge(result, ilot, on = 'nqu')
result = pd.merge(result,parcelle_cadastrale, on = 'ilot_id')


result4 = pd.merge(voie, result_adrbad, on='voie_id')
result5 = pd.merge(result, result4, on='parcelle_id')
result_adrbad = result5 

# Les 239 qui ne sont pas matché avec adrbad sont marché avec adrsimple
adrsimple = read_table('adrsimple')
result_adrsimple = pd.merge(result2, adrsimple, on='adresse_id')
len(result2) # => 30692
len(adrsimple)  # => 95461
len(result_adrsimple)  # => 239


## 68 features: can we drop some of them?
#result  = result[['affaire_id', 'bien_id','designation','libelle_adresse','ville_adresse',
                 'observations','codepostal_adresse']]
                 
#path = '/home/kevin/Desktop/data_etalab/Insalubrite/adresse.csv'
#result.to_csv(path)

#path_to_good_adress = '/home/kevin/Downloads/adresse.geocoded.csv'
#result_bis = pd.read_csv(path_to_good_adress)
#result_bis = result_bis[['affaire_id', 'bien_id', 'observations','designation','result_label']]                 
#result_bis                