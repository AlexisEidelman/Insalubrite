# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:15:44 2017

"""
import os
import pandas as pd
import requests
from io import StringIO

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table

### éude des adresses
adresse = read_table('adresse')
# est-ce que les adresses sont ou bien dans adrbad ou bien dans adrsimple ?
# vu la tête de adresse on peut supposer que oui
adrsimple = read_table('adrsimple')


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
adrbad = adrbad[['adresse_id','parcelle_id','voie_id',
                 'numero', 'suffixe1', 'suffixe2', 'suffixe3']]

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
result = pd.merge(result, parcelle_cadastrale, on = 'ilot_id')


result4 = pd.merge(voie, result_adrbad, on='voie_id')
result5 = pd.merge(result, result4, on='parcelle_id')
result_adrbad = result5

# Les 239 qui ne sont pas matché avec adrbad sont marché avec adrsimple
adrsimple = read_table('adrsimple')
result_adrsimple = pd.merge(result2, adrsimple, on='adresse_id')
len(result2) # => 30692
len(adrsimple)  # => 95461
len(result_adrsimple)  # => 239


### sauvegarde les données qui concernent les adressses seules :
adresses_final = result_adrbad[['codeinsee', 'codepostal', 'nomcommune', 
               'numero', 'suffixe1', 'nom_typo', 'affaire_id']]
adresses_final['suffixe1'].fillna('', inplace=True)

adresses_final['libelle'] = adresses_final['numero'].astype(str) + ' ' + \
    adresses_final['suffixe1'] + ' ' + \
    adresses_final['nom_typo'] + ', Paris'
adresses_final['libelle'] = adresses_final['libelle'].str.replace('  ', ' ')  


def use_api_adresse(tab):
    '''retourne un DataFrame augmenté via 
    https://adresse.data.gouv.fr/api-gestion'''
    
    path_csv_temp = os.path.join(path_output, 'temp.csv')
    tab[['libelle', 'codepostal']].to_csv(
        path_csv_temp, index=False
        ) 
    
    data = {
        'postcode': 'codepostal'    
        }

    r = requests.post('http://api-adresse.data.gouv.fr/search/csv/',
                      files = {'data': open(path_csv_temp)},
                      json = data)
    print(r.status_code, r.reason)
    
    return pd.read_csv(StringIO(r.content.decode('UTF-8')))

adresses_final_ban = use_api_adresse(adresses_final)
adresses_final_ban = adresses_final_ban[['result_label', 'result_score', 'result_id']]
adresses_final_ban.set_index(adresses_final.index, inplace=True)
adresses_final = adresses_final.join(adresses_final_ban)


path_csv_adressses = os.path.join(path_output, 'adresses_ban.csv')
adresses_final.to_csv(path_csv_adressses, index=False, encoding='utf8')