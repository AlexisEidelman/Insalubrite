#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crée la table avec chaque ligne égale à une visite (suite à l'ouverture d'une
affaire) et en colonnes:
    +résultat visite (infos sur l'infraction commise par exemple l'article
      enfreint et infos sur la prescription à suivre),
    +adresse de la visite,
    +signalement
    +puis éventuellement informations venant d'autres sources: BSPP, APUR,
      EAU de PARIS

#TODO: Rajouter BSPP

@author: kevin
"""

import pandas as pd
import os

from insalubrite.Sarah.read import read_table
from insalubrite.config_insal import path_output

path_result_adrbad = os.path.join(path_output, 'result_adrbad.csv')
path_result_adrsimple = os.path.join(path_output, 'result_adrsimple.csv')
# le nom result_adrbad n'est pas très explicite final ?
# TODO: essayer de faire une seule table finale où le fait de passer
# par adrbad ou adrsimple n'apparait pas

#Merge affhygiene and cr_visite into result1
affhygiene = read_table('affhygiene')
cr_visite = read_table('cr_visite')
del affhygiene['parties_cummunes']
del affhygiene['type_bien_concerne']
cr_visite  = cr_visite[['affaire_id', 'observations_visibles']]
##Une affaire donne lieu à une visite
sum(cr_visite.affaire_id.isin(affhygiene.affaire_id))/len(\
                    cr_visite.affaire_id) ## => 100%
#Toute visite est faite dans le cadre d'une affaire
sum(affhygiene.affaire_id.isin(cr_visite.affaire_id))/len(\
                    affhygiene.affaire_id) ## => 91.42%
#91,42% des affaires ont donné lieu à un compte-rendu visite
result1 = cr_visite.merge(affhygiene, on = 'affaire_id')


#Prescription
presc = read_table('prescription')
presc = presc[['id', 'affaire_id', 'libelle']]
presc['libelle_prescription'] = presc['libelle']
del presc['libelle']
histo = read_table('prescriptionhisto')
histo = histo[['prescription_id', 'compterenduvisite_id','arrete_id']]
tout = presc.merge(histo, left_on = 'id', right_on = 'prescription_id',
                   how='outer')
tout.head()

#Infraction
inf = read_table('infraction')
inf = inf[['id', 'articles', 'libelle', 'affaire_id']]
inf['libelle_infraction'] = inf['libelle']
del inf['libelle']
sum(inf.id.isin(tout.prescription_id))/len(inf) #=>92,14%
tout = inf.merge(tout, left_on = 'id', right_on = 'prescription_id',
                   how='outer')
tout.head()

#Affaire hygiene, prescription, infraction
#TODO: affaire_id dans prescription et infraction
sum(result1.affaire_id.isin(tout.affaire_id_x))/len(result1) #=>66,67%
sum(result1.affaire_id.isin(tout.affaire_id_y))/len(result1) #=>66,13%
tout = result1.merge(tout, left_on = 'affaire_id', right_on = 'affaire_id_x',
                   how='outer')
tout.head()
affhygiene = tout
affhygiene['affaire_id'] = affhygiene['affaire_id_x']
del affhygiene['affaire_id_x']

#Lien avec les adresses
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
parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id',\
                                                     'code_cadastre']]
#Merge parcelle_cadastrale with ilot on ilot_id
arrondissement = read_table('arrondissement')
arrondissement = arrondissement[['id', 'codeinsee', 'codepostal',\
                                 'nomcommune']]
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
result_adrbad.to_csv(path_result_adrbad)

# Les 239 qui ne sont pas matché avec adrbad sont marché avec adrsimple
adrsimple = read_table('adrsimple')
result_adrsimple = pd.merge(result2, adrsimple, on='adresse_id')
len(result2) # => 30692
len(adrsimple)  # => 95461
len(result_adrsimple)  # => 239
result_adrsimple.to_csv(path_result_adrsimple)


