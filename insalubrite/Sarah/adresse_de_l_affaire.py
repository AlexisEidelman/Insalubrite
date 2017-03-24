# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:15:44 2017


"""
from insalubrite.Sarah.read import read_table

aff = read_table('affaire')

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

