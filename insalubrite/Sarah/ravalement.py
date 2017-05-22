#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement
"""

import pandas as pd


from insalubrite.config_insal import path_sarah, path_output
from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.affhygiene import tables_reliees_a, est_reliee_a
from insalubrite.Sarah.adresse_de_l_affaire import add_adresse_id

#############################
##tables utiles à l'étude###
############################

affravalement = read_table('affravalement')
#tables_reliees_a('affravalement')
##('arrete_ravalement', 'affaire_id', 'affravalement'),
##('incitation_ravalement', 'affaire_id', 'affravalement'),
##('pv_ravalement', 'affaire_id', 'affravalement')
#est_reliee_a('affravalement')
##('affravalement', 'affaire_id', 'affaire'),
##('affravalement', 'immeuble_id', 'immeuble')


########################
#Adresse de l'affaire##
#######################
immeuble = read_table('immeuble')
immeuble.rename(columns = {'id':'immeuble_id',
                           'adresseprincipale_id': 'adresse_id'},
                inplace = True)
#immeuble_id est un bien_id qui permettra de relier l'affaire à son adresse 
# via add_adresse_id

#affaire = read_table('affaire')
#table = affravalement.merge(immeuble,
#                            on = 'immeuble_id',
#                            how = 'outer',
#                            indicator = True,
#                            )
#tables_reliees_a('immeuble')
##('affravalement', 'immeuble_id', 'immeuble'),
##('batiment', 'immeuble_id', 'immeuble'),
##('certificatnotaire', 'immeuble_id', 'immeuble'),
##('immeuble_affaireperilsecu', 'immeuble_id', 'immeuble'),
##('operation_immeuble', 'immeuble_id', 'immeuble')
#est_reliee_a('immeuble')

#######################
#Problèmes constatés##
######################

##### PV Ravalement ##########
pv_ravalement = read_table('pv_ravalement')
pv_ravalement.rename(columns = {'id':'pv_ravalement_id',
                                'date_envoi':'date_envoi_pv',
                                'date_creation':'date_creation_pv'},
                inplace = True)
#pb_de_date = ['11-04-29 00:00:00']
#pv_ravalement.query('date_envoi >= "11-04-29 00:00:00"')
pv_ravalement['date_envoi_pv'] = pv_ravalement['date_envoi_pv'].str[:10]
pv_ravalement['date_creation_pv'] = pv_ravalement['date_creation_pv'].astype(str).str[:10]
pv_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours','numero'],
                   axis = 1,
                   inplace = True)
# un procès verbal a été envoyé à date_envoi

table = affravalement.merge(pv_ravalement,
                            on = 'affaire_id',
                            how= 'left',
                            )

#tables_reliees_a('pv_ravalement')

pv_ravalement_facade = read_table('pv_ravalement_facade')
pv_ravalement_facade.rename(columns = {'facadesconcernees_id':'facade_id'},
                inplace = True)
table = table.merge(pv_ravalement_facade,
                    on = 'pv_ravalement_id',
                    how = 'left',
                    #indicator = True,
                    )

# relie à la façade concernée
#est_reliee_a('pv_ravalement_facade')
facade = read_table('facade')
facade.rename(columns = {'id':'facade_id'},
                inplace = True)
##len(facade) =>105839

table = table.merge(facade,
                    on = 'facade_id',
                    how = 'left',
                    indicator = True,
                    )
#table._merge.value_counts(dropna=False)
#variables d'intérêt: adresse_id, batiment_id, typefacade

###### Incitation ravalement ######
incitation_ravalement = read_table('incitation_ravalement')
#pour la même affaire plusieurs incitations au ravalement possibles
incitation_ravalement.rename(columns = {'id':'incitation_ravalement_id'},
                inplace = True)
incitation_ravalement_facade = read_table('incitation_ravalement_facade')

arrete_ravalement_incitation_ravalement = read_table('arrete_ravalement_incitation_ravalement')

#### Arrêté Ravalement ############
arrete_ravalement = read_table('arrete_ravalement')
arrete_ravalement.rename(columns = {'id':'arrete_ravalement_id'},
                inplace = True)
arrete_ravalement_facade = read_table('arrete_ravalement_facade')

# on a donné un arreté
# variables d'intérêt: délai
#TODO: à quoi cela correspond-il? délai grand => gravité?




