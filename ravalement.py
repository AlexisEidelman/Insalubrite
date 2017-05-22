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
tables_reliees_a('affravalement')
#('arrete_ravalement', 'affaire_id', 'affravalement'),
#('incitation_ravalement', 'affaire_id', 'affravalement'),
#('pv_ravalement', 'affaire_id', 'affravalement')
est_reliee_a('affravalement')
#('affravalement', 'affaire_id', 'affaire'),
#('affravalement', 'immeuble_id', 'immeuble')


############################
#Problèmes diagnostiqués####
###########################
#Termite, Plomb et saturnisme, Signalement pp, Démolition

########################
#Adresse de l'affaire##
#######################
immeuble = read_table('immeuble')
immeuble.rename(columns = {'id':'immeuble_id',
                           'adresseprincipale_id': 'adresse_id'},
                inplace = True)
affaire = read_table('affaire')
table = affravalement.merge(immeuble,
                            on = 'immeuble_id',
                            how = 'outer',
                            indicator = True,
                            )
tables_reliees_a('immeuble')
#('affravalement', 'immeuble_id', 'immeuble'),
#('batiment', 'immeuble_id', 'immeuble'),
#('certificatnotaire', 'immeuble_id', 'immeuble'),
#('immeuble_affaireperilsecu', 'immeuble_id', 'immeuble'),
#('operation_immeuble', 'immeuble_id', 'immeuble')
est_reliee_a('immeuble')

pv_ravalement = read_table('pv_ravalement')
arrete_ravalement = read_table('arrete_ravalement')
pv_ravalement_facade = read_table('pv_ravalement_facade')
incitation_ravalement = read_table('incitation_ravalement')
arrete_ravalement_facade = read_table('arrete_ravalement_facade')
incitation_ravalement_facade = read_table('incitation_ravalement_facade')
arrete_ravalement_incitation_ravalement = read_table('arrete_ravalement_incitation_ravalement')



tables_reliees_a('pv_ravalement')