# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 13:24:09 2017

"""


#### 
#On regarde si une affaire de la table affaire et ou bien dans
#aff_hygiene ou bien dans aff_ravalement
####

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql

affaire = read_table('affaire')
hyg = read_table('affhygiene')
rava = read_table('affravalement')

# premier test
assert len(affaire) == len(hyg) + len(rava)

# affaire_id est bien dans l'id de affaire
assert all(hyg['affaire_id'].isin(affaire['id']))
assert all(rava['affaire_id'].isin(affaire['id']))

# affaire_id n'est pas à la fois dans hygiène et dans ravalement
assert(all(~hyg['affaire_id'].isin(rava['affaire_id'])))
assert(all(~rava['affaire_id'].isin(hyg['affaire_id'])))

# les affaires sont bien dans affhygiene et dans affravalement
liste_affaire = (rava['affaire_id'].append(hyg['affaire_id'])).tolist()
assert(all(affaire['id'].isin(liste_affaire)))