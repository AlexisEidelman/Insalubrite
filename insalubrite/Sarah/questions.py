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

import pandas as pd

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


####
## Question sur bien_id
####
hyg.bien_id

# on va regarder les id de toutes les tables et
# selectionner celle qui contiennent les valeur de bien_id
import os

primary_key, foreign_key = read_sql()
tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

potentiel_match = []
for name in tables_on_disk:
    tab = read_table(name, nrows=0)
    if 'id' in tab.columns:
        tab = read_table(name)
        id_tab = tab['id']
        if all(hyg.bien_id.isin(id_tab)):
            potentiel_match.append(name)

print(potentiel_match)
# return ['ficherecolem']


####
## infractionhisto pointe vers cr_visite qui pointe vers affaire
## infractionhisto pointe vers infraction qui pointe vers affaire
## On verifie la cohérence
####

infractionhisto = read_table('infractionhisto')

#Dans infractionhisto, la ligne d'indice 47206 correspond à
#infraction_id = 28588 compterenduvisite_id = 46876 
#ils doivent correspondre à la même affaire

cr_visite_brut = read_table('cr_visite')
infraction_brut = read_table('infraction')
cr_visite_brut[cr_visite_brut.id == 46876].affaire_id
infraction_brut[infraction_brut.id == 28588].affaire_id
##=>True
#Est-ce vrai pour toute ligne de infractionhisto?
infractionhisto_avant_merge = infractionhisto[['infraction_id','compterenduvisite_id']]
infractionhisto_avant_merge.drop_duplicates(inplace=True)
infraction_visite = infractionhisto_avant_merge.merge(cr_visite_brut,
                                                      left_on = 'compterenduvisite_id',
                                                      right_on = 'id',
                                                      how = 'left')
infraction_affaire = infractionhisto_avant_merge.merge(infraction_brut,
                                                       left_on = 'infraction_id',
                                                       right_on = 'id',
                                                       how = 'left')
parinfraction = pd.Series(infraction_affaire.affaire_id.unique())
parvisite = pd.Series(infraction_visite.affaire_id.unique())
parinfraction.isin(parvisite).value_counts() ##=> True 16937, False 1
parvisite.isin(parinfraction).value_counts() ##=> True 16937, False 125

## Comprendre les 125 affaires liées à cr_visite mais pas cohérentes avec 
## infraction
cr_visite  = cr_visite_brut[['affaire_id', 'date']].drop_duplicates()
incoherent_aff_id = parvisite[~parvisite.isin(parinfraction)]
incoherent_visits = cr_visite.loc[cr_visite.affaire_id.isin(incoherent_aff_id)]
incoherent_visits.groupby('affaire_id').size()
