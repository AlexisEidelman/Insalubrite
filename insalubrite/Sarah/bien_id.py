# -*- coding: utf-8 -*-
"""
voir adresse_de_l_affaire

Au depart, on passait par le signalement de l'affaire qui était
relié à une adresse.
Problème : beaucoup d'affaires n'ont pas de signalement.


Finalement, la comprhénsion de la variable bien_id directement
reliée à affygiène fait prendre une autre piste plus précise et
plus compléte puisqu'on n'a plus le problème de signalement

(voir question pour confirmation)

"""

import os
import pandas as pd

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresse_de_l_affaire import parcelle

from insalubrite.match_to_ban import merge_df_to_ban


hyg = read_table('affhygiene')
bien_ids = hyg.bien_id

# autre hypothèse bien_id se retrouve dans plusieurs tables !
localhabite = read_table('localhabite')
batiment = read_table('batiment')
immeuble = read_table('immeuble')
parcelle_cadastrale = read_table('parcelle_cadastrale')

#
#tous_les_ids = immeuble.id.append(batiment.id).append(parcelle.id).append(localhabite.id)
#assert all(tous_les_ids.value_counts() == 1)
#assert all(bien_ids.isin(tous_les_ids))
#
hyg['bien_id_provenance'] = ''
for origine_possible in ['immeuble', 'batiment', 'parcelle_cadastrale', 'localhabite']:
    cond = bien_ids.isin(eval(origine_possible).id)
    hyg.loc[cond, 'bien_id_provenance'] = origine_possible
print('origine de bien_id \n',
      hyg['bien_id_provenance'].value_counts(), "\n")

##### Travail sur les info des differentes tables

### Séléction des variables portant de l'information
localhabite_to_keep = bien_ids[hyg['bien_id_provenance'] == 'localhabite']
localhabite = localhabite[localhabite.id.isin(localhabite_to_keep)]
del localhabite['codification'] #inutile et incomplet
# TODO: on pourrait travailler sur l'étage

batiment_to_keep = bien_ids[hyg['bien_id_provenance'] == 'batiment']
batiment_to_keep = batiment_to_keep.append(localhabite.batiment_id)
batiment = batiment[batiment.id.isin(batiment_to_keep)]
# en passant
batiment['designation'] = batiment['designation'].str.lower().str.strip()
batiment['designation'] = batiment['designation'].str.replace('timent', 't')
batiment['designation'] = batiment['designation'].str.replace('bat', 'bât')
batiment['designation'] = batiment['designation'].str.replace('bät', 'bât')
batiment['designation'] = batiment['designation'].str.replace('bât.', 'bât ')
del batiment['digicode']  # inutile

immeuble_to_keep = bien_ids[hyg['bien_id_provenance'] == 'immeuble']
immeuble_to_keep = immeuble_to_keep.append(batiment.immeuble_id)
immeuble = immeuble[immeuble.id.isin(immeuble_to_keep)]
immeuble = immeuble.loc[:, immeuble.notnull().sum() > 0] # retire les colonnes vides
# une étude colonne par colonne
del immeuble['champprocedure'] # tous vrais sauf 10
del immeuble['demoli'] # tous vrais sauf 1
que_des_2 = ['diagplomb', 'diagtermite', 'etudemql', 'grilleanah',
             'rapportpreop', 'risquesaturn', 'signalementprefecturepolice',
             ]
immeuble.drop(que_des_2, axis=1, inplace=True)
del immeuble['tournee_id'] # que 8 valeurs

parcelle_cadastrale = parcelle()

### Travail sur les id et les fusions
print("Un local habité est dans un bâtiment\n",
      "Un bâtiment est dans un immeuble\n",
      "Un immeuble est dans une parcelle\n")

# on va chercher d'abord pour chaque table les info annexes
# parcelle_cadastrale ok grace à parcelle()

# on a pas mal d'adresse principale vide ?!
adresse = read_table('adresse')
immeuble.rename(columns={'adresseprincipale_id': 'adresse_id'}, inplace=True)
immeuble = immeuble.merge(adresse, how='left')
#=> on s'arrête là car toute la gestion des adresses est faire ailleurs
# voir adresse_de_l_affaire

entree = read_table('modentbat')[['id','libelle']]
entree.columns = ['modentbat_id', 'mode_entree_batiment']
batiment = batiment.merge(entree, how='left')
del batiment['modentbat_id']

hautfacade = read_table('hautfacade')[['id','libelle']]
hautfacade.columns = ['hautfacade_id', 'hauteur_facade']
batiment = batiment.merge(hautfacade, how='left')
del batiment['hautfacade_id']

