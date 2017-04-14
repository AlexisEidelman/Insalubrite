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

from insalubrite.match_to_ban import merge_df_to_ban


hyg = read_table('affhygiene')
bien_ids = hyg.bien_id

# autre hypothèse bien_id se retrouve dans plusieurs tables !
localhabite = read_table('localhabite')
batiment = read_table('batiment')
immeuble = read_table('immeuble')
parcelle = read_table('parcelle_cadastrale')

#
#tous_les_ids = immeuble.id.append(batiment.id).append(parcelle.id).append(localhabite.id)
#assert all(tous_les_ids.value_counts() == 1)
#assert all(bien_ids.isin(tous_les_ids))
#
#hyg['bien_id_provenance'] = ''
#for origine_possible in ['immeuble', 'batiment', 'parcelle', 'localhabite']:
#    cond = bien_ids.isin(eval(origine_possible).id)
#    hyg.loc[cond, 'bien_id_provenance'] = origine_possible


##### Travail sur les info des differentes tables

### Séléction des variables portant de l'information

localhabite = localhabite[localhabite.id.isin(bien_ids)]
del localhabite['codification'] #inutile et incomplet
# TODO: on pourrait travailler sur l'étage

batiment = batiment[batiment.id.isin(bien_ids)]
# en passant
batiment['designation'] = batiment['designation'].str.lower().str.strip()
batiment['designation'] = batiment['designation'].str.replace('timent', 't')
batiment['designation'] = batiment['designation'].str.replace('bat', 'bât')
batiment['designation'] = batiment['designation'].str.replace('bät', 'bât')
batiment['designation'] = batiment['designation'].str.replace('bât.', 'bât ')
del batiment['digicode']  # inutile

immeuble = immeuble[immeuble.id.isin(bien_ids)]
immeuble = immeuble.loc[:, immeuble.notnull().sum() > 0] # retire les colonnes vides
# une étude colonne par colonne
del immeuble['champprocedure'] # tous vrais sauf 10
del immeuble['demoli'] # tous vrais sauf 1
que_des_2 = ['diagplomb', 'diagtermite', 'etudemql', 'grilleanah',
             'rapportpreop', 'risquesaturn', 'signalementprefecturepolice',
             ]
immeuble.drop(que_des_2, axis=1, inplace=True)
del immeuble['tournee_id'] # que 8 valeurs


### Travail sur les id et les fusions
print("Un local habité est dans un bâtiment \n",
      "Un bâtiment est dans un immeuble \n",
      "Un immeuble est dans une parcelle \n")