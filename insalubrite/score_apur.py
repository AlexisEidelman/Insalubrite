# -*- coding: utf-8 -*-
"""
Created on Fri Apr 28 10:32:28 2017

@author: User
"""

import os
import pandas as pd

from insalubrite.config_insal import path_bspp, path_output

path_affaires = os.path.join(path_output, 'sarah_adresse.csv')
adresses_sarah = pd.read_csv(path_affaires)

path_parcelles = os.path.join(path_output, 'niveau_parcelles.csv')
parcelles = pd.read_csv(path_parcelles)

path_adresses = os.path.join(path_output, 'niveau_adresses.csv')
adresse = pd.read_csv(path_adresses)



# petits logements locatifs
tab = parcelles[parcelles["NB_LG"].notnull()]

cols_type_logement = ['NB_LG_GRA', 'NB_LG_LOC', 'NB_LG_NIM', 'NB_LG_PRO',
                      'NB_LG_IMP', 'NB_LG_VAC', 'NB_LG_DIV', 'NB_LG_UTL',
                      'NB_LG_LPF']
cols_nb_pieces = ['NB_PIEC_1', 'NB_PIEC_2', 'NB_PIEC_3', 'NB_PIE_4P']
cols_taille_logements = ['NB_LG1_9', 'NB_LG1019', 'NB_LG2029',
                         'NB_LG3039', 'NB_LG4049', 'NB_LG5069',
                         'NB_LG7089', 'NB_LG_S90']


tab[cols_type_logement].sum(axis=1) == tab['NB_LG']
tab[cols_nb_pieces].sum(axis=1) == tab['NB_LG']
assert all(tab[cols_taille_logements].sum(axis=1) == tab['NB_LG'])

pb_nb_pieces = tab[tab[cols_nb_pieces].sum(axis=1) != tab['NB_LG']]
pb_nb_pieces['NB_LG'] - pb_nb_pieces[cols_nb_pieces].sum(axis=1)
(pb_nb_pieces['NB_LG'] - pb_nb_pieces[cols_nb_pieces].sum(axis=1)).value_counts()
# beaucoup de 1

pb_type = tab[tab[cols_type_logement].sum(axis=1) != tab['NB_LG']]
pb_type['NB_LG'] - pb_type[cols_type_logement].sum(axis=1)
(pb_type['NB_LG'] - pb_type[cols_type_logement].sum(axis=1)).value_counts()
# beaucoup de 1 mais aussi des type non remplis

log_1_et_2_pieces = parcelles['NB_PIEC_1'] + parcelles['NB_PIEC_2']
taux_petites_surfaces = 100*log_1_et_2_pieces/tab['NB_LG']
taux_logement_locatif = 100*tab['NB_LG_LOC']/tab['NB_LG']

petits_log_locatif = (taux_petites_surfaces > 70) & (taux_logement_locatif > 70)
#• 3800 sur 30 000


