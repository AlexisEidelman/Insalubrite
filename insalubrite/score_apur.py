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


log_1_et_2_pieces = parcelles['NB_PIEC_1'] + parcelles['NB_PIEC_2']
taux_petites_surfaces = 100*log_1_et_2_pieces/parcelles['NB_LG']
taux_logement_locatif = 100*parcelles['NB_LG_LOC']/parcelles['NB_LG']

petits_log_locatif = (taux_petites_surfaces > 70) & (taux_logement_locatif > 70)

#• 3800 sur 30 000


