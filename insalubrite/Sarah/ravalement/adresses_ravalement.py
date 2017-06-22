#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement: ADRESSE
"""

import pandas as pd
import os

from insalubrite.Sarah.adresses import adresses
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import  path_output

##############################
###  Adresse de l'affaire  ##
#############################

def affaire_avec_adresse(affaire):
    """
       Elle prend la table <affaire ravalement> contenant adresse_id et la
       relie à l'adresse de la Base d'Adresse Nationale correspondante
    """
    assert 'adresse_id' in affaire.columns
    affaire_with_adresse = affaire[affaire.adresse_id.notnull()]

    adresse = adresses()[['adresse_id', 'typeadresse',
        'libelle', 'codepostal', 'codeinsee', 'code_cadastre']]

    sarah = affaire_with_adresse.merge(adresse, on = 'adresse_id',
                                       how = 'left')

    # match ban
    match_possible = sarah['codepostal'].notnull() & sarah['libelle'].notnull()
    sarah_adresse = sarah[match_possible]
    sarah_adresse = merge_df_to_ban(sarah_adresse,
                             os.path.join(path_output, 'temp.csv'),
                             ['libelle', 'codepostal'],
                             name_postcode = 'codepostal')
    sarah = sarah_adresse.append(sarah[~match_possible])
    sarah = sarah.append(affaire[affaire.adresse_id.isnull()])
    return sarah
