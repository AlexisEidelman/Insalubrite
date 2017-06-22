#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement
"""

import pandas as pd
import os

from insalubrite.Sarah.ravalement.adresses_ravalement import affaire_avec_adresse

from insalubrite.config_insal import  path_output


def _ravalement(table, suffixe, output):
    table = affaire_avec_adresse(table)
    table.drop(['typeadresse','adresse_id'],axis=1,inplace = True)
    table.rename(columns = {'libelle':'libelle_'+suffixe+'_ravalement'},
                         inplace = True)
    path_ravalement = os.path.join(path_output, output)
    table.to_csv(path_ravalement, encoding="utf8", index=False)
    return table

    