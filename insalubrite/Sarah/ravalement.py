#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement
"""

import pandas as pd


from insalubrite.config_insal import path_sarah, path_output
from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.affhygiene import tables_reliees_a, est_reliee_a

#tables utiles à l'étude
affravalement = read_table('affravalement')
pv_ravalement = read_table('pv_ravalement')
arrete_ravalement = read_table('arrete_ravalement')
pv_ravalement_facade = read_table('pv_ravalement_facade')
incitation_ravalement = read_table('incitation_ravalement')
arrete_ravalement_facade = read_table('arrete_ravalement_facade')
incitation_ravalement_facade = read_table('incitation_ravalement_facade')
arrete_ravalement_incitation_ravalement = read_table('arrete_ravalement_incitation_ravalement')


tables_reliees_a(affravalement)