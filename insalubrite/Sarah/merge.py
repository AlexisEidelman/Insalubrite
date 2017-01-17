# -*- coding: utf-8 -*-
"""
Fusionne toutes les tables

On récupère une liste des tables pérésentes et on utilise le
sql pour voir si on peut faire le rattachement automatique

"""

import os

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql

primary_key, foreign_key = read_sql()


tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

########################
#  Lien csv et sql
########################

set(primary_key.keys()) - tables_on_disk
#{'datadocumenttemp', 'template', 'datadocument'}
tables_on_disk - set(primary_key.keys())
#{'spatial_ref_sys', 'groleg', 'notification', 'gprocedureg',
# 'geometry_columns', 'gtemplateg', 'alerte'}
#TODO: ce seront des tables à expertiser

tables_good = set(primary_key.keys()) & tables_on_disk
# les tables que l'on doit augmenter
tables_to_merge = set(x[0] for x in foreign_key)
# les tables qui augmentent
tables_to_merge_with = set(x[2] for x in foreign_key)

# tables_to_merge - tables_on_disk
# => {'datadocument', 'datadocumenttemp'} logique
# tables_to_merge_with - tables_on_disk
# => {'procedure', 'role'} récupéré par ailleur

tables_isolees = tables_good - tables_to_merge - tables_to_merge_with
# {'communefrance', 'perioderecolement', 'jbpm_variable', 'parametres_edition'}
#for table in tables_isolees:
#    isolee = read_table(table)
#    print('\n', table)
#    print(isolee)

tables_to_merge - tables_to_merge_with
# on commence par les tables qui sont augmentées mais qui n'augmentent pas
for tab2_name in tables_to_merge - tables_to_merge_with:
    tab2 = read_table(tab2_name)
    var2 = primary_key[tab2_name]
    assert var2 in tab2
    print(tab2_name, len(tab2))
    for link in [x for x in foreign_key if x[2] == tab2_name]:
        tab1_name = link[0]
        var1 = link[1]
        tab1 = read_table(tab1_name)
        assert var1 in tab1.columns
        assert all(tab1.loc[tab1[var1].notnull(), var1].isin(tab2[var2]))
        tab1 = tab1.merge(tab2, how='left', left_on=var1, right_on=var2,
                          suffixes=('',tab2_name))


