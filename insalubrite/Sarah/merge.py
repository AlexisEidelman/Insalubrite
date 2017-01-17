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

tables_to_merge = set(x[0] for x in foreign_key)
tables_to_merge_with = set(x[2] for x in foreign_key)

# tables_to_merge - tables_on_disk
# => {'datadocument', 'datadocumenttemp'} logique
# tables_to_merge_with - tables_on_disk
# => {'procedure', 'role'} récupéré par ailleur


for table, key, autre_table in foreign_key:
    print(table, key, autre_table)
    
tables_to_merge - tables_to_merge_with


for tab2_name in tables_to_merge_with - tables_to_merge:
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
     