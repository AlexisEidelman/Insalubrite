# -*- coding: utf-8 -*-
"""
Fusionne toutes les tables

On récupère une liste des tables pérésentes et on utilise le
sql pour voir si on peut faire le rattachement automatique


Une étape compliquée consiste à définir un ordre des fusions. 
En effet, on veut aggréger d'abord les tables qui n'ont pas de 
lien avec les autres. 

TODO : REGARDER PAR AFFAIRE !!

"""

import os

from collections import Counter

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

def lists_table_from_links(list_of_links, set_format=False):
    # les tables que l'on doit augmenter
    tables_to_merge = [x[0] for x in list_of_links]
    # les tables qui augmentent
    tables_to_merge_with = [x[2] for x in list_of_links]
    if set_format:
        return set(tables_to_merge), set(tables_to_merge_with)
    return tables_to_merge, tables_to_merge_with


tables_to_merge, tables_to_merge_with = lists_table_from_links(foreign_key, 
                                                               set_format=True)
# tables_to_merge - tables_on_disk
# => {'datadocument', 'datadocumenttemp'} logique
# tables_to_merge_with - tables_on_disk
# => {'procedure', 'role'} récupérés par ailleurs

tables_isolees = tables_good - tables_to_merge - tables_to_merge_with
print('on a au depart', len(tables_good), 'tables')
# {'communefrance', 'perioderecolement', 'jbpm_variable', 'parametres_edition'}
#for table in tables_isolees:
#    isolee = read_table(table)
#    print('\n', table)
#    print(isolee)

########################
#  Fusions
########################
def fusion(list_des_fusions, verbose=False):
    for tab2_name in list_des_fusions:
        tab2 = read_table(tab2_name)
        var2 = primary_key[tab2_name]
        assert var2 in tab2 or var2 == 'None'
#        print(tab2_name, len(tab2))
        for link in [x for x in foreign_key if x[2] == tab2_name]:
            tab1_name = link[0]
            var1 = link[1]
            if verbose:
                print('On augmente ',tab1_name , 'avec', tab2_name)
#            tab1 = read_table(tab1_name)
#            assert var1 in tab1.columns
#            assert all(tab1.loc[tab1[var1].notnull(), var1].isin(tab2[var2]))
#            tab1 = tab1.merge(tab2, how='left', left_on=var1, right_on=var2,
#                              suffixes=('',tab2_name))
            

########################
#  Ordre des fusions
########################

tables_good = set(primary_key.keys()) & tables_on_disk & (tables_to_merge | tables_to_merge_with)
print('on a au depart', len(tables_good), 'tables')
print('on a aussi ', len(tables_isolees), 'tables qui ne sont reliées à aucune autre')

# Si on voit les liens comme un arbre, on procède en coupant les feuilles"
# On regarde les tables qui sont des "merge_with" mais pas des "to_merge"
# on supprime le lien à chaque fois ce qui permet de réitérer le processus

# on copie la liste de liens
num_etape = 0
links = [x for x in foreign_key if x[0] in tables_good and x[2] in tables_good]
links = [x for x in links if x[0] != x[2]] # retire ARRETE_RAVALEMENT dans ARRETE_RAVALEMENT


def columns_by_table():
    columns = dict()
    tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))
    for table in tables_on_disk:
        tab = read_table(table, nrows=0)
        columns[table] = tab.columns
    return columns


cols = columns_by_table()


def links_of_table(table_name, links):
    list_of_tab = [x for x in links if x[0] == table_name or x[2] == table_name]
    return list_of_tab


### tables proches par leur columns ###

tab_by_cols = {}
for key, value in sorted(cols.items()):
    ident_columns = value.tolist()
    ident_columns = ' '.join(sorted(ident_columns))
    tab_by_cols.setdefault(ident_columns, []).append(key)


for key, value in tab_by_cols.items():
    if len(value) > 1:
        print(key)
    

###### Tables Ponts ###########
# on gère les "tables ponts" : celles qui n'ont que deux colonnes qui 
# mettent en relation deux tables. Il vaut mieux les traiter tout de suite.

# reperage des ces tables
def tables_ponts(columns):
    deux_colonnes = dict(
        val for val in columns.items()
        if len(val[1]) == 2)
    
    pont = list()
    libelle = list()
    autres = list()
    for key, val in deux_colonnes.items():
        if val[0] == 'id' and val[1] == 'libelle':
            libelle.append(key)
        elif val[0][-3:] == '_id' and val[1][-3:] == '_id':
            links_tab = links_of_table(key, links)
            if len(links_tab) == 2:
                pont.append(key)
            else:
                print(key, val)
                print(links_tab)
                autres.append(key)
        else:
            print('else', key, val)
            autres.append(key)

    return pont, libelle, autres



ponts, libelles, autres = tables_ponts(cols)
#
links_pont = list()
for tab_pont in ponts:
    for link in links_of_table(tab_pont, links):
        links.remove(link)
        links_pont.append(link)        


# liste des tables

def common_table(tab1_name, tab2_name):
    ''' regarde si deux tables ne sont pas en fait deux sous ensembles
        d'un même truc
    '''
    tab1 = read_table(tab1_name)
    tab2 = read_table(tab2_name)



###### Autres tables  ###########
tables_csv_good = tables_good.copy()
list_of_merge = ['initialisation']
while len(list_of_merge) > 0:
    num_etape += 1 
    print('\n ********* étape', str(num_etape))
    print("à cette étape, on a", len(links), 'liens')
    to_merge, merge_with = lists_table_from_links(links)
    count_to_merge = Counter(to_merge)
    # on commence par les tables qui sont augmentées mais qui n'augmentent pas
    list_of_merge = set(merge_with) - set(to_merge)
    print("On a", len(list_of_merge), 'table qui ne seront plus augmentées. \n')
    print("il s'agit de : \n", '\n - '.join(list_of_merge), '\n')
    fusion(list_of_merge, verbose=True)
    links = [x for x in links if x[2] not in list_of_merge]
    tables_csv_good -= list_of_merge
    


# est-ce que affaire est une table centrale ? 

oriented_links = [x for x in foreign_key if x[0] in tables_good and x[2] in tables_good]
oriented_links = [x for x in oriented_links if x[0] != x[2]] # retire ARRETE_RAVALEMENT dans ARRETE_RAVALEMENT
import pandas as pd
to_gephi = pd.DataFrame.from_records(oriented_links)
to_gephi.columns = ['target', 'lien', 'source']
to_gephi.to_csv('links.csv', index=False)