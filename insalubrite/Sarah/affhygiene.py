# -*- coding: utf-8 -*-
"""
Analyse autours de affhygiène
"""
import os


from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_sql, read_table

primary_key, foreign_key = read_sql()
tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

def tables_reliees_a(name_table):
    return [x for x in foreign_key if x[2] == name_table]

def est_reliee_a(name_table):
    return [x for x in foreign_key if x[1] == name_table]
    
# on copie la liste de liens

tab = read_table('affhygiene')
cercle1 = tables_reliees_a('affhygiene')
print(cercle1)

#####################
### Premier tour ####

for element in cercle1:
    name = element[0]
    cercle2 = tables_reliees_a(name)
    print(name)
    if len(cercle2) == 0:
        tab_autre = read_table(name)
        print(tab_autre.columns)
        tab_autre.columns = [x + '_' + name for x in tab_autre.columns]
        tab_autre = tab_autre.rename(columns={'affaire_id_' + name: 'affaire_id'})
#        print(tab_autre.head())
        tab = tab.merge(tab_autre, on='affaire_id', how='left') # c'est toujours affaire_id
    else:
        print(cercle2)

################################################
### Des tables compliquées et peut-être inutiles

# on se dit qu'il y a un truc à regarder avec trois tables. 
# est-ce qu'elles se complètent ? 
# est-ce qu'une affaire est obligatoirement dans l'une des trois ?
suspectes = ['arretehyautre', 'mainlevee', 'recouvrement']
tables_suspectes = dict()
affaires_id = dict()
for name in suspectes:
    tables_suspectes[name] = read_table(name)
    affaires_id[name] = tables_suspectes[name]['affaire_id']

assert all(affaires_id['mainlevee'].isin(affaires_id['arretehyautre']))
tables_suspectes['mainlevee']
test = tables_suspectes['arretehyautre'].merge(
    tables_suspectes['mainlevee'],
    on = 'affaire_id',
    how='left')
# recouvrement  a deux ligne, deux fois la même au numéro d'identifiant près

# pour l'instant pas de conclusion
# TODO: en faire une ! :) savoir quel est l'intérêt de ces tables


# Prescription : lien en la table et la tablehisto
presc = read_table('prescription')
histo = read_table('prescriptionhisto')
tout = presc.merge(histo, left_on = 'id', right_on = 'prescription_id',
                   how='outer')

tout['libelle_y'][tout['libelle_x'] != tout['libelle_y']]

### 
