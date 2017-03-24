# -*- coding: utf-8 -*-
"""
Analyse autours de affhygiène

Conclusion : cette table est intéressante car elle permet
via cr_visite, d'accéder aux résultats de l'affaire.
C'est donc par elle que l'on va avoir la classification des
bâtiments et des visites
"""

import os

from numpy import nan

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_sql, read_table

primary_key, foreign_key = read_sql()
tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

verbose = True

def tables_reliees_a(name_table):
    return [x for x in foreign_key if x[2] == name_table]

def est_reliee_a(name_table):
    return [x for x in foreign_key if x[0] == name_table]

# on copie la liste de liens

hyg = read_table('affhygiene')
cercle1 = tables_reliees_a('affhygiene')

## Contenu de la table affhygiene
if verbose:
    print('''La table affhygiene contient quatre variable:
        \t partie commune qui est vraie dans 5% des cas
        \t type bien concerne 
        \t le lien vers affaire_id
        \t le lien vers bien_id qui est l'id de ficherecolem qui pointe vers
        tournee et facade
        ''')

# print(cercle1)


tab = hyg.copy()


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


#######################################
########### CR_VISITE #################
#######################################
cr = read_table('cr_visite')

'''
résumé des lignes qui suivent et de l'impression sur la table :

Il est possible que cette table qui contient deux colonnes signalement police
et date_signalement_pref_police soit celle qui contienne la variable de sortie.

La table peut être enrichie (voir cercle_cr ci-dessous) et cela peut préciser
la procédure mais il faut voir si c'est utile réellement

On a quelques informations sur les logements (étage, superficie, composition),
mais c'est assez peu rempli.
Il y a des éléments sur le plomb et le saturnisme à mieux comprendre

Une interrogation sur le fait que des affaires n'ont pas de cr
et que d'autres en ont plusieurs

'''

# description de la table
len(cr)

# composition infantine
cr.loc[cr['composition_enfantine'] == 'inconnu', 'composition_enfantine'] = nan
cr.composition_enfantine.value_counts()
# non rempli dans 96% des cas !!
cr['nb_enf'] = cr.composition_enfantine.str.split(';').str.len()

# diag plomb
cr[cr.date_diag_plomb.notnull()].diag_plomb.value_counts()
# la date peut importe, on ajoute le plomb
# TODO: est ce que date_diag_plomb et date_signalement_saturn
# sont la même chose (semble le cas en fin de base)
cr[cr.diag_plomb == 1].iloc[:,[4,6]]
# TODO: quelle différence avec signalement saturn ?

cr.signalement_demandelogement.value_counts(dropna=False)
# => bonne variable, seulement 19 manquant

##### signalement_police
cr.signalement_police.value_counts(dropna=False)
# la bonne variabele ?


#fusion avec affhygiène
assert(all(cr.affaire_id.isin(hyg.affaire_id)))
hyg.affaire_id.isin(cr.affaire_id).value_counts()
# 50599 cr pour 38 000 affaires
# 9 % des affaires n'nont pas de cr
# 56% ont 1 CR
# 28% ont 2 CR
# les autres en ont 3 ou plus
### comprendre pourquoi :
## exemple: cr[cr.affaire_id == 11224]
## on a l'impression que c'est rempli une fois (enfant, superficie, etc.) et pas plus

###

###### tables reliées à CR_Visie
cercle_cr = tables_reliees_a('cr_visite')
for element in cercle_cr:
    name = element[0]
    cercle2 = tables_reliees_a(name)
    print(name)
    if len(cercle2) == 0:
        tab_autre = read_table(name)
        print(tab_autre.columns)
        # c'est toujours affaire_id
    else:
        print(cercle2)

## pvcsp, pvrsd, rapport et classement sont similaires
## mise_en_demeure est un peu particulier
## infractionhisto et prescriptionhisto de l'autre


###################################
####### Prescription ##############
###################################

# Prescription : lien en la table et la tablehisto
presc = read_table('prescription')
histo = read_table('prescriptionhisto')
tout = presc.merge(histo, left_on = 'id', right_on = 'prescription_id',
                   how='outer')
tout.head()
# ne correspond pas toujours les état, les reste change...
# TODO: demander ce que c'est
# interprétation actuelle : on a une prescription par
# affaire et un suivi jusqu'à ne plus en avoir.
# si c'est ça, comme ce qui nous intéresse c'est le premier avis, il faut
# surtout prescription_histo pour voir
# qui a eu un souci un jour et compléter par la prescription actuelle quand
# quand c'est le premier avis


#######################################
########### infraction #################
#######################################

inf = read_table('infraction')
histo = read_table('prescriptionhisto')
tout = presc.merge(histo, left_on = 'id', right_on = 'prescription_id',
                   how='outer')
tout.head()