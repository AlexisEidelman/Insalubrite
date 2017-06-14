#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement: FACADES
"""

import pandas as pd

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.affhygiene import est_reliee_a

############################
###      Façade     #######
###########################
est_reliee_a('facade')
## ('facade', 'affectfacade_id', 'affectfacade'),
## ('facade', 'materfacade_id', 'materfacade'),
# ('facade', 'batiment_id', 'batiment'),
## ('facade', 'hautfacade_id', 'hautfacade'),
# ('facade', 'adresse_id', 'adresse'),
## ('facade', 'typefacade_id', 'typefacade')]
def facade_table():
    """
       Crée la table <façade> qui donne des infos sur chaque façade: son nom
       (voir désignation), sa hauteur, le matériau de construction.
    """
    facade = read_table('facade')
    facade.rename(columns = {'id':'facade_id'},inplace = True)
    ####Merge avec type facade###
    def add_info(table_ini, info_table_name, col):
        info_table = read_table(info_table_name)
        info_table.drop(['active','ordre'], axis = 1, inplace = True)
        info_table.rename(columns = {'id':col[0],'libelle':col[1]},
                          inplace = True,
                          )
        table = table_ini.merge(info_table, on = col[0], how = 'left')
        table.drop([col[0]], axis = 1, inplace = True)
        return table
    ####Merge avec type facade###
    table = add_info(facade,'typefacade',['typefacade_id','type_facade'])
    ###Merge avec haut facade###
    table = add_info(table, 'hautfacade',['hautfacade_id','hauteur_facade'])
    ####Merge avec mater facade###
    table = add_info(table, 'materfacade',['materfacade_id','materiau_facade'])
    ####Merge avec affect facade###
    table = add_info(table, 'affectfacade',['affectfacade_id','affectation_facade'])
    ####Variables inutiles####
    table.drop(['the_geom','possedecbles','possedeterr','recolable'], 
               axis = 1, inplace = True)
    return table

facade = facade_table()
print("{} différentes façades pour {} bâtiments".format(facade.facade_id.nunique(),
      facade.batiment_id.nunique()))

#### Immeuble ############
def immeuble_table():
    """
       Crée la table <immeuble> qui fait le pont entre l'immeuble et la parcelle
       dans laquelle il se trouve
    """
    immeuble = read_table('immeuble')
    immeuble.rename(columns = {'id':'immeuble_id',
                               'adresseprincipale_id': 'adresse_id'},
                    inplace = True)

    ###Suppression colonnes inutiles ###
    immeuble = immeuble.loc[:, immeuble.notnull().sum() > 1] # retire les colonnes vides
    # une étude colonne par colonne
    del immeuble['champprocedure'] # tous vrais sauf 10
    del immeuble['demoli'] # tous vrais sauf 1
    que_des_2 = ['diagplomb', 'diagtermite', 'etudemql', 'grilleanah',
                 'rapportpreop', 'risquesaturn', 'signalementprefecturepolice',
                 ]
    immeuble.drop(que_des_2, axis=1, inplace=True)
    del immeuble['tournee_id'] # que 8 valeurs

    ### Date recolement ####
    immeuble['daterecolement'] = immeuble['daterecolement'].astype(str).str[:10]
    return immeuble

#immeuble = immeuble_table()
#immeuble_id est un bien_id qui permettra de relier l'affaire à son adresse
#print("{} immmeubles disponibles pour matcher".format(immeuble.immeuble_id.nunique()))
