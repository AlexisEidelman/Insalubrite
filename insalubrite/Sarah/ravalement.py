#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement
"""

import pandas as pd


from insalubrite.config_insal import path_sarah, path_output
from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.affhygiene import tables_reliees_a, est_reliee_a
from insalubrite.Sarah.adresse_de_l_affaire import add_adresse_id

#############################
##tables utiles à l'étude###
############################

affravalement = read_table('affravalement')
#tables_reliees_a('affravalement')
##('arrete_ravalement', 'affaire_id', 'affravalement'),
##('incitation_ravalement', 'affaire_id', 'affravalement'),
##('pv_ravalement', 'affaire_id', 'affravalement')
#est_reliee_a('affravalement')
##('affravalement', 'affaire_id', 'affaire'),
##('affravalement', 'immeuble_id', 'immeuble')


########################
#Adresse de l'affaire##
#######################
immeuble = read_table('immeuble')
immeuble.rename(columns = {'id':'immeuble_id',
                           'adresseprincipale_id': 'adresse_id'},
                inplace = True)
#immeuble_id est un bien_id qui permettra de relier l'affaire à son adresse 
# via add_adresse_id

#affaire = read_table('affaire')
#table = affravalement.merge(immeuble,
#                            on = 'immeuble_id',
#                            how = 'outer',
#                            indicator = True,
#                            )
#tables_reliees_a('immeuble')
##('affravalement', 'immeuble_id', 'immeuble'),
##('batiment', 'immeuble_id', 'immeuble'),
##('certificatnotaire', 'immeuble_id', 'immeuble'),
##('immeuble_affaireperilsecu', 'immeuble_id', 'immeuble'),
##('operation_immeuble', 'immeuble_id', 'immeuble')
#est_reliee_a('immeuble')

#######################
#Problèmes constatés##
######################

##### PV Ravalement ##########
def pv_table():
    ##Travail sur la table pv ravalement##
    pv_ravalement = read_table('pv_ravalement')
    pv_ravalement.rename(columns = {'id':'pv_ravalement_id',
                                    'date_envoi':'date_envoi_pv',
                                    'date_creation':'date_creation_pv'},
                    inplace = True)
    #pb_de_date = ['11-04-29 00:00:00']
    #pv_ravalement.query('date_envoi >= "11-04-29 00:00:00"')
    pv_ravalement['date_envoi_pv'] = pv_ravalement['date_envoi_pv'].str[:10]
    pv_ravalement['date_creation_pv'] = pv_ravalement['date_creation_pv'].astype(str).str[:10]
    pv_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours','numero'],
                       axis = 1,
                       inplace = True,
                       )
    # un procès verbal a été envoyé à date_envoi
    
    ##Merge avec affravalement##
    table = affravalement.merge(pv_ravalement,
                                on = 'affaire_id',
                                how= 'left',
                                )
    
    #tables_reliees_a('pv_ravalement')
    
    pv_ravalement_facade = read_table('pv_ravalement_facade')
    pv_ravalement_facade.rename(columns = {'facadesconcernees_id':'facade_id'},
                                inplace = True,
                                )
    ##Merge avec pv_ravalementè_facade##
    #Fais la liaison avec les façades
    table = table.merge(pv_ravalement_facade,
                        on = 'pv_ravalement_id',
                        how = 'left',
                        #indicator = True,
                        )
    return table

pv = pv_table()

# relie à la façade concernée
#est_reliee_a('pv_ravalement_facade')

##len(facade) =>105839

############################
###Travail sur façade#######
###########################
est_reliee_a('facade')
## ('facade', 'affectfacade_id', 'affectfacade'),
## ('facade', 'materfacade_id', 'materfacade'),
# ('facade', 'batiment_id', 'batiment'),
## ('facade', 'hautfacade_id', 'hautfacade'),
# ('facade', 'adresse_id', 'adresse'),
## ('facade', 'typefacade_id', 'typefacade')]
def facade_table():
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
    #TODO: vérifier que c'est effectivement inutile
    table.drop(['the_geom'], axis = 1, inplace = True)
    return table

facade = facade_table()

table = table.merge(facade,
                    on = 'facade_id',
                    how = 'left',
                    indicator = True,
                    )
#table._merge.value_counts(dropna=False)
#variables d'intérêt: adresse_id, batiment_id, typefacade

###### Incitation au ravalement (dans le cadre d'une affaire) ######
def incitation_table():
    incitation_ravalement = read_table('incitation_ravalement')
    #pour la même affaire plusieurs incitations au ravalement possibles
    
    incitation_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours',
                                #'nature'
                                ],
                       axis = 1,
                       inplace = True,
                       )
    incitation_ravalement.rename(columns = {'id':'incitation_ravalement_id',
                                    'date_envoi':'date_envoi_incitation_ravalement'},
                                 inplace = True)
    incitation_ravalement['date_envoi_incitation_ravalement'] = \
                incitation_ravalement['date_envoi_incitation_ravalement'].str[:10]
    
    incitation_ravalement_facade = read_table('incitation_ravalement_facade')
    incitation_ravalement_facade.rename(columns = {'facadesconcernees_id':'facade_id'},
                                inplace = True,
                                )
    incitation = incitation_ravalement.merge(incitation_ravalement_facade,
                                on = 'incitation_ravalement_id',
                                how = 'left')
    
    ###Petit travail sur les délais d'incitation ###
    delai = incitation['delai']*((incitation['type_delai']==3).astype(int) + 
                      30*((incitation['type_delai']==4).astype(int)))
    incitation['delai_incitation_raval_en_jours'] = delai
    incitation.drop(['delai','type_delai'], axis = 1, inplace = True)
    return incitation
incitation_ravalement = incitation_table()
# une incitation peut consuire à un arrêté
arrete_ravalement_incitation_ravalement = read_table('arrete_ravalement_incitation_ravalement')

#### Arrêté Ravalement ############
def arrete_table():
    ###Travail sur la table arrete ravalement ###
    arrete_ravalement = read_table('arrete_ravalement')
    arrete_ravalement.rename(columns = {'id':'arrete_ravalement_id',
                                        'date_delai':'date_delai_arrete',
                                        'date_enregistrement':'date_enregistrement_arrete',
                                        'date_envoi':'date_envoi_arrete',
                                        'date_notification':'date_notification_arrete',
                                        'date_signature':'date_signature_arrete',
                                        'date_visite':'date_visite_arrete'},
                            inplace = True)
    arrete_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours'],
                           axis = 1,
                           inplace = True,
                           )
    arrete_ravalement['date_delai_arrete'] = \
                arrete_ravalement['date_delai_arrete'].astype(str).str[:10]
    arrete_ravalement['date_enregistrement_arrete'] = \
                arrete_ravalement['date_enregistrement_arrete'].astype(str).str[:10]
    arrete_ravalement['date_envoi_arrete'] = \
                arrete_ravalement['date_envoi_arrete'].astype(str).str[:10]
    arrete_ravalement['date_notification_arrete'] = \
                arrete_ravalement['date_notification_arrete'].astype(str).str[:10]
    arrete_ravalement['date_signature_arrete'] = \
                arrete_ravalement['date_signature_arrete'].astype(str).str[:10]
    arrete_ravalement['date_visite_arrete'] = \
                arrete_ravalement['date_visite_arrete'].astype(str).str[:10]
    ###Petit travail sur les délais d'arreté ###
    delai = arrete_ravalement['delai']*((arrete_ravalement['type_delai']==3).astype(int) + 
                      30*((arrete_ravalement['type_delai']==4).astype(int)))
    arrete_ravalement['delai_arrete_raval_en_jours'] = delai
    arrete_ravalement.drop(['delai','type_delai'], axis = 1, inplace = True)            
    
    arrete_ravalement_facade = read_table('arrete_ravalement_facade')
    arrete_ravalement_facade.rename(columns = {'facadesconcernees_id':'facade_id'},
                                    inplace = True)
    table = arrete_ravalement.merge(arrete_ravalement_facade, 
                                    on = 'arrete_ravalement_id',
                                    how = 'left')
    return table
arrete = arrete_table()

#TODO: Merge avec facade
#Table créées: pv, incitation, arrete, facade





