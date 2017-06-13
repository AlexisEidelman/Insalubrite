#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement
"""

import pandas as pd
import os

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.affhygiene import tables_reliees_a, est_reliee_a
from insalubrite.Sarah.adresses import adresses
from insalubrite.match_to_ban import merge_df_to_ban

from insalubrite.config_insal import  path_output


#######################
#Problèmes constatés##
######################

##### PV Ravalement ##########
def pv_table():
    """"
        Crée la table <PV> ravalement: affaires avec pv (ou non) avec quelques
        infos comme la date de l'envoi du pv, quel facade concernée, quel
        immeuble.
    """
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
    affravalement = read_table('affravalement')
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


###### Incitation au ravalement (dans le cadre d'une affaire) ######
# une incitation au ravalement est l'analogue d'une prescription dans une
# affaire d'hygiene
# se fait à la même adresse
def incitation_table():
    """
       Crée la table <incitation> au ravalement.
    """
    incitation_ravalement = read_table('incitation_ravalement')
    #pour la même affaire ouverte à une adresse donnée
    # plusieurs incitations au ravalement possibles (jusqu'à 20)

    incitation_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours',
                                'nature'
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


    # une incitation peut consuire à un arrêté
    arrete_ravalement_incitation_ravalement = read_table('arrete_ravalement_incitation_ravalement')
    arrete_ravalement_incitation_ravalement.rename(
            columns = {'incitationsreferencees_id':'incitation_ravalement_id'},
            inplace = True)
    incitation = incitation.merge(arrete_ravalement_incitation_ravalement,
                                  on='incitation_ravalement_id',
                                  how = 'left',
                                  #indicator = True,
                                  )
    incitation.rename(columns = {'arrete_ravalement_id':'arrete_suite_a_incitation_id'},
                 inplace = True)
    incitation['arrete_suite_a_incitation'] = incitation['arrete_suite_a_incitation_id'].notnull()
    incitation.drop('arrete_suite_a_incitation_id', axis = 1, inplace = True)
    return incitation

#### Arrêté Ravalement ############
def arrete_table():
    """
       Crée la table <arrêté> avec les infos temporelles sur la vie de l'arrêté
    """
    ###Travail sur la table arrete ravalement ###
    arrete_ravalement = read_table('arrete_ravalement')
    arrete_ravalement.drop(['copieconforme_en_cours', 'renotification_en_cours',
                            'cdd_id','nature'],
                           axis = 1,
                           inplace = True,
                           )
    dates = ['date_delai','date_enregistrement','date_envoi',
             'date_notification','date_signature','date_visite']
    arrete_ravalement[dates] = arrete_ravalement[dates].apply(lambda x: x.astype(str).str[:10])
    dates_arrete = [col + '_arrete' for col in dates]
    rename_dates = dict(zip(dates, dates_arrete))
    rename_dates['id'] = 'arrete_ravalement_id'
    arrete_ravalement.rename(columns = rename_dates, inplace = True)

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
    
    table['injonction'] = table['injonction_id'].notnull()
    table.drop('injonction_id', axis = 1, inplace = True)
    return table


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
    #TODO: vérifier que c'est effectivement inutile
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

immeuble = immeuble_table()
#immeuble_id est un bien_id qui permettra de relier l'affaire à son adresse
print("{} immmeubles disponibles pour matcher".format(immeuble.immeuble_id.nunique()))

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

if __name__ == '__main__':
    ##############################
    #####   Etape FINALE  #########
    #############################

    #Table créées aux étapes précédentes: pv, incitation, arrete, facade, immeuble

    # HYPOTHESE PV
    # On va garder un pv par affaire
    # généralement lorsqu'on a plusiseurs pv pour une affaire c'est souvent
    # le même immeuble mais des façades différentes: donc on va garder juste une
    # façade
    #pv.groupby(['affaire_id','facade_id']).size().sort_values()
    #pv = pv.groupby('affaire_id').first().reset_index()

    def pv_final():
        pv = pv_table()
        print("{} affaires de ravalement dans {} immeubles".format(pv.affaire_id.nunique(),
              pv.immeuble_id.nunique()))
        print("{} procès verbaux de ravalement".format(pv.pv_ravalement_id.nunique()))
        #la longueur de table pv est supérieure aux nombres d'affaires car
        #La même affaire peut avoir plusieurs pv
        #pv.pv_ravalement_id.value_counts(dropna=False)
        #pv.query("pv_ravalement_id == 57632")
        
        pv = pv.merge(facade,
                      on = 'facade_id',
                      how = 'left',
                      #indicator = '_merge_facade_pv',
                      )
        ## Etape rename ##
        facades_infos = ['copropriete','designation','batiment_id','type_facade',
                         'hauteur_facade','materiau_facade','affectation_facade']
        facades_pv = [col + '_pv' for col in facades_infos]
        rename_facades_pv = dict(zip(facades_infos, facades_pv))
        #rename_facades_pv['id'] = 'pv_ravalement_id'
        pv.rename(columns = rename_facades_pv, inplace = True)
    
        pv.drop('facade_id', axis=1,inplace = True)
        
        
        return pv
    
    pv_ravalement = pv_final()
    pv_ravalement = pv_ravalement[pv_ravalement.pv_ravalement_id.notnull()]
    pv_ravalement.drop('date_envoi_pv', axis = 1, inplace = True)
    pv_ravalement['pv_ravalement'] = pv_ravalement['pv_ravalement_id'].notnull()
    pv_ravalement.drop('pv_ravalement_id', axis = 1, inplace = True)
    
    # HYPOTHESE INCITATION
    #On va garder une incitation par affaire
    ##incitation.groupby('affaire_id').size().sort_values()
    #incitation.query("affaire_id == 4935")
    ##2 incitations (en 2007 puis en 2008) à la même adresse sur 3 façades:
    ## chaque fois ça a donné des arrêtés différents
    #incitation.query("affaire_id == 2704")
    ##même constat
    #incitation = incitation.groupby('affaire_id').first().reset_index()
    
    def incitation_final():
        incitation = incitation_table()
        print("{} incitations".format(incitation.incitation_ravalement_id.nunique()))
        incitation.drop('adresse_id', axis=1, inplace = True)
        
        incitation = incitation.merge(facade,
                                      on = 'facade_id',
                                      how = 'left',
                                      #indicator = '_merge_facade_incitation',
                                      )
        ## Etape rename ##
        facades_infos = ['copropriete','designation','batiment_id','type_facade',
                         'hauteur_facade','materiau_facade','affectation_facade']
        facades_incitation = [col + '_incitation' for col in facades_infos]
        rename_facades_incitation = dict(zip(facades_infos, facades_incitation))
        incitation.rename(columns = rename_facades_incitation, inplace = True)

        incitation.drop('facade_id', axis=1, inplace = True)
        return incitation

    incitation_ravalement = incitation_final()
    incitation_ravalement = incitation_ravalement[
            incitation_ravalement.incitation_ravalement_id.notnull()]
    incitation_ravalement.drop('incitation_ravalement_id', axis = 1, inplace = True)
    
    # HYPOTHESE ARRETE
    # On va garder un arrêté par affaire
    #arrete = arrete.groupby('affaire_id').first().reset_index()
    def arrete_final():
        arrete = arrete_table()
        print("{} arrêtés".format(arrete.arrete_ravalement_id.nunique()))

        arrete.drop('adresse_id', axis = 1 , inplace =True)
        arrete = arrete.merge(facade,
                              on = 'facade_id',
                              how = 'left',
                              #indicator = '_merge_facade_arrete',
                              )
        ## Etape rename ##
        facades_infos = ['copropriete','designation','batiment_id','type_facade',
                         'hauteur_facade','materiau_facade','affectation_facade']
        facades_arrete = [col + '_arrete' for col in facades_infos]
        rename_facades_arrete = dict(zip(facades_infos, facades_arrete))
        arrete.rename(columns = rename_facades_arrete, inplace = True)
        arrete.drop(['facade_id','date_enregistrement_arrete', 
                     'date_envoi_arrete', 'date_notification_arrete', 
                     'date_signature_arrete', 'date_visite_arrete'], 
                    axis=1, inplace = True)
        return arrete
    
    arrete_ravalement = arrete_final()

    #TODO
    ##### Merge IMMEUBLE ####
#    affaire = pv_incitation_arrete_facade.merge(immeuble,
#                                on = ['immeuble_id',
#    #                                  'adresse_id',
#                                      ],
#                                how = 'left',
#                                suffixes = ['','_immeuble'],
#    #                            indicator = '_merge_immeuble',
#                                )
    def _ravalement(table, suffixe, output):
        table = affaire_avec_adresse(table)
        table.drop(['typeadresse','adresse_id'],axis=1,inplace = True)
        table.rename(columns = {'libelle':'libelle_'+suffixe+'_ravalement'},
                             inplace = True)
        path_ravalement = os.path.join(path_output, output)
        table.to_csv(path_ravalement, encoding="utf8", index=False)
        return table
    
    pv_ravalement = _ravalement(pv_ravalement,'pv', 'pv_ravalement.csv')
    incitation_ravalement = _ravalement(incitation_ravalement,'incitation', 
                                        'incitation_ravalement.csv')
    arrete_ravalement = _ravalement(arrete_ravalement, 'arrete', 
                                    'arrete_ravalement.csv')
    
    incitation_ravalement = incitation_ravalement[
            incitation_ravalement.adresse_ban_id.notnull()]
    arrete_ravalement = arrete_ravalement[
            arrete_ravalement.adresse_ban_id.notnull()]
    