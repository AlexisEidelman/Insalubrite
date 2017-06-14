#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement: INCITATION
"""

import pandas as pd

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.ravalement.create_table_ravalement import _ravalement
from insalubrite.Sarah.ravalement.facade import facade_table

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
    
    facade = facade_table()
    print("{} différentes façades pour {} bâtiments".format(facade.facade_id.nunique(),
      facade.batiment_id.nunique()))
    
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

incitation_ravalement = _ravalement(incitation_ravalement,'incitation', 
                                    'incitation_ravalement.csv')
incitation_ravalement = incitation_ravalement[
        incitation_ravalement.adresse_ban_id.notnull()]