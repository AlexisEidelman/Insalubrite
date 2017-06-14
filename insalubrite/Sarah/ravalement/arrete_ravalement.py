#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement: ARRETE
"""

import pandas as pd

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.ravalement.create_table_ravalement import _ravalement
from insalubrite.Sarah.ravalement.facade import facade_table

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

# HYPOTHESE ARRETE
# On va garder un arrêté par affaire
#arrete = arrete.groupby('affaire_id').first().reset_index()
def arrete_final():
    arrete = arrete_table()
    print("{} arrêtés".format(arrete.arrete_ravalement_id.nunique()))

    arrete.drop('adresse_id', axis = 1 , inplace =True)
    
    facade = facade_table()
    print("{} différentes façades pour {} bâtiments".format(facade.facade_id.nunique(),
      facade.batiment_id.nunique()))
    
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
arrete_ravalement = _ravalement(arrete_ravalement, 'arrete', 
                                'arrete_ravalement.csv')

arrete_ravalement = arrete_ravalement[
        arrete_ravalement.adresse_ban_id.notnull()]