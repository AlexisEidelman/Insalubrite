#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Enrichit la BDD avec les données de ravalement: PV
"""

import pandas as pd

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.ravalement.create_table_ravalement import _ravalement
from insalubrite.Sarah.ravalement.facade import facade_table

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

def pv_final():
    pv = pv_table()
    print("{} affaires de ravalement dans {} immeubles".format(pv.affaire_id.nunique(),
          pv.immeuble_id.nunique()))
    print("{} procès verbaux de ravalement".format(pv.pv_ravalement_id.nunique()))
    #la longueur de table pv est supérieure aux nombres d'affaires car
    #La même affaire peut avoir plusieurs pv
    #pv.pv_ravalement_id.value_counts(dropna=False)
    #pv.query("pv_ravalement_id == 57632")
    
    facade = facade_table()
    print("{} différentes façades pour {} bâtiments".format(facade.facade_id.nunique(),
      facade.batiment_id.nunique()))

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

pv_ravalement = _ravalement(pv_ravalement,'pv', 'pv_ravalement.csv')
