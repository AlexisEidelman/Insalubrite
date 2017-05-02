#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Etudie la cohérence mise en demeure Sarah

"""

import pandas as pd
import os 

from insalubrite.Sarah.read import read_sql, read_table
from insalubrite.Sarah.affhygiene import tables_reliees_a, est_reliee_a

from insalubrite.config_insal import path_sarah


def etude_preliminaire():
    """
    """
    mise_en_demeure = read_table('mise_en_demeure')
    mise_en_demeure.head()
    mise_en_demeure.groupby(['date_envoi','date_notification']).size()
    cercle1 = tables_reliees_a('mise_en_demeure')
    ##('pvrsd', 'mise_en_demeure_id', 'mise_en_demeure')
    cercle2 = est_reliee_a('mise_en_demeure')
    ##('mise_en_demeure', 'compte_rendu_id', 'cr_visite'),
    ##('mise_en_demeure', 'type_delai', 'typedelai')
    pvrsd = read_table('pvrsd')
    pvrsd.head()
    cercle3 = tables_reliees_a('pvrsd')
    #Vide
    cercle4 = est_reliee_a('pvrsd')
    ##('pvrsd', 'mise_en_demeure_id', 'mise_en_demeure'),
    ##('pvrsd', 'compte_rendu_id', 'cr_visite')
 
    
def mise_en_demeure_table():
    mise_en_demeure = read_table('mise_en_demeure')
    mise_en_demeure.rename(columns={'id': 'mise_en_demeure_id'}, 
                           inplace=True)
    #pvrsd = procès verbal règlement sanitaire départemental
    pvrsd = read_table('pvrsd')
    #var_to_merge_on = (mise_en_demeure.columns&pvrsd.columns).tolist()
    mise_en_demeure_augmentee = mise_en_demeure.merge(pvrsd,
                                                      on = 'mise_en_demeure_id',
                                                      how = 'left',
                                                      indicator = True
                                                      )
    typedelai = read_table('typedelai')[['id','libelle']]
    typedelai.rename(columns={'id': 'type_delai'}, 
                           inplace=True)
    mise_en_demeure_augmentee = mise_en_demeure_augmentee.merge(typedelai,
                                                                on = 'type_delai',
                                                                how = 'left')
    mise_en_demeure_augmentee['delai'] =  mise_en_demeure_augmentee['delai'].astype(str).str.cat(
                                         mise_en_demeure_augmentee['libelle'])
    mise_en_demeure_augmentee.drop(['type_delai','libelle'],
                                   axis = 1,
                                   inplace = True)
    cr_visite = read_table('cr_visite')[['id','affaire_id']]
    cr_visite.rename(columns={'id': 'compte_rendu_id'}, 
                           inplace=True)
    mise_en_demeure_augmentee = mise_en_demeure_augmentee.merge(cr_visite,
                                                                left_on = 'compte_rendu_id_x',
                                                                right_on = 'compte_rendu_id',
                                                                how = 'left')
    
    return mise_en_demeure_augmentee



    