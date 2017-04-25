#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lien infraction prescription

"""

import pandas as pd
import numpy as np

from insalubrite.Sarah.read import read_table

def prescription_table():
    ##prépare table prescription
    prescription = read_table('prescription')
    prescription.rename(columns={'id':'prescription_id'},inplace=True)
    prescription.rename(columns={'libelle':'prescription'},inplace=True)
    
    #prépare table prescription_type
    prescription_type = read_table('prescriptiontype')
    #prescription_type.ordre.isnull().all() ## =>True
    #prescription_type.active.value_counts()
    #True 32, False 1
    prescription_type.drop(['active','ordre'], axis = 1, inplace = True)
    prescription_type.rename(columns={'id':'prescriptiontype_id',
                                      'libelle':'type_prescription'},
                             inplace=True
                             )
    
    prescription_augmentee = prescription.merge(prescription_type,
                                                on = 'prescriptiontype_id',
                                                how = 'left',
    #                                            indicator = True
                                                )
    
    ##prépare etat_prescription
    etat_prescription = read_table('etatprescription')
    assert prescription.etat_id.isin(etat_prescription.id).all()
    etat_prescription.rename(columns={'id':'etat_id',
                                      'libelle':'etat_prescription'
                                      },
                             inplace=True
                             )
    
    prescription_augmentee = prescription_augmentee.merge(etat_prescription,
                                                          on = 'etat_id',
                                                          how = 'left'
                                                          )
    prescription_augmentee = prescription_augmentee.groupby('affaire_id').first().reset_index()
    
    return prescription_augmentee

def infraction_table():
    infraction = read_table('infraction')[['affaire_id', 'infractiontype_id']]

    infractiontype = read_table('infractiontype')
    infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
    infractiontype.rename(columns={'id': 'infractiontype_id',
                                   'libelle': 'type_infraction'}, inplace=True)

    infraction_augmentee = infraction.merge(infractiontype, 
                                            on='infractiontype_id', 
                                            how='left'
                                            )
    infraction_augmentee = infraction_augmentee.groupby('affaire_id').first().reset_index()
    return infraction_augmentee

def lien_prescription_infraction():
    prescription = prescription_table()
    #len(prescription) ##=>17799
    infraction = infraction_table()
    #len(infraction) ##=>18232
    lien_prescription_infraction = prescription.merge(infraction, 
                                                      on=['affaire_id','infractiontype_id'], 
                                                      how='outer'
                                                      )
    #len(lien_prescription_infraction) ##=>20313
    return lien_prescription_infraction

if __name__ == '__main__':
    #from insalubrite.Sarah.lien_prescription_infraction import lien_prescription_infraction
    lien = lien_prescription_infraction()
    lien.groupby(['prescription_id','infractiontype_id']).size()