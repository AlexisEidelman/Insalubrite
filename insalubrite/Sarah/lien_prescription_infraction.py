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
    prescription.rename(columns={'id':'prescription_id',
                                 'libelle':'prescription'
                                 },
                        inplace=True
                        )
    
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
    prescription_augmentee = prescription_augmentee.groupby('affaire_id'
                                                        ).first().reset_index()
    
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
    infraction_augmentee = infraction_augmentee.groupby('affaire_id'
                                                        ).first().reset_index()
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
    
    #Petit travail de remplissage suite à l'étude de infraction 30
    infraction30 = lien_prescription_infraction.infractiontype_id == 30
    lien_prescription_infraction.loc[infraction30,
                            'prescriptiontype_id'].fillna(23, inplace = True)
    lien_prescription_infraction.loc[infraction30,
                           'type_prescription'].fillna('Libre', inplace = True)
    lien_prescription_infraction.loc[infraction30,
                            'type_infraction'].fillna('Libre', inplace = True)
    
    return lien_prescription_infraction

def etude_lien_prescription_infraction30():
    from insalubrite.Sarah.lien_prescription_infraction import lien_prescription_infraction
    lien = lien_prescription_infraction()
    lien.groupby(['infractiontype_id','prescriptiontype_id']
                ).size().sort_values(ascending=False)
    #conclusion: à un type d'infraction correspond un type de prescription
    
    # étude 1:  le cas infraction_type == 30 
    test_salubre = lien[lien.infractiontype_id == 30]
    len(test_salubre) ##=>5724
    test_salubre.groupby(['infractiontype_id','prescriptiontype_id']).size(
            ).sort_values(ascending=False)
    #un seul type de prescription associé: prescriptiontype_id = 23
    test_salubre.prescriptiontype_id.value_counts(dropna=False)
    test_salubre.type_prescription.value_counts(dropna=False)
    test_salubre.articles.value_counts(dropna=False)
    # les articles ne correspondent à rien dans le
    # code de la santé publique sauf CSP L 1331-22 
    # ni dans code de construction et de l'habitat
    
    # les valeurs non attribuées n'ont aucune raison de le rester
    test_salubre['prescriptiontype_id'].fillna(23, inplace = True)
    test_salubre['type_prescription'].fillna('Libre', inplace = True)
    test_salubre['type_infraction'].fillna('Libre', inplace = True)
    
    test_salubre.groupby(['prescription']).size().sort_values(ascending=False)
    ## => Reprise 81.2%, Libre 5.4%
    
    test_salubre.articles.value_counts(dropna=False)
    #la majorité des articles ne sont pas mentionnés: 94.3%
    test_salubre.groupby(['type_prescription','prescription']).size().sort_values(ascending=False)
    ##=>type_prescription = 'Libre' prescription: 'Reprise' 4646, 'Libre' 309
    
    #étude 2: les cas où apparaît la modalité 'Libre'
    libre = lien[lien.prescription == 'Libre']
    libre.type_infraction.value_counts(dropna=False)
    
    libre = lien[lien.type_infraction == 'Libre']
    libre.prescription.value_counts(dropna=False)
    
    
    #Résultats:
    # infraction_type == 30 <=> prescriptiontype == 23 
    # => 81 % prescription == 'Reprise' , 5.4% prescription == 'Libre' 
    # et 94% d'articles pas mentionnés
    #Interprétation:
    # lorsque l'inspecteur a fait la visite du logement il n'a pas trouvé 
    # les signes d'insalubrité attendus
    # Il a donc exercé son droit de reprise pour déclarer le logement non touché
    #Conclusion
    # il faut considérer les infractiontype == 30 comme salubres

if __name__ == '__main__':
    lien = lien_prescription_infraction()
    