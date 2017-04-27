# -*- coding: utf-8 -*-
"""
Relie l'affaire à l'adresse

Au depart, on passait par le signalement de l'affaire qui était
relié à une adresse.
Problème : beaucoup d'affaires n'ont pas de signalement.


Finalement, la comprhénsion de la variable bien_id directement
reliée à affygiène fait prendre une autre piste plus précise et
plus compléte puisqu'on n'a plus le problème de signalement

Ce programme compare les adresses obtenues en passant par 
bien id et par signalement

"""

import os
import pandas as pd
import numpy as np

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.Sarah.bien_id import adresse_via_bien_id
from insalubrite.Sarah.signalement import adresses_via_signalement

def coherence_adresses_via_signalement_et_via_bien_id():
    hyg = read_table('affhygiene')
    hyg_bien_id = adresse_via_bien_id(hyg)
    hyg_bien_id = hyg_bien_id[['affaire_id', 'bien_id',
            'bien_id_provenance',
            'parcelle_id',
            'adresse_id',
            'codeinsee',
            'codepostal']]
    
    hyg_bien_id = hyg_bien_id[hyg_bien_id.adresse_id.notnull()]
    
    hyg_signalement = adresses_via_signalement(hyg)
    hyg_signalement = hyg_signalement[hyg_signalement.adresse_id.notnull()]
    hyg_signalement = hyg_signalement.groupby('affaire_id').first().reset_index()
    
    test = hyg_signalement.merge(hyg_bien_id, on='affaire_id',
                                 how='outer',
                                 indicator='origine',
                                 suffixes=('_bien','_sign'),
                                 )
    
    
    test.origine.value_counts()
    
    matched = test[test['origine'] == 'both']
    matched['valid'] = matched['adresse_id_bien'] == matched['adresse_id_sign']
    
    adresse = adresses()[['adresse_id', 'libelle','codepostal', 'code_cadastre']]
    matched = matched.merge(adresse, left_on='adresse_id_bien', right_on = 'adresse_id',
                            how = 'left')
    del matched['adresse_id']
    matched = matched.merge(adresse, left_on='adresse_id_sign', right_on = 'adresse_id',
                            how = 'left',
                            suffixes=('_bien','_sign'),
                            )
    
    pd.crosstab(matched['bien_id_provenance_bien'], matched['valid'])
    pb = matched[~matched['valid']][['affaire_id',
        'adresse_id_bien', 'libelle_bien',
        'adresse_id_sign', 'libelle_sign',
         ]]
    
    ## => quand c'est matché, ça marche !!

def add_adresse_id(table, indicator=False):
    '''
       Extrait le maximum d'adresse_id en passant via bien_id ou via signalement
       indicator à la même fonction que dans la fonction merge de pandas
    '''
    assert 'affaire_id' in table.columns
    #Dans Sarah on a deux sources d'adresses: bien_id ou signalement_id 
    #37% des affaires sont matchées avec bien_id
    #71% des affaires sont matchées avec signalement
    
    #merge table avec adresses venant de bien_id
    table_bien_id = adresse_via_bien_id(table)
    assert table_bien_id['affaire_id'].value_counts().max() == 1    
    
    #merge table avec adresses venant de signalement
    table_signalement = adresses_via_signalement(table)
    table_signalement.drop(['signalement_id','merge_signalement'],
                           axis = 1,
                           inplace = True)
    assert table_signalement['affaire_id'].value_counts().max() == 1 
    
    var_to_merge_on = table.columns.tolist()
    table_bien_id = table_bien_id[var_to_merge_on + ['adresse_id']]
    table_signalement = table_signalement[var_to_merge_on + ['adresse_id']]
    table_adresses_max = table_bien_id.merge(table_signalement, 
                                on = var_to_merge_on,
                                how='outer',
                                suffixes=('_sign','_bien'),
                                indicator=indicator,
                                )
#    assert len(table_adresses_max) == len(table_bien_id)
    #table_adresses_max.adresse_id_bien.value_counts(dropna=False)
    ##=> NaN 30832, reste 21843
    #table_adresses_max.adresse_id_sign.value_counts(dropna=False)
    ##=> NaN 14339, reste 38336
    (table_adresses_max.adresse_id_bien == \
               table_adresses_max.adresse_id_sign).value_counts(dropna = False)
    ##=>False 40568, True 12107
    
    #Idée: Construire une colonne adresse_id avec adresse_id_sign quand ça 
    # existe et sinon adresse_id_bien
    table_adresses_max['adresse_id'] = table_adresses_max['adresse_id_sign']
    no_adresse_signalement = table_adresses_max['adresse_id_sign'].isnull()
    table_adresses_max.loc[no_adresse_signalement, 'adresse_id'] = \
            table_adresses_max.loc[no_adresse_signalement, 'adresse_id_bien' ] 

    #retire les doublons, en ne gardant
    table_adresses_max.drop_duplicates(inplace=True)
    table_adresses_max.groupby(var_to_merge_on)['adresse_id'].nunique()
    
    assert len(table_adresses_max) == len(table)
    return table_adresses_max
    
if __name__ == '__main__':
    hyg = read_table('affhygiene')[['affaire_id', 'bien_id']]
    adresse_par_affaires = adresse_max(hyg, indicator='origine')
    
    