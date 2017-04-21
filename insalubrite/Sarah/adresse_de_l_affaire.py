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

def adresse_max(table):
    '''
       Extrait le maximum d'adresse_id en passant via bien_id ou via signalement
    '''
    assert 'affaire_id' in table.columns
    #Dans Sarah on a deux sources d'adresses: bien_id ou signalement_id 
    #37% des affaires sont matchées avec bien_id
    #71% des affaires sont matchées avec signalement
    
    #merge table avec adresses venant de bien_id
    table_bien_id = adresse_via_bien_id(table)
    table_bien_id = table_bien_id.groupby('affaire_id').first().reset_index()
    
    #merge table avec adresses venant de signalement
    table_signalement = adresses_via_signalement(table)
    table_signalement = table_signalement.groupby('affaire_id').first().reset_index()
    
    #Compléter table_signalement avec table_bien_id
    adresse_via_bien_id = table_signalement.adresse_id.isnull()
    table_signalement.loc[adresse_via_bien_id, 'adresse_id'] = \
         table_bien_id.loc[adresse_via_bien_id, 'adresse_id']
    
    
    return table_signalement
    
if __name__ == '__main__':
    hyg = read_table('affhygiene')
    adresse_par_affaires = adresse_max(hyg)
    
    