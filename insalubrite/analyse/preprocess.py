# -*- coding: utf-8 -*-
"""
Pour avoir de belles variables
"""

import pandas as pd

def keep_cols_for_analysis(tab, choix_niveau_proprietaire = 'L_PDNIV0'):
    niveaux_prorietaire = ['L_PD', 'L_PDNIV0', 'L_PDNIV1', 'L_PDNIV2']
    assert choix_niveau_proprietaire in niveaux_prorietaire
    niveaux_prorietaire.remove(choix_niveau_proprietaire)
             
    out = tab.drop(['adresse_ban_id', 'affaire_id', "codeinsee",
                    'titre', 'code_cadastre',
                    'date_creation',
                    ] + 
                    niveaux_prorietaire
                    ,
                    axis=1)                           
    return out


def preprocess_data(tab):
    le = LabelEncoder()
    list_encoded = list()
    for name, col in tab.select_dtypes(['object']).iteritems():
        print(name)
        list_encoded.append(name)
        tab['l_' + name] = le.fit_transform(col)

    tab.drop(list_encoded, axis=1, inplace=True)
    return tab



def to_float(tab):
    ''' on transforme les donnée objetcs en données float en passant via 
        dummy
    '''
    categorielles = tab.select_dtypes(['object', 'category'])
    for col in categorielles:
        assert categorielles[col].nunique() < 20
        
    categorielles = pd.get_dummies(categorielles, drop_first=False)
    
    for name, col in tab.select_dtypes(['bool']).iteritems():
        tab.loc[:,name] = 1*tab[name]
    
    
    return tab.select_dtypes(['float', 'bool']).join(categorielles)


def to_qualitative(tab, nombre_de_division = 4):
    ''' on transforme les donnée objetcs en données float en passant via 
        dummy
    '''
   
    for name, col in tab.select_dtypes(['bool']).iteritems():
        tab.loc[:,name] = tab[name].astype('str')
    
    numeriques = tab.select_dtypes(['float'])
    for col in numeriques:
        numeriques.loc[:,col] = pd.qcut(numeriques.loc[:,col],
                                 nombre_de_division,
                                 duplicates='drop') 
    
    out = tab.select_dtypes(exclude=['float']).join(numeriques)
    assert out.shape == tab.shape    
    return out

if __name__ == '__main__':
    from data import get_data
    
    tab_ini = get_data('batiment')
    tab = keep_cols_for_analysis(tab_ini)
    float_tab = to_float(tab)
    quali_tab = to_qualitative(tab, 4)
    
    quali_in_numerique = to_float(quali_tab)
    