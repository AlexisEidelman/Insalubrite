# -*- coding: utf-8 -*-
"""
Pour avoir de belles variables
"""
import pandas as pd
from sklearn.preprocessing import LabelEncoder

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


def encode(tab, drop_old = True):
    ''' on transforme les donnée objects en                 
        
        drop_old = True pour supprimer les anciennes variables
        
        renvoie de nouvelles préfixée par l_ + ancien nom avec dtype = int64
        
    '''    
    le = LabelEncoder()
    list_encoded = list()
    out = tab.copy()
    for name, col in out.select_dtypes(exclude=['int', 'float']).iteritems():
        list_encoded.append(name)
        out['l_' + name] = le.fit_transform(col)

    if drop_old:
        out.drop(list_encoded, axis=1, inplace=True)
    return out



def to_float(tab):
    ''' on transforme les donnée objects en données float en passant via 
        dummy
        
        renvoie des colonnes avec dtype = uint8
        
    '''
    categorielles = tab.select_dtypes(['object', 'category'])
    for col in categorielles:
        assert categorielles[col].nunique() < 20
        
    categorielles = pd.get_dummies(categorielles, drop_first=False)
    
    for name, col in tab.select_dtypes(['bool']).iteritems():
        tab.loc[:,name] = 1*tab[name].astype(float)
    
    return tab.select_dtypes(['float', 'bool']).join(categorielles)


def to_qualitative(tab, nombre_de_division = 4):
    ''' on transforme les donnée float en données object ou catégories
        - si on prend peu de valeurs (20), on transforme en string
        - sinon, on utilise une répartition en quantile
    
        renvoie des colonnes avec dtype = object ou category        
        
    '''
   
    for name, col in tab.select_dtypes(['bool']).iteritems():
        tab.loc[:,name] = tab[name].astype('str')
    
    numeriques = tab.select_dtypes(['float'])
    for col in numeriques:
        if numeriques[col].nunique() < 20: # on pourrait mettre len(numeriques)/100 par exemple 
            numeriques.loc[:,col] = numeriques[col].astype('object')
        else:
            numeriques.loc[:,col] = pd.qcut(numeriques.loc[:,col].rank(method='first'),
                                     nombre_de_division,
#                                     duplicates='drop'
                                     ) 
    
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
    
    encoded = encode(quali_tab)
    print(quali_tab.dtypes)