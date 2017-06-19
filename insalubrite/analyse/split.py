#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
On crée ici différentes façon de spliter un sujet

Il retourne comme train_test_split de sickit-learn
un quadruplet : X_train, X_test, y_train, y_test
"""

import pandas as pd
from sklearn.model_selection import train_test_split

def simple_split(tab, train_size=0.75, avec_remise=False):
    if not avec_remise:
        assert "adresse_ban_id" in tab
    X = tab.drop(['est_insalubre'], axis=1)
    Y = tab['est_insalubre']
    
    X_train, X_test, y_train, y_test = train_test_split(X, Y,
                                                        train_size=train_size,
                                                        )
    if not avec_remise:
        deja_vu = X_test.adresse_ban_id.isin(X_train.adresse_ban_id)
        print("on retire {0:.2f} % du train parce qu'ils sont dans le test".format(
            100*sum(deja_vu)/len(deja_vu)))
        print("La partie test représente {0:.2f} % de la table initiale".format(
            100*sum(~deja_vu)/len(tab)))
        X_test = X_test[~deja_vu]
        y_test = y_test[~deja_vu]
        
        del X_test['adresse_ban_id']
        del X_train['adresse_ban_id']  
    
    return X_train, X_test, y_train, y_test


def split_by_date(tab, serie_date, train_size=0.75, avec_remise=False):
    ''' sépare en entrainant sur les affaires les plus anciennnes
        
        Lorsque avec_remise est faux, on retire les affaires déjà
        visitées (repérées via adresse_ban_id)
        Attention, à ce moment là, le ration train size sur test size est plus
        grand que le paramètre train_size
        
        Remarque : on supprime la variable adresse_ban_id lorsqu'elle est présente
        '''
    assert isinstance(serie_date, str)
    assert serie_date in tab.columns
    if not avec_remise:
        assert "adresse_ban_id" in tab
    
    nb_train_rows = int(len(tab)*train_size)
    out_tab = tab.sort_values(serie_date)
    del out_tab[serie_date]
    
    train = out_tab.iloc[:nb_train_rows]
    test = out_tab.iloc[nb_train_rows:]
    print("On place dans le set d'apprentissage les visites qui ont eu lieu avant",
          tab[serie_date].iloc[nb_train_rows].date(),
            "et dans le jeu de test, celle qui ont eu lieu après")
    
    if not avec_remise:
        deja_vu = test.adresse_ban_id.isin(train.adresse_ban_id)
        print("on retire {0:.2f} % du train parce qu'ils sont dans le test".format(
            100*sum(deja_vu)/len(deja_vu)))
        print("La partie test représente {0:.2f} % de la table initiale".format(
            100*sum(~deja_vu)/len(tab)))
        test = test[~deja_vu]
        
        del test['adresse_ban_id']
        del train['adresse_ban_id']
        

    return (train.drop(['est_insalubre'], axis=1),
            test.drop(['est_insalubre'], axis=1),
            train['est_insalubre'],
            test['est_insalubre'],
            )
            
    

if __name__ == '__main__':    
    from data import get_data
    import preprocess
    
    tab_ini = get_data('batiment') 
    tab = preprocess.keep_cols_for_analysis(tab_ini)
    float_tab = preprocess.to_float(tab)
    float_tab.loc[:,'date'] = pd.to_datetime(tab_ini['date_creation'])

    float_tab["adresse_ban_id"] = tab_ini["adresse_ban_id"]
    a, b, c, d = split_by_date(float_tab, "date", avec_remise=False)
    a, b, c, d = simple_split(float_tab, avec_remise=False)


    
