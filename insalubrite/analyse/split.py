#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
On crée ici différentes façon de spliter un sujet

Il retourne comme train_test_split de sickit-learn
un quadruplet : X_train, X_test, y_train, y_test
"""

import pandas as pd
from sklearn.model_selection import train_test_split

def simple_split(tab, train_size=0.75):
    X = tab.drop(['est_insalubre'], axis=1)
    Y = tab['est_insalubre']
    X_train, X_test, y_train, y_test = train_test_split(X, Y,
                                                        train_size=train_size,
                                                        )

def split_by_date(tab, serie_date, train_size=0.75):
    assert isinstance(serie_date, str)
    assert serie_date in tab.columns
    
    nb_train_rows = int(len(tab)*train_size)
    out_tab = tab.sort_values(serie_date)
    del out_tab[serie_date]
    
    train = out_tab.iloc[:nb_train_rows]
    test = out_tab.iloc[nb_train_rows:]
    print("On place dans le set d'apprentissage les visites qui ont eu lieu avant",
          tab[serie_date].iloc[nb_train_rows].date(),
            "et dans le jeu de test, celle qui ont eu lieu après")
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
    a, b, c, d = split_by_date(float_tab, "date")
