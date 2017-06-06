#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
On crée ici différentes façon de spliter un sujet
"""

from sklearn.model_selection import train_test_split

def simple_split(tab, train_size=0.75):
    X = tab.drop(['est_insalubre'], axis=1)
    Y = tab['est_insalubre']
    X_train, X_test, y_train, y_test = train_test_split(X, Y,
                                                        train_size=train_size,
                                                        )

