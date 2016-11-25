# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 11:20:56 2015

@author: Alexis Eidelman
"""

import numpy as np
import pandas as pd
import os
import datetime


pd.set_option('display.max_rows', 5)
pd.reset_option('precision')
pd.set_option('max_colwidth',80)
pd.set_option('display.width', 100)

tabs = {}

## Lecture
deb_path = "D:\\data\\Habitation_Paris\\S29_ISAGEO1E_"
list_tabnames = ["Dossiers-Avec-Comp", "Dossiers-Renseignement", "Dossiers-Sans-Comp",
                 "Dossiers-Signalement", "Dossiers",
                 "Sageo-Immeubles",
                 "Tiers", "Biens"]
date = "2015-07-20_23_03"

for tabname in list_tabnames:
    tabs[tabname] = pd.read_csv(deb_path + tabname + "_" + date + ".csv",
    sep=';', encoding='cp1252')


## Test sur les IDENT des différentes tables
ident_tab_dossiers = set(tabs['Dossiers']['Numero_dossier'].tolist())
check_all_ident_are_in_other_table = set(ident_tab_dossiers)

for tabname in list_tabnames:
    tab = tabs[tabname]
    if tabname == "Sageo-Immeubles":
        continue
    print tabname
    print 1, len(tab)
    list_ident = tab['Numero_dossier'].tolist()
    set_ident = set(list_ident)
    print 2, len(ident_tab_dossiers - set_ident)
    assert len(set_ident - ident_tab_dossiers) == 0
    if tabname != "Dossiers":
        print 3,  len(set_ident - check_all_ident_are_in_other_table), len(set_ident)
        check_all_ident_are_in_other_table = check_all_ident_are_in_other_table - set_ident
        print 4, len(check_all_ident_are_in_other_table)

## test sur la table dossier
tab = tabs['Dossiers']
date_creation = tab[u'Date_creation']
tab = tab[date_creation.notnull()]
date = pd.DatetimeIndex(tab[u'Date_creation'])
tab.groupby([date.year, date.month]).size().plot(kind="bar")

for val in tab[u'Date_creation']:
    try:
        datetime.datetime.strptime(val, '%Y-%m-%d')
    except:
        if np.isnan(val):
            pass
        else:
            import pdb
            print val
            pdb.set_trace()

## séléction des années récentes
tab_recente = tab[date >= '2011']