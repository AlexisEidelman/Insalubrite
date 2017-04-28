# -*- coding: utf-8 -*-
"""
A expertiser
"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur


def _create_ASP(tab):
    ASP1 = tab['codeinsee'].astype(str).str[3:5].str.zfill(3)
    ASP2 = tab['C_SEC']
    ASP3 = tab['N_PC'].astype(str).str.zfill(4)
    return ASP1 + '-' + ASP2 + '-' + ASP3


def read_parcelle(yr):
    year = str(yr)
    path_apur_year = path_apur + year

    # parcelle cadastralle
    path_file_excel = os.path.join(path_apur_year,
                            '00 PARCELLE_CADASTRALE_STAT_' + year + '.xlsx'
                            )
    path_file_csv = path_file_excel[:-5] + '.csv'

    if os.path.exists(path_file_csv):
        return pd.read_csv(path_file_csv)

    else:
        tab = pd.read_excel(path_file_excel)

        tab.rename(columns={'C_CAINSEE': 'codeinsee'}, inplace=True)
        # TODO: attention, N_SQ_PC le numéro séquentiel est
        # déterminé par année avec réaffectation des numéros d'une année
        # sur l'autre. C'est très piège, on retire tout de suite.
        tab.drop(['N_SQ_PC', 'N_SQ_PD', 'OBJECTID', 'B_GRAPH'], axis=1,
                      inplace=True)

        # C_PDNIV0, C_PDNIV1 et C_PDNIV2, correspondent au libellé
        #                  L_PDNIV0, L_PDNIV1 et L_PDNIV2
        # ces éléments sont imbriqués, les niveaux deux correspondent à un groupe
        #                  de niveau 1 qui correspond à un groupe de niveau 0
        tab.drop(['C_PDNIV0', 'C_PDNIV1', 'C_PDNIV2'], axis=1,
                      inplace=True)


        cols_logement_activite = [x for x in tab.columns if 'NB_LA_' in x]
        # en fait on a NB_LOCACT qui est mieux rempli que le NB_LA
        # parcelles['NB_LA'] = parcelles[cols_logement_activite].sum(axis=1)
        tab.drop(cols_logement_activite, axis=1, inplace=True)

        tab['ASP'] = _create_ASP(tab)
        tab.drop(['C_SEC', 'N_PC', 'codeinsee'], axis=1, inplace=True)

        tab.to_csv(path_file_csv, index=False, encoding='utf8')
    return read_parcelle(yr) # pour lire le csv


def test_identifiant(tab):
    # est-ce utile ?
    for col in tab.columns:
        try:
            a = tab[col].astype(float)
        except:
            serie = tab[col]
            if serie.nunique() < 5:
                print(col, '\n')
                print(serie.value_counts())
            else:
                if len(serie) == serie.nunique():
                    print('cette variable est un identifiant ', col)

if __name__ == '__main__':
    tab = read_parcelle(2015)
    logement = [x for x in tab.columns if 'NB_LG' in x]
    piece = [x for x in tab.columns if 'NB_PIEC' in x]
    nb_la = [x for x in tab.columns if 'NB_LA' in x]
    restant = [x for x in tab.columns if x not in logement + piece + nb_la]

#
## geoloc des parcelles
#tab_file = os.path.join(path, '00 SRU2014 avec geoloc.xls')
#geoloc = pd.read_excel(tab_file)
#var_communes = [x for x in tab.columns if x in geoloc.columns]
#
## eau de paris
#tab_file = os.path.join(path, '10 Eau-de-Paris_APUR 2014.xlsx')
#eau = pd.read_excel(tab_file)
#
## mise en demeure
#tab_file = os.path.join(path, '30 mises en demeure STH 2014.xls')
#demeure = pd.read_excel(tab_file)
## parse la localisaion
#local = demeure[u'Localisation        ']
## le premier terme c'est la caractéristique
## à la fin, après ":, ", c'est les adresses
#type_loc = local.str.split(' :').str[0]
#type_loc.value_counts()
#type_of_location = type_loc.unique().tolist()
#
#adresses = local.str.split(':,').str[-1]
