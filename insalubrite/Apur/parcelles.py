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

    
    cols_type_logement = ['NB_LG_GRA', 'NB_LG_LOC', 'NB_LG_NIM', 'NB_LG_PRO',
                          'NB_LG_IMP', 'NB_LG_VAC', 'NB_LG_DIV', 'NB_LG_UTL',
                          'NB_LG_LPF']
    cols_nb_pieces = ['NB_PIEC_1', 'NB_PIEC_2', 'NB_PIEC_3', 'NB_PIE_4P',
                      'NB_PIANX']
    cols_taille_logements = ['NB_LG1_9', 'NB_LG1019', 'NB_LG2029',
                             'NB_LG3039', 'NB_LG4049', 'NB_LG5069',
                             'NB_LG7089', 'NB_LG_S90']
    
    restant = [x for x in tab.columns 
        if x not in cols_type_logement + cols_nb_pieces + cols_taille_logements
        ]


    assert all(tab[cols_taille_logements].sum(axis=1) == tab['NB_LG'])

    pb_nb_pieces = tab.loc[tab[cols_nb_pieces].sum(axis=1) != tab['NB_LG']]
    diff = pb_nb_pieces['NB_LG'] - pb_nb_pieces[cols_nb_pieces].sum(axis=1)
    (diff/pb_nb_pieces['NB_LG']).describe()
    # beaucoup de 1
    
    pb_type = tab[tab[cols_type_logement].sum(axis=1) != tab['NB_LG']]
    diff = pb_type['NB_LG'] - pb_type[cols_type_logement].sum(axis=1)
    (pb_type['NB_LG'] - pb_type[cols_type_logement].sum(axis=1)).value_counts()
    (diff/pb_type['NB_LG']).describe()
    # beaucoup de 1 mais aussi des type non remplis
    
    annee = tab.loc[:,['AN_MIN', 'AN_MAX', 'AN_BATLG', 'AN_BATLOA', 'AN_BATSUR']]

    diff_annee = annee.add(-annee['AN_MIN'], axis=0).mask(annee == 0)
    une_seule_annnee = annee[annee['AN_MAX'] == annee['AN_MIN']]