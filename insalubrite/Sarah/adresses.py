# -*- coding: utf-8 -*-
"""
Fichiers utilisant toutes les travaux sur les adresses
associées à la base SARAH
"""

import pandas as pd
from insalubrite.Sarah.read import read_table


def parcelles():
    ''' travaille au niveau de la parcelle cadastrale et offre une
        table propre
        A l'issue de ce programme, les tables:
            - parcelle_cadastrale
            - ilot
            - quartier_admin
            - arrondissement
        n'ont plus de raison d'être appelée
    '''
    print(" On a :\n",
          " - 75105 parcelle cadastralle, dans\n",
          " - 5031 ilots, dans\n",
          " - 80 quarties administratifs, dans\n",
          " - 20 arrondissements, dans\n",
          )

    parcelle_cadastrale = read_table('parcelle_cadastrale')
    parcelle_cadastrale.rename(columns = {'id':'parcelle_id'}, inplace = True)
    # les variables non retenues n'ont pas d'intérêt
    parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id',\
                                               'code_cadastre']]

    ilot = read_table('ilot')
    ilot.rename(columns = {'nsq_ia':'ilot_id'}, inplace = True)
    parcelle_augmentee = parcelle_cadastrale.merge(ilot, on='ilot_id', how='left')

    quartier_admin = read_table('quartier_admin')
    quartier_admin = quartier_admin[['nsq_qu', 'tln', 'nsq_ca']]
    quartier_admin.rename(columns = {'nsq_qu':'nqu',
                                     'tln': 'quartier_admin'}, inplace = True)

    parcelle_augmentee = parcelle_augmentee.merge(
        quartier_admin, on='nqu', how='left')

    arrondissement = read_table('arrondissement')
    arrond = arrondissement[['id', 'codeinsee', 'codepostal', 'nomcommune']]
    arrond.rename(columns = {'id':'nsq_ca'}, inplace = True)
    parcelle_augmentee = parcelle_augmentee.merge(arrond, on='nsq_ca', how='left')

    # on renomme ilot_id pour ne pas chercher à fusionner encore par la suite
    parcelle_augmentee.drop(['nqu', 'nsq_ca'], axis=1, inplace=True)
    parcelle_augmentee['ilot_id'].fillna(-1, inplace=True)
    parcelle_augmentee['numero_ilot'] = parcelle_augmentee['ilot_id'].astype(int)
    del parcelle_augmentee['ilot_id']

    return parcelle_augmentee


def adresses():
    ''' unification des adresses de adrbad et adrsimple
        ajoute les parcelles
    '''
    # étape 3 : adrbad et adrsimple
    ## étape 3.1 : adrbad

    adrbad = read_table('adrbad')
    adrbad = adrbad[['adresse_id','parcelle_id','voie_id',
                 'numero', 'suffixe1', 'suffixe2', 'suffixe3']]
    adrbad['numero'] = adrbad['numero'].fillna(-1).astype(int).astype(str)
    adrbad['numero'].replace('-1', '', inplace=True)

    adrbad['suffixe1'].fillna('', inplace=True)
    adrbad['suffixe1'].replace(['b','t','q'], ['bis', 'ter', 'quater'], inplace=True)

    voie = read_table('voie')
    voie.rename(columns = {'id':'voie_id'}, inplace = True)
    voie = voie[['voie_id','nom_typo',
             # 'type_voie'
             ]]

    adrbad = pd.merge(voie, adrbad, on='voie_id', how='left')
    del adrbad['voie_id']

    adrbad['libelle'] = adrbad['numero'].astype(str) + ' ' + \
    adrbad['suffixe1'] + ' ' + \
    adrbad['nom_typo'] + ', Paris'
    adrbad['libelle'] = adrbad['libelle'].str.replace('  ', ' ')
    adrbad = adrbad[['adresse_id', 'parcelle_id', 'libelle']]

    ## étape 3.2 : adrsimple
    adrsimple = read_table('adrsimple')
    assert 'libelle' not in adrsimple.columns
    adrsimple['numero_adresse1'].fillna('', inplace=True)
    # Travail sur les bis, ter et autres
    adrsimple['numero_adresse2'] = adrsimple['numero_adresse2'].str.lower().str.strip()
    adrsimple['numero_adresse2'].replace(['b','t'], ['bis', 'ter'], inplace=True)
    adrsimple['bis_ou_ter'] = ''
    adrsimple.loc[adrsimple['numero_adresse2'].isin(['bis', 'ter']), 'bis_ou_ter'] = \
    adrsimple.loc[adrsimple['numero_adresse2'].isin(['bis', 'ter']), 'numero_adresse2']

    adrsimple['ville_adresse'] = adrsimple['ville_adresse'].str.lower()
    adrsimple['ville_adresse'].fillna('', inplace=True)
    ville_paris = adrsimple['ville_adresse'].str.startswith('paris ')
    adrsimple.loc[ville_paris, 'ville_adresse'] = 'paris'
    
    adrsimple['libelle_adresse'].fillna('', inplace=True)
    adrsimple['libelle_adresse'] = adrsimple['libelle_adresse'].str.split('PARIS-').str[0]  
    
    adrsimple['libelle'] = adrsimple['numero_adresse1'] + ' ' + \
    adrsimple['bis_ou_ter'] + ' ' + \
    adrsimple['libelle_adresse'].str.lower()  + ', ' + \
    adrsimple['ville_adresse'].str.lower()
    adrsimple['libelle'] = adrsimple['libelle'].str.replace('  ', ' ')

    adrsimple = adrsimple[['libelle', 'codepostal_adresse', 'parcelle_id',
                       'adresse_id']]

    ## étape 3.3 : rassemble adrbad et adrsimple
    adresse = adrbad.append(adrsimple)
    cadastre = parcelles()

    adresse = pd.merge(adresse, cadastre, on='parcelle_id', how='left')

    # adresse.rename(columns = {'codepostal_adresse':'codepostal'}, inplace = True)
    adresse.loc[adresse['codepostal'].isnull(), 'codepostal'] = \
    adresse.loc[adresse['codepostal'].isnull(), 'codepostal_adresse']
    del adresse['codepostal_adresse']

    table_adresse = read_table('adresse')
    adresse = table_adresse.merge(adresse, how='outer', indicator='test')
    adresse = adresse[adresse['adresse_id'].notnull()]
    assert all(adresse['test'] == 'both')
    del adresse['test']
    return adresse


if __name__ == "__main__":
    import os     
    from insalubrite.match_to_ban import merge_df_to_ban
    from insalubrite.config_insal import path_bspp, path_output

    tab = adresses()    
    tab_ban = merge_df_to_ban(tab[tab.codepostal.notnull()],
                             os.path.join(path_output, 'temp.csv'),
                             ['libelle', 'codepostal'],
                             name_postcode = 'codepostal')
                    