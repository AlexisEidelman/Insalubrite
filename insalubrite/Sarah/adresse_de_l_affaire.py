# -*- coding: utf-8 -*-
"""
Relie l'affaire à l'adresse

Au depart, on passait par le signalement de l'affaire qui était
relié à une adresse.
Problème : beaucoup d'affaires n'ont pas de signalement.


Finalement, la comprhénsion de la variable bien_id directement
reliée à affygiène fait prendre une autre piste plus précise et
plus compléte puisqu'on n'a plus le problème de signalement

"""
import os
import pandas as pd

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table

from insalubrite.match_to_ban import merge_df_to_ban


def signalement_des_affaires(table,
                             liste_var_signalement=None):
    '''  '''


    signalement_affaire = read_table('signalement_affaire')
    if 'signalement_id' in table.columns:
        table.rename(columns = {'signalement_id':'signalement_id_orig'},
                                inplace = True)
    table_signalement_affaire = pd.merge(table, signalement_affaire,
                                         on='affaire_id',
                                         how='left',
                                         indicator='merge_signalement')
    #    len(table.affaire_id) # => 37322
    #    len(lien_signalement_affaire.affaire_id) ## => 30871
    #    len(result1.affaire_id) ## => 30692

    # étape 2 : signalement
    signalement = read_table('signalement')
    var_to_keep = ['id', 'adresse_id']
    if liste_var_signalement is not None:
        var_to_keep += liste_var_signalement

    signalement = signalement[var_to_keep]
    ##Rename 'id' column of signalement table
    signalement.rename(columns = {'id':'signalement_id'}, inplace = True)
    table_signalement = pd.merge(table_signalement_affaire, signalement,
                                 on='signalement_id',
                                 how='left')
    return table_signalement


def parcelle():
    ''' travaille au niveau de la parcelle cadastrale et offre une
        table propre
    '''
    print("75105 parcelle cadastralle répartis en 5031 ilots \n",
          "eux-même répartis en 80 quarties administratifs \n",
          "eux-même répartis en 20 arrondissements \n")


    parcelle_cadastrale = read_table('parcelle_cadastrale')
    parcelle_cadastrale.rename(columns = {'id':'parcelle_id'}, inplace = True)
    # les variables non retenues n'ont pas d'intérêt
    parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id',\
                                               'code_cadastre']]

    ilot = read_table('ilot')
    ilot.rename(columns = {'nsq_ia':'ilot_id'}, inplace = True)
    parcelle_augmentee = parcelle_cadastrale.merge(ilot, on = 'ilot_id')


    quartier_admin = read_table('quartier_admin')
    quartier_admin = quartier_admin[['nsq_qu', 'tln', 'nsq_ca']]
    quartier_admin.rename(columns = {'nsq_qu':'nqu',
                                     'tln': 'quartier_admin'}, inplace = True)

    parcelle_augmentee = parcelle_augmentee.merge(quartier_admin, on='nqu')


    arrondissement = read_table('arrondissement')
    arrond = arrondissement[['id', 'codeinsee', 'codepostal', 'nomcommune']]
    arrond.rename(columns = {'id':'nsq_ca'}, inplace = True)
    parcelle_augmentee = parcelle_augmentee.merge(arrond, on = 'nsq_ca')


    parcelle_augmentee.drop(['nqu', 'nsq_ca'], axis=1, inplace=True)
    parcelle_augmentee['ilot_id'] = parcelle_augmentee['ilot_id'].astype(int)

    return parcelle_augmentee


def adresse_par_affaires(table, liste_var_signalement=None):
    ''' retrounes la table avec l'adresse correspondant à chaque affaire
        repérée par affaire_id
        en éliminant les affaire_id qui ne sont pas dans signalement_affaire
    '''
    assert 'affaire_id' in table.columns

    # étape 1 et 2: signalement affaire
    table_signalement = signalement_des_affaires(table, liste_var_signalement)

    # étape 3 : adrbad et adrsimple
    ## étape 3.1 : adrbad
    def adrbad_complet():
        adrbad = read_table('adrbad')
        adrbad = adrbad[['adresse_id','parcelle_id','voie_id',
                         'numero', 'suffixe1', 'suffixe2', 'suffixe3']]

        # parenthèse: travail sur adrbad
        voie = read_table('voie')
        voie.rename(columns = {'id':'voie_id'}, inplace = True)
        voie = voie[['voie_id','code_ville','libelle','nom_typo','type_voie']]
        adrbad_voie = pd.merge(voie, adrbad, on='voie_id')

        arrond_quartier_ilot = parcelle()

        adrbad = pd.merge(adrbad_voie, arrond_quartier_ilot, on='parcelle_id')

        return adrbad


    adrbad = adrbad_complet()
    adrbad.drop(['libelle'], axis = 1, inplace = True)

    assert 'libelle' not in adrbad.columns
    adrbad['suffixe1'].fillna('', inplace=True)
    adrbad['suffixe1'].replace(['b','t','q'], ['bis', 'ter', 'quater'], inplace=True)

    adrbad['libelle'] = adrbad['numero'].astype(str) + ' ' + \
        adrbad['suffixe1'] + ' ' + \
        adrbad['nom_typo'] + ', Paris'
    adrbad['libelle'] = adrbad['libelle'].str.replace('  ', ' ')


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

    adrsimple['libelle'] = adrsimple['numero_adresse1'] + ' ' + \
        adrsimple['bis_ou_ter'] + ' ' + \
        adrsimple['libelle_adresse']

    adrsimple['libelle'] = adrsimple['libelle'].str.replace('  ', ' ')
    adrsimple.rename(columns = {'codepostal_adresse':'codepostal'}, inplace = True)

    # TODO: utiliser parcelle_id
    adrsimple['code_cadastre'] = 'inconnu_car_source_adrsimple'

    ## étape 3.3 : rassemble adrbad et adrsimple
    adresse = adrbad.append(adrsimple)

    ## étape 3.4 : fusionne table et adresse
    if 'libelle' in table_signalement.columns:
        table_signalement.rename(columns = {'libelle':'libelle_table'}, inplace = True)
    table_adresses = table_signalement.merge(adresse[['adresse_id', 'libelle',
                                                      'codepostal', 'code_cadastre']],
                                             on='adresse_id',
                                             how = 'left')
    len(table_signalement) # => 38534
    len(adresse)  # => 241767
    len(table_adresses)  # => 38534

    ## étape 3.5 : envoie à l'API
    print("appel à l'api de  adresse.data.gouv.fr, cette opération peut prendre du temps")
    table_ban = merge_df_to_ban(
        table_adresses,
        os.path.join(path_output, 'temp.csv'),
        ['libelle', 'codepostal'],
        name_postcode = 'codepostal'
        )

    table_ban.rename(columns = {
        'result_label':'adresse_ban',
        'result_score': 'score_matching_adresse',
        'result_id': 'id_adresse'
        },
        inplace = True)

    return table_ban


if __name__ == '__main__':
    ### éude des adresses
    #adresse = read_table('adresse')
    # les adresses sont ou bien dans adrbad ou bien dans adrsimple

    # Merge tables
    path_affaires = os.path.join(path_output, 'compterenduinsalubre_v0.csv')
    compterenduinsalubre = pd.read_csv(path_affaires, encoding='utf8')

    adresses_final = adresse_par_affaires(compterenduinsalubre)