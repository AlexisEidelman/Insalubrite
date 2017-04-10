# -*- coding: utf-8 -*-
"""
Relie l'affaire à l'adresse

"""
import os
import pandas as pd

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table

from insalubrite.match_to_ban import merge_df_to_ban


def adresse_par_affaires(table):
    ''' retrounes la table avec l'adresse correspondant à chaque affaire
        repérée par affaire_id
        en éliminant les affaire_id qui ne sont pas dans signalement_affaire
    '''
    assert 'affaire_id' in table.columns
        
    # étape 1 : signalement affaire
    signalement_affaire = read_table('signalement_affaire')
    table_signalement_affaire = pd.merge(table, signalement_affaire, on='affaire_id')
    #    len(table.affaire_id) # => 37322
    #    len(lien_signalement_affaire.affaire_id) ## => 30871
    #    len(result1.affaire_id) ## => 30692
    
    # étape 2 : signalement
    signalement = read_table('signalement')
    signalement = signalement[['id', 'observations', 'adresse_id']]
    ##Rename 'id' column of signalement table
    signalement.rename(columns = {'id':'signalement_id'}, inplace = True)
    table_signalement = pd.merge(table_signalement_affaire, signalement, 
                                 on='signalement_id')
    #    len(table_signalement_affaire.signalement_id) ## => 30692
    #    len(signalement.signalement_id) ## => 36080
    #    len(table_signalement.signalement_id) ## => 30692
    
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
        
       
        arrondissement = read_table('arrondissement')
        arrond = arrondissement[['id', 'codeinsee', 'codepostal', 'nomcommune']]
        arrond.rename(columns = {'id':'nsq_ca'}, inplace = True)
    
        quartier_admin = read_table('quartier_admin')

        arrond_quartier = pd.merge(arrond, quartier_admin, on = 'nsq_ca')        
        arrond_quartier.rename(columns = {'nsq_qu':'nqu'}, inplace = True)
        
        ilot = read_table('ilot')
        ilot.rename(columns = {'nsq_ia':'ilot_id'}, inplace = True)
        arrond_quartier_ilot = pd.merge(arrond_quartier, ilot, on = 'nqu')
        

        parcelle_cadastrale = read_table('parcelle_cadastrale')
        parcelle_cadastrale.rename(columns = {'id':'parcelle_id'}, inplace = True)
        parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id',\
                                                   'code_cadastre']]
        #Merge parcelle_cadastrale with ilot on ilot_id
        arrond_quartier_ilot = pd.merge(arrond_quartier_ilot, 
                                        parcelle_cadastrale, on = 'ilot_id')
        
        adrbad = pd.merge(adrbad_voie, arrond_quartier_ilot, on='parcelle_id')

        return adrbad


    adrbad = adrbad_complet()
    adrbad.drop(['libelle'], axis = 1, inplace = True)
    
    assert 'libelle' not in adrbad.columns
    adrbad['suffixe1'].fillna('', inplace=True)
    adrbad['libelle'] = adrbad['numero'].astype(str) + ' ' + \
        adrbad['suffixe1'] + ' ' + \
        adrbad['nom_typo'] + ', Paris'
    adrbad['libelle'] = adrbad['libelle'].str.replace('  ', ' ')    
    

    ## étape 3.2 : adrsimple    
    adrsimple = read_table('adrsimple')
    assert 'libelle' not in adrsimple.columns
    adrsimple['numero_adresse1'].fillna('', inplace=True)
    adrsimple['libelle'] = adrsimple['numero_adresse1'] + ' ' + adrsimple['libelle_adresse']
    adrsimple.rename(columns = {'codepostal_adresse':'codepostal'}, inplace = True)
    

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
    path_affaires = os.path.join(path_output, 'compterenduinsalubre.csv')
    compterenduinsalubre = pd.read_csv(path_affaires, encoding='utf8')
    adresses_final = adresse_par_affaires(compterenduinsalubre)
    path_csv_adressses = os.path.join(path_output, 'adresses_ban.csv')
    adresses_final.to_csv(path_csv_adressses, index=False, encoding='utf8')
    adresses_final.groupby(['affaire_id','date']).size()