# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:15:44 2017

"""
import os
import pandas as pd

from insalubrite.config_insal import path_output
from insalubrite.Sarah.read import read_table

from insalubrite.match_to_ban import merge_df_to_ban


def adresse_par_affaires(table):
    ''' retrounes la table avec l'adresse correspondant à chaque affaire
        repérée par affaire_id
    '''
    assert 'affaire_id' in table.columns
        
    # étape 1 : signalement affaire
    signalement_affaire = read_table('signalement_affaire')
    table_signalement_affaire = pd.merge(table, signalement_affaire, 
                                         on='affaire_id')
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
        arrond_quartier['nqu'] = arrond_quartier['nsq_qu']
        del arrond_quartier['nsq_qu']
        
        ilot = read_table('ilot')
        ilot['ilot_id'] = ilot['nsq_ia']
        del ilot['nsq_ia']
        arrond_quartier_ilot = pd.merge(arrond_quartier, ilot, on = 'nqu')
        

        parcelle_cadastrale = read_table('parcelle_cadastrale')
        parcelle_cadastrale['parcelle_id'] = parcelle_cadastrale['id']
        del parcelle_cadastrale['id']
        parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id',\
                                                   'code_cadastre']]
        #Merge parcelle_cadastrale with ilot on ilot_id
        arrond_quartier_ilot = pd.merge(arrond_quartier_ilot, 
                                        parcelle_cadastrale, on = 'ilot_id')
        
        adrbad = pd.merge(adrbad_voie, arrond_quartier_ilot, on='parcelle_id')

        return adrbad


    adrbad = adrbad_complet()
    
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
    
    
    
    ## étape 3.4 : fusionne
    
    table_adrbad = pd.merge(table_signalement, adrbad, on='adresse_id')
    #    len(table_signalement) # => 30692
    #    len(adrbad)  # => 146306
    #    len(table_adrbad)  # => 30453
    
    

    # Les 239 qui ne sont pas matché avec adrbad sont matchés avec adrsimple
    table_adrsimple = pd.merge(table_signalement, adrsimple, on='adresse_id')
    #    len(table_signalement) # => 30692
    #    len(adrsimple)  # => 95461
    #    len(table_adrsimple)  # => 239

    
    

    ### sauvegarde les données qui concernent les adressses seules :
    
    
    #pour adresse bad
    adresses_bad_final = merge_df_to_ban(
        table_adrbad,
        os.path.join(path_output, 'temp.csv'),
        ['libelle', 'codepostal'],
        name_postcode = 'codepostal'
        )
    #pour adresse simple
    adresses_simple_final = merge_df_to_ban(
    table_adrsimple,
    os.path.join(path_output, 'temp_simple.csv'),
    ['libelle', 'codepostal_adresse'],
    name_postcode = 'codepostal_adresse'
    )
    
    adresses_bad_final = adresses_bad_final[['affaire_id', 'date', \
                                  'infractiontype_id','titre','result_label']]
    adresses_simple_final = adresses_simple_final[['affaire_id','date',\
                                  'infractiontype_id','titre','result_label']]
    adresses_final = pd.concat([adresses_bad_final, adresses_simple_final])
    return adresses_final


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
