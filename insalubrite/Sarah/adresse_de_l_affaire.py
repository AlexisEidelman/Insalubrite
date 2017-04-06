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
    table_signalement_affaire = pd.merge(table, signalement_affaire, on='affaire_id')
    #    len(table.affaire_id) # => 37322
    #    len(lien_signalement_affaire.affaire_id) ## => 30871
    #    len(result1.affaire_id) ## => 30692
    
    # étape 2 : signalement
    signalement = read_table('signalement')
    signalement = signalement[['id', 'observations', 'adresse_id']]
    ##Rename 'id' column of signalement table
    signalement['signalement_id'] = signalement['id']
    del signalement['id']
    table_signalement = pd.merge(table_signalement_affaire, signalement, on='signalement_id')
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
        voie['voie_id'] = voie['id']
        del voie['id']
        voie = voie[['voie_id','code_ville','libelle','nom_typo','type_voie']]
        adrbad_voie = pd.merge(voie, adrbad, on='voie_id')
        
       
        arrondissement = read_table('arrondissement')
        arrond = arrondissement[['id', 'codeinsee', 'codepostal', 'nomcommune']]
        arrond['nsq_ca'] = arrond['id']
        del arrond['id']
    
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
        parcelle_cadastrale = parcelle_cadastrale[['parcelle_id','ilot_id','code_cadastre']]
        #Merge parcelle_cadastrale with ilot on ilot_id
        arrond_quartier_ilot = pd.merge(arrond_quartier_ilot, parcelle_cadastrale, on = 'ilot_id')
        
        adrbad = pd.merge(adrbad_voie, arrond_quartier_ilot, on='parcelle_id')

        return adrbad


    adrbad = adrbad_complet()
    table_adrbad = pd.merge(table_signalement, adrbad, on='adresse_id')
    #    len(table_signalement) # => 30692
    #    len(adrbad)  # => 146306
    #    len(table_adrbad)  # => 30453
    

    ## étape 3.2 : adrsimple    
    # Les 239 qui ne sont pas matché avec adrbad sont matchés avec adrsimple
    adrsimple = read_table('adrsimple')
    table_adrsimple = pd.merge(table_signalement, adrsimple, on='adresse_id')
    len(table_signalement) # => 30692
    len(adrsimple)  # => 95461
    len(table_adrsimple)  # => 239


    ## étape 3.3 : rassemble adrbad et adrsimple
    # TODO: 


    ### sauvegarde les données qui concernent les adressses seules :
    adresses_final = table_adrbad[['codeinsee', 'codepostal', 'nomcommune',
                   'numero', 'suffixe1', 'nom_typo', 'affaire_id',
                   'code_cadastre']]
    adresses_final['suffixe1'].fillna('', inplace=True)
    
    adresses_final['libelle'] = adresses_final['numero'].astype(str) + ' ' + \
        adresses_final['suffixe1'] + ' ' + \
        adresses_final['nom_typo'] + ', Paris'
    adresses_final['libelle'] = adresses_final['libelle'].str.replace('  ', ' ')
    
    
    #pour adresse sarah
    adresses_final = merge_df_to_ban(
        adresses_final,
        os.path.join(path_output, 'temp.csv'),
        ['libelle', 'codepostal'],
        name_postcode = 'codepostal'
        )
    
    return adresses_final


if __name__ == '__main__':
    ### éude des adresses
    adresse = read_table('adresse')
    # est-ce que les adresses sont ou bien dans adrbad ou bien dans adrsimple ?
    # vu la tête de adresse on peut supposer que oui
    adrsimple = read_table('adrsimple')
    
    
    # Merge tables
    affhygiene = read_table('affhygiene')
        
    adresses_final = adresse_par_affaires(affhygiene)
    path_csv_adressses = os.path.join(path_output, 'adresses_ban.csv')
    adresses_final.to_csv(path_csv_adressses, index=False, encoding='utf8')
