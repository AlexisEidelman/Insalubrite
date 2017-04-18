# -*- coding: utf-8 -*-
"""
voir adresse_de_l_affaire

Au depart, on passait par le signalement de l'affaire qui était
relié à une adresse.
Problème : beaucoup d'affaires n'ont pas de signalement.


Finalement, la comprhénsion de la variable bien_id directement
reliée à affygiène fait prendre une autre piste plus précise et
plus compléte puisqu'on n'a plus le problème de signalement

(voir question pour confirmation)

"""

import os
import numpy as np

from insalubrite.Sarah.read import read_table
from insalubrite.Sarah.adresses import parcelles, adresses
from insalubrite.config_insal import path_output


def adresse_via_bien_id(table):
    '''
    Trouve l'adresse d'une affaire en utilisant le bien_id
    Cet identifiant renvoie à l'une des quatre tables suivante :
        - localhabite 
        - batiment
        - immeuble
        - parcelle_cadastrale
        
    La fonction utilise une table contenant une variable bien_id
    et
    retrounes la table avec l'adresse_id correspondant à chaque affaire
    '''
    
    assert 'bien_id' in table.columns
    bien_ids = table['bien_id']
    
    # autre hypothèse bien_id se retrouve dans plusieurs tables !
    localhabite = read_table('localhabite')
    batiment = read_table('batiment')
    immeuble = read_table('immeuble')
    parcelle_cadastrale = read_table('parcelle_cadastrale')
    
    table['bien_id_provenance'] = ''
    for origine_possible in ['localhabite', 'batiment', 'immeuble', 'parcelle_cadastrale']:
        cond = bien_ids.isin(eval(origine_possible).id)
        table.loc[cond, 'bien_id_provenance'] = origine_possible
    print('origine de bien_id \n',
          table['bien_id_provenance'].value_counts(), "\n")
    
    ##### Travail sur les info des differentes tables
    
    del localhabite['codification'] #inutile et incomplet
    # TODO: on pourrait travailler sur l'étage
    
    # en passant
    batiment['designation'] = batiment['designation'].str.lower().str.strip()
    batiment['designation'] = batiment['designation'].str.replace('timent', 't')
    batiment['designation'] = batiment['designation'].str.replace('bat', 'bât')
    batiment['designation'] = batiment['designation'].str.replace('bät', 'bât')
    batiment['designation'] = batiment['designation'].str.replace('bât.', 'bât ')
    del batiment['digicode']  # inutile
    
    immeuble = immeuble.loc[:, immeuble.notnull().sum() > 1] # retire les colonnes vides
    # une étude colonne par colonne
    del immeuble['champprocedure'] # tous vrais sauf 10
    del immeuble['demoli'] # tous vrais sauf 1
    que_des_2 = ['diagplomb', 'diagtermite', 'etudemql', 'grilleanah',
                 'rapportpreop', 'risquesaturn', 'signalementprefecturepolice',
                 ]
    immeuble.drop(que_des_2, axis=1, inplace=True)
    del immeuble['tournee_id'] # que 8 valeurs
    
    parcelle_cadastrale = parcelles()
    
    
    ### Travail sur les id et les fusions
    print("Un local habité est dans un bâtiment\n",
          "Un bâtiment est dans un immeuble\n",
          "Un immeuble est dans une parcelle\n")
    
    # on va chercher d'abord pour chaque table les info annexes
    # parcelle_cadastrale ok grace à parcelle()
    
    # on a pas mal d'adresse principale vide ?!
    immeuble.rename(columns={'adresseprincipale_id': 'adresse_id'}, inplace=True)
    #=> on s'arrête là car toute la gestion des adresses est faire ailleurs
    # voir adresse_de_l_affaire
    
    entree = read_table('modentbat')[['id','libelle']]
    entree.columns = ['modentbat_id', 'mode_entree_batiment']
    batiment = batiment.merge(entree, how='left')
    del batiment['modentbat_id']
    
    hautfacade = read_table('hautfacade')[['id','libelle']]
    hautfacade.columns = ['hautfacade_id', 'hauteur_facade']
    batiment = batiment.merge(hautfacade, how='left')
    del batiment['hautfacade_id']
    
    
    table['localhabite_id'] = table['bien_id']*(table['bien_id_provenance'] == 'localhabite')
    table.replace(0, np.nan, inplace=True)
    localhabite.rename(columns={'id': 'localhabite_id'}, inplace=True)
    table = table.merge(localhabite, on = 'localhabite_id', how='left')
    
    bien_id_batiment = table['bien_id_provenance'] == 'batiment'
    table.loc[bien_id_batiment, 'batiment_id'] = table.loc[bien_id_batiment, 'bien_id']
    batiment.rename(columns={'id': 'batiment_id'}, inplace=True)
    table = table.merge(batiment, on = 'batiment_id', how='left')
    
    bien_id_immeuble = table['bien_id_provenance'] == 'immeuble'
    table.loc[bien_id_immeuble, 'immeuble_id'] = table.loc[bien_id_immeuble, 'bien_id']
    immeuble.rename(columns={'id': 'immeuble_id'}, inplace=True)
    table = table.merge(immeuble, on = 'immeuble_id', how='left')
    
    bien_id_parcelle = table['bien_id_provenance'] == 'parcelle_cadastrale'
    table.loc[bien_id_parcelle, 'parcelle_id'] = table.loc[bien_id_parcelle, 'bien_id']
    table = table.merge(parcelle_cadastrale, on = 'parcelle_id', how='left')
    
    table.rename(
        columns={
        'observations_x': 'observations_localhabite',
        'observations_y': 'observations_batiment',
        'observations': 'observations_immeuble',
        },
        inplace=True)


    return table



if __name__ == '__main__':

    hyg = read_table('affhygiene')
    hyg = adresse_via_bien_id(hyg)
    
    ### dans immeuble il y a parcelle et adresse.
    # or adresse est lié à parcelle, il faut vérifier la cohérence
    adresse = adresses()[['adresse_id', 'parcelle_id']]
    test = hyg[hyg.adresse_id.notnull()].merge(adresse, on='adresse_id', how='left', indicator=True)
    sum(test['parcelle_id_x'] != test['parcelle_id_y'])
    # 1 seule erreur
    
    path_adresses = os.path.join(path_output, 'adresses.csv')
    hyg.to_csv(path_adresses)