# -*- coding: utf-8 -*-
"""

Rassemble les données générées dans la création de base
pour avoir des données sur lesquelles produire des statistiques et autres infos.

Pour être clair: on prend ici les données sarah_adresse, niveau_parcelles et
niveau_adresses comme données. On ne les modifie pas.

"""


def build_output(tab, name_output = 'output', libre_est_insalubre = True,
                niveau_de_gravite = False):
    ''' crée plusieurs output possible à partir de la variable
        infractiontype_id qui est supprimée par cette fonction
        Les options concernent :
            - la valeur "Libre", infractiontype_id == 30
            - le niveau de gravite : est-ce qu'on veut une sortie binaire 
        ou bien quelque chose de plus fin distinguant les affaires code de la 
        santé publique et les autres.
    '''
    assert 'infractiontype_id' in tab.columns
    infractiontype_id = tab['infractiontype_id']

    output = infractiontype_id.isnull()
    if libre_est_insalubre:
        output = output | (infractiontype_id == 30)

    if niveau_de_gravite:
        output = 1*output
        cond_gravite = infractiontype_id.isin(range(23,29))
        output[cond_gravite] = 2

    # si titre est dans
    if 'titre' in tab.columns:
        del tab['infractiontype_id']

    tab[name_output] = output
    return tab


def nettoyage_brutal(table):
    # on ne garde que quand le match ban est bon
    table = table[table['adresse_ban_id'].notnull()]
    #del tab['adresse_ban_id']
    # =>  72 lignes en moins
    
    # Il y 188 cadastre de Sarah qui n'ont pas été retrouvé dans les bases
    # Apur au niveau cadastral
    table = table[table['M2_SHAB'].notnull()]

    return table


def variables_de_sarah(table):
    ''' retourne une serie de booléen qui avec True si on a les infos
        sur les variables en plus issues de Sarah
    '''
    # volontairement, on appelle les variables avec le même nom que dans 
    # create_bae_travail. Les deux doivent concorder.
    colonnes_en_plus = ['possedecaves','mode_entree_batiment',
                        'hauteur_facade', 'copropriete']
    table_reduites = tab[colonnes_en_plus]
    
    # table_reduites.notnull().sum(axis=1).value_counts()
    # => on a 3600 cas, où copropriété est rempli et pas les trois autres
    # => on met de côté ces 3600 cas
    
    # on a beaucoup plus d'info sur la copro, que sur les autres
    return table_reduites.notnull().all(axis=1)


def niveau(table, niveau):
    ''' il s'agit de renvoyer la table conforme au niveau d'intérêt
        niveau peut avoir plusieurs valeurs :
        - 'batiment': on renvoie les données pour lesquelles les variables
            issues de sarah sont remplies
        - 'adresse': la table la plus complète, on retire les variables de 
            sarah, mais on a toutes les affaires au niveu adresses avec les
            infos au niveau cadastre
        - 'parcelle': on travaille au niveau parcelle
        TODO: pour l'instant, retire les variables distriué au niveau 
        adresse, on pourrait/devrait en faire une somme au niveau parcelle
    '''
    colonnes_en_plus = ['possedecaves','mode_entree_batiment',
                        'hauteur_facade', 'copropriete']
    assert niveau in ['batiment', 'adresse', 'parcelle']
    if niveau == 'batiment':
        condition = variables_de_sarah(table)
        output = table[condition]
    if niveau == 'adresse':
        output = table.drop(colonnes_en_plus, axis=1)
    if niveau == 'parcelle':
        print("ce niveau n'est pas encore implémenté")
        output = table.drop(colonnes_en_plus, axis=1)
    
    assert all(output.isnull.sum() == 0)
        

if __name__ == "__main__":

    import os
    import pandas as pd
    
    from insalubrite.config_insal import path_bspp, path_output
    
    path_affaires = os.path.join(path_output, 'niveau_adresses.csv')
    adresses_sarah = pd.read_csv(path_affaires)
    
    path_parcelles = os.path.join(path_output, 'niveau_parcelles.csv')
    parcelles = pd.read_csv(path_parcelles)
    assert parcelles['code_cadastre'].isnull().sum() == 0
    
    path_adresses = os.path.join(path_output, 'niveau_adresses.csv')
    adresse = pd.read_csv(path_adresses)
    
    ### étape 1
    # on rassemble toutes les infos
    tab = adresses_sarah.merge(adresse, how='left').merge(parcelles, how='left')
    # On a toutes les affaires (avec une visite) y compris les non matchées
    
    tab = nettoyage_brutal(tab)
    
    tab = build_output(tab, name_output='est_insalubre')
    
    # on pourrait s'en servir avant de supprimer, par exemple en 
    # calculant le temps en la réalisation et l'affaire, mais 
    # dans tous les cas, il faut la retirer
    del tab['realisation_saturnisme']
    
    # Plusieurs niveau de séléction
    
    # on supprime les variables inutiles pour l'analyse
    tab.drop(
        [
        # 'adresse_ban_id',
        'adresse_ban_score', # on ne garde que l'adresse en clair
        'adresse_id', 'typeadresse',
        #'affaire_id', # On garde affaire_id pour des matchs évenuels plus tard (c'est l'index en fait)
        'articles', 'type_infraction', #'infractiontype_id'  on garde par simplicité mais on devrait garder que 'titre',
        'bien_id', 'bien_id_provenance', # interne à Sarah
        'codeinsee_x', 'codeinsee_y',# recoupe codepostal
        'libelle', # = adresse_ban
    
        ],
        axis=1, inplace=True, errors='ignore')
    

    
    
    # faire les trois niveaux de table
    niveau_parcelles = tab.groupby('code_cadastre').sum()
    # TODO: ce n'est pas bon parce qu'il peut y avoir plusieurs affaire dans une
    # parcelle, on veut sommer le deman
    
    
    date = pd.to_datetime(tab['date_creation'])
    # Analyse dans le temps
    # =>  on est bien pour 2009
    tab.groupby([date.dt.year])['est_insalubre'].count()
    tab.groupby([date.dt.year])['est_insalubre'].mean().loc[2006:]
    tab.groupby([date.dt.month])['est_insalubre'].count()
    tab.groupby([date.dt.month])['est_insalubre'].mean()
