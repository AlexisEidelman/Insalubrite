# -*- coding: utf-8 -*-
"""

Rassemble les données générées dans la création de base
pour avoir des données sur lesquelles produire des statistiques et autres infos.

Pour être clair: on prend ici les données sarah_adresse, niveau_parcelles et
niveau_adresses comme données. On ne les modifie pas.

"""

import os
import pandas as pd
from sklearn.preprocessing import LabelEncoder

    
from insalubrite.config_insal import path_bspp, path_output

def build_output(tab, name_output = 'output', libre_est_salubre = True,
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

    insalubres = infractiontype_id.notnull()
    if libre_est_salubre:
        insalubres = insalubres & (infractiontype_id != 30)

    if niveau_de_gravite:
        insalubres = 1*insalubres
        cond_gravite = infractiontype_id.isin(range(23,29))
        insalubres[cond_gravite] = 2

    # si titre est dans
    if 'titre' in tab.columns:
        tab['titre'].fillna('Rien', inplace=True)
        del tab['infractiontype_id']

    tab[name_output] = insalubres
    return tab


def _nettoyage_brutal(table):
    # on ne garde que quand le match ban est bon
    table = table[table['adresse_ban_id'].notnull()]
    table = table[table['codeinsee'].notnull()]
    #del tab['adresse_ban_id']
    # =>  72 lignes en moins

    # Il y 188 cadastre de Sarah qui n'ont pas été retrouvé dans les bases
    # Apur au niveau cadastral
    table = table[table['M2_SHAB'].notnull()]
    
    # Il y a des valeurs nulles dans la suface habitable par exemple 
    table = table[table['M2_SHAB'] > 0]
    # retire table['est_insalubre'].value_counts()
    # False    48
    # True     26
    return table


def variables_de_sarah(table):
    ''' retourne une serie de booléen qui avec True si on a les infos
        sur les variables en plus issues de Sarah
    '''
    # volontairement, on appelle les variables avec le même nom que dans
    # Les deux doivent concorder.
    colonnes_en_plus = ['possedecaves','mode_entree_batiment',
                        'hauteur_facade', 'copropriete']
    table_reduites = table[colonnes_en_plus]

    # table_reduites.notnull().sum(axis=1).value_counts()
    # on a beaucoup plus d'info sur la copro, que sur les autres
    # => 3600 cas, où copropriété est rempli et pas les trois autres
    # => on accepte de perdre cette info. met de côté ces 3600 cas

    return table_reduites.notnull().all(axis=1)


def get_niveau(table, niveau):
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
        output = table.loc[condition]
    if niveau == 'adresse':
        output = table.drop(colonnes_en_plus, axis=1)
    if niveau == 'parcelle':
        print("ce niveau n'est pas encore implémenté")
        niveau_parcelles = tab.groupby('code_cadastre').sum()
        # TODO: ce n'est pas bon parce qu'il peut y avoir plusieurs affaire dans une
        # parcelle, on veut sommer le deman
        output = table.drop(colonnes_en_plus, axis=1)

    assert all(output.isnull().sum() == 0)
    return output


colonnes_pompiers = [
        'Assechement de locaux', 'Assechement de locaux (sur requisition)',
       'Chaudiere surchauffee', 'Court-circuit',
       "Debouchage d'egout ou de canalisation", 'Eboulement, effondrement',
       'Feu appel douteux', 'Feu ayant existe', 'Feu de ...',
       'Feu de cheminee', "Fuite d'eau", "Fuite d'hydrocarbure dans un local",
       'Fuite de gaz (autres)', 'Fuite de gaz (butane ou propane)',
       'Fuite de gaz enflammee', 'Immeuble en peril', 'Inondation importante',
       'Materiaux menacant de chuter', 'Odeur suspecte',
       "Panne d'origine electrique",
       "Personne bloquee dans une cabine d'ascenseur",
       'Prelevement monoxyde de carbone'
       ]


colonnes_demandeurs = [
        "Logé chez d'autres personnes",
       'Centre départemental de l’enfance et de la famille ou centre maternel',
       'Foyers agents Ville ou agents CASVP',
       'Logé dans un logement de fonction par votre employeur',
       'Logé dans un foyer', 'Logé à titre gratuit',
       "Logé dans un hôtel social, par un centre d'hébergement, un logement d'urgence ou une association",
       "Logé à l'hôtel",
       "Dans un local non destiné à l'habitat (cave, parking, etc.)",
       'Logé chez des parents', 'Propriétaire', 'Locataire dans le privé',
       'Résidence étudiant', 'Résidence hôtelière à vocation sociale',
       'Sans domicile fixe', 'Structure d’hébergement',
       'Sous locataire ou hébergé dans un logement à titre temporaire',
       'Locataire dans un logement social', 'Louez solidaire et sans risque',
       'Dans un squat'
    ]

cols_type_logement = ['NB_LG_GRA', 'NB_LG_LOC', 'NB_LG_NIM', 'NB_LG_PRO',
                      'NB_LG_IMP', 'NB_LG_VAC', 'NB_LG_DIV', 'NB_LG_UTL',
                      'NB_LG_LPF']
cols_nb_pieces = ['NB_PIEC_1', 'NB_PIEC_2', 'NB_PIEC_3', 'NB_PIE_4P',
                  'NB_PIANX']
cols_taille_logements = ['NB_LG1_9', 'NB_LG1019', 'NB_LG2029',
                         'NB_LG3039', 'NB_LG4049', 'NB_LG5069',
                         'NB_LG7089', 'NB_LG_S90']

def get_data(niveau, libre_est_salubre=True, niveau_de_gravite=False,
             pompier_par_intevention = False,
             demandeur_par_type = False,
             repartition_logement_par_nb_pieces=False,
             repartition_logement_par_taille=False,
             repartition_logement_par_type=False,
             toutes_les_annes = False,
             ):

    path_parcelles = os.path.join(path_output, 'niveau_parcelles.csv')
    parcelles = pd.read_csv(path_parcelles)
    assert parcelles['code_cadastre'].isnull().sum() == 0
    parcelles = build_output(parcelles, name_output='est_insalubre',
                             libre_est_salubre=libre_est_salubre,
                             niveau_de_gravite=niveau_de_gravite)


    path_adresses = os.path.join(path_output, 'niveau_adresses.csv')
    adresse = pd.read_csv(path_adresses)
    adresse = build_output(adresse, name_output='est_insalubre',
                           libre_est_salubre=libre_est_salubre,
                           niveau_de_gravite=niveau_de_gravite)

    # on rassemble toutes les infos
    tab = adresse.merge(parcelles, how='left')

    
    # On a toutes les affaires (avec une visite) y compris les non matchées

    # on pourrait s'en servir avant de supprimer, par exemple en
    # calculant le temps en la réalisation et l'affaire, mais
    # dans tous les cas, il faut la retirer
    del tab['realisation_saturnisme']

    tab = _nettoyage_brutal(tab)

    if not pompier_par_intevention:
        tab.loc[:,'intevention_bspp'] = tab[colonnes_pompiers].sum(axis=1)
        tab.drop(colonnes_pompiers, axis=1, inplace=True)

    if not demandeur_par_type:
        tab.drop(colonnes_demandeurs, axis=1, inplace=True)
        del tab['TOTAL_DEM']
        tab.rename(columns={
            'TOT_LOC': 'Demandeurs locataires',
            'TOT_PROP': 'Demandeurs propriétaires'
            }, inplace=True)
    else:
        tab.drop(['TOT_PROP', 'TOT_LOC', 'TOTAL_DEM'], axis=1, inplace=True)

    # on met la répartitions des logement en ratio du nombre de logements
    for col in cols_type_logement + cols_taille_logements + cols_nb_pieces:
        tab[col] /= tab['NB_LG']

    if not repartition_logement_par_nb_pieces:
        tab.drop(cols_nb_pieces, axis=1, inplace=True)
    if not repartition_logement_par_taille:
        tab.drop(cols_taille_logements, axis=1, inplace=True)
    if not repartition_logement_par_type:
        tab.drop(cols_type_logement, axis=1, inplace=True)
    else:
        print('attention, il y a quelque lignes pour lesquelles le type de',
              "logement n'est pas rempli")

    if not toutes_les_annes:
        tab.drop(['AN_MAX', 'AN_BATLG', 'AN_BATLOA', 'AN_BATSUR'], axis=1, inplace=True)


    output = get_niveau(tab, niveau)

    # format des variables
    # les booléens codés par sarah en 0, 1 et 2 avec des NaN
    # NB: il faut le faire après get_niveau
    for var in ['possedecaves', 'copropriete']:
        temp = output[var].fillna(-1)
        output.loc[:, var] = temp.astype(int).astype(str)
    
    output.loc[:,'hotel meublé'] = output['hotel meublé'].astype(bool)
    output.loc[:,'B_PUBLIC'] = output['B_PUBLIC'] == 'O'
    return output



if __name__ == "__main__":
    tab = get_data("batiment", libre_est_salubre=True, niveau_de_gravite=False)

    def analyse_temporelle(table):
        date = pd.to_datetime(table['date_creation'])
        # Analyse dans le temps
        # =>  on est bien pour 2009
        table.groupby([date.dt.year])['est_insalubre'].count()
        print(table.groupby([date.dt.year])['est_insalubre'].mean().loc[2006:])
        table.groupby([date.dt.month])['est_insalubre'].count()
        table.groupby([date.dt.month])['est_insalubre'].mean()

    analyse_temporelle(tab)