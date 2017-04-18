# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 13:24:09 2017

"""

import os
import pandas as pd

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql


def affaires_hyg_ou_raval():
    '''
    On regarde si une affaire de la table affaire et ou bien dans
    aff_hygiene ou bien dans aff_ravalement
    '''
    affaire = read_table('affaire')
    hyg = read_table('affhygiene')
    rava = read_table('affravalement')

    # premier test
    assert len(affaire) == len(hyg) + len(rava)

    # affaire_id est bien dans l'id de affaire
    assert all(hyg['affaire_id'].isin(affaire['id']))
    assert all(rava['affaire_id'].isin(affaire['id']))

    # affaire_id n'est pas à la fois dans hygiène et dans ravalement
    assert(all(~hyg['affaire_id'].isin(rava['affaire_id'])))
    assert(all(~rava['affaire_id'].isin(hyg['affaire_id'])))

    # les affaires sont bien dans affhygiene et dans affravalement
    liste_affaire = (rava['affaire_id'].append(hyg['affaire_id'])).tolist()
    assert(all(affaire['id'].isin(liste_affaire)))


def _recherche_valeurs_in_id(liste_valeurs, in_vars = ['id'], verbose=False):
    ''' fonction utile pour la questions suivante
        elle cherche dans les tables si les id contiennent toutes les
        valeurs de liste_valeurs

        si la liste des valeurs est _id alors on cherche dans toutes les
        colonnes se terminant par _id
    '''
    primary_key, foreign_key = read_sql()
    tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

    potentiel_match = []
    vars_a_etudier = in_vars

    for name in tables_on_disk:
        tab = read_table(name, nrows=0)
        if in_vars == '_id':
            vars_a_etudier = [col for col in tab.columns if col[-3:] == '_id']

        if any([var in tab.columns for var in vars_a_etudier]):
            if verbose:
                print('table', name)
            tab = read_table(name)

            for var in vars_a_etudier:
                if var in tab.columns:
                    id_tab = tab[var]
                    if all(liste_valeurs.isin(id_tab)):
                        if verbose:
                            print(' matched pour', id_tab, '!!!')
                        if in_vars == '_id':
                            potentiel_match.append((name, var))
                        else:
                            potentiel_match.append(name)
    return potentiel_match


def question_bien_id():
    ''' On va regarder les id de toutes les tables et
    selectionner celle qui contiennent les valeur de bien_id
    '''
    hyg = read_table('affhygiene')
    liste_valeurs = hyg.bien_id
    print(_recherche_valeurs_in_id(liste_valeurs))
    # => ['ficherecolem']

    # autre hypothèse bien_id se retrouve dans plusieurs tables !
    immeuble = read_table('immeuble')
    batiment = read_table('batiment')
    parcelle = read_table('parcelle_cadastrale')
    localhabite = read_table('localhabite')

    tous_les_ids = immeuble.id.append(batiment.id).append(parcelle.id).append(localhabite.id)
    assert all(tous_les_ids.value_counts() == 1)
    assert all(liste_valeurs.isin(tous_les_ids))
    # on a donc trouvé. On valide par le fait que les numéro sont dispersé dans les bases




def signalement_des_affaires():
    ''' lorsque l'on fuisionne affhygiene avec signalement affaire,
        on a beaucoup de raté, comment se fait-ce ?
    '''
    affaire = read_table('affaire')
    affhyg = read_table('affhygiene')
    signalement_affaire = read_table('signalement_affaire')
    test = pd.merge(affhyg, signalement_affaire,
                                         on='affaire_id',
                                         how='outer',
                                         indicator='provenance')
    #test.provenance.value_counts()
    #both          30692
    #left_only     10433
    #right_only      179
    # => beaucoup d'affaire n'ont pas de signalement
    id_affaires_sans_signalement = test.loc[test.provenance == 'left_only',
                                            'affaire_id']
    id_affaires_sans_signalement.sort_values(inplace=True)
    shift = id_affaires_sans_signalement - id_affaires_sans_signalement.shift(1)
    # => 2/3 beaucoup d'écart de 1 comme si cela concernait des affaires proches


    # exemple
    # random = id_affaires_sans_signalement.iloc[5644] # = 5685
    random = 5644
    affaire_ids = id_affaires_sans_signalement.iloc[random-  5:random + 5]

    bien_id_correspondant = affhyg.loc[affhyg['affaire_id'].isin(affaire_ids)]
    cr = read_table('cr_visite')
    cr[cr['affaire_id'].isin(bien_id_correspondant['affaire_id'])]


    ccc = cr[cr['affaire_id'].isin(id_affaires_sans_signalement)]


def coherence_libelle_infractiontype():
    ''' On regarde la cohérence entre les colonnes de infraction_histo
    et celle que l'on peut avoir dans infraction_type

    cohérence entre l'infraction type et articles, titre et libelle
    '''


    infraction_histo = read_table('infractionhisto')

    infractiontype = read_table('infractiontype')
    infractiontype.drop(['active', 'ordre'], axis=1, inplace=True)
    infractiontype.rename(columns={'id': 'infractiontype_id'}, inplace=True)

    test = infraction_histo.merge(infractiontype,
                                  on='infractiontype_id',
                                            how='outer', indicator=True)


    pb_article = test['articles_x'] != test['articles_y']
    test['pb_article'] = pb_article
    pb_titre = test['titre_x'] != test['titre_y']
    test['pb_titre'] = pb_titre
    pb_libelle = test['libelle_x'] != test['libelle_y']
    test['pb_libelle'] = pb_libelle
    pb_quelconque = pb_article | pb_titre | pb_libelle

    pb = test[pb_quelconque]
    len(test) - len(pb)  # on a 30% sans problème
    pb[['pb_libelle', 'pb_titre', 'pb_article']].sum()
    pb.groupby(['pb_libelle', 'pb_titre', 'pb_article']).size()
    # bcp de problème de cohérence titre

    pb[pb_libelle].groupby(['libelle_x','libelle_y']).size()
    infraction_histo.libelle.isin(infractiontype.libelle).value_counts()


def coherence_affaires_histoinfractions():
    '''
     infractionhisto pointe vers cr_visite qui pointe vers affaire
     infractionhisto pointe vers infraction qui pointe vers affaire
     On verifie la cohérence
    '''
    infractionhisto = read_table('infractionhisto')
    #Dans infractionhisto, la ligne d'indice 47206 correspond à
    #infraction_id = 28588 compterenduvisite_id = 46876
    #ils doivent correspondre à la même affaire
    cr_visite = read_table('cr_visite')
    cr_visite.rename(columns={'id':'compterenduvisite_id'}, inplace=True)
    infraction = read_table('infraction')
    infraction.rename(columns={'id':'infraction_id'}, inplace=True)

    ##=>True
    #Est-ce vrai pour toute ligne de infractionhisto?
    infractionhisto = infractionhisto[['id', 'infraction_id',
                                       'compterenduvisite_id']]
    infractionhisto.rename(columns={'id':'infractionhisto_id'},
                                       inplace=True)
    infractionhisto.drop_duplicates(inplace=True)


    via_table_cr_visite = infractionhisto.merge(cr_visite,
                                              on = 'compterenduvisite_id',
                                              how = 'left')

    via_table_infraction = infractionhisto.merge(infraction,
                                                 on = 'infraction_id',
                                                 how = 'left',
                                                 indicator='is_in_infraction')

    infractionhisto.infraction_id.isin(infraction.infraction_id)

    parinfraction = pd.Series(via_table_infraction.affaire_id.unique())
    parvisite = pd.Series(via_table_cr_visite.affaire_id.unique())
    parinfraction.isin(parvisite).value_counts() ##=> True 16937, False 1
    parvisite.isin(parinfraction).value_counts() ##=> True 16937, False 125

    test_coherence = via_table_infraction.merge(via_table_cr_visite,
                                              on=['infractionhisto_id', 'affaire_id'],
                                              how='outer',
                                              indicator=True)

    problemes = test_coherence[test_coherence['_merge'] != 'both']
    test_coherence._merge.value_counts()
    # Gloablement ça marche très bien. Pas d'incohérence.
    # on a 710 incohérences qui viennent de ce qu'on n'a pas d'affaire_id
    # pour absolument toutes les lignes de la table infraction.
    # => passer par cr_visite est plus sûr


def coherence_arretehyautre_pvscp():
    ''' Question : quelle cohérence entre arretehyautre et pvscp ? '''
    ## Ne marche pas !

    arrete = read_table('arretehyautre')[['id', 'affaire_id','type_id']]
    arrete.rename(columns={'id': 'arrete_hygiene_id'}, inplace=True)

    pvcsp = read_table('pvcsp')

    ## Première vérification
    pvcsp_avec_arrete = pvcsp.merge(arrete,
                                    on='arrete_hygiene_id',
                                    how='outer',
                                    indicator=True)
    # bcp moins de pvscp que d'arrete mais tous dedans.
    # supsicion : il n'y a que les arrêté en cours, les autres sont dans classés
    # sinon, il y a bien cohérence puisque les pvcsp renvoie à arrete

    ## vérification des cohérences des affaires
    cr = read_table('cr_visite')
    cr = cr[['id', 'affaire_id']]
    cr.rename(columns={'id': 'compte_rendu_id'}, inplace=True)

    arrete_avec_cr = arrete.merge(cr,
                                       on='affaire_id',
                                       how='left',
                                       indicator=True)
    # => 15 non match ? cf arrete_avec_cr._merge.value_counts()


    pvcsp_avec_cr = pvcsp.merge(cr,
                                on='compte_rendu_id',
                                how='left',
                                indicator=True)
    # pvcsp_avec_cr._merge.value_counts()

    affaires_pvscp =  pvcsp_avec_cr.affaire_id
    affaires_arrete = arrete_avec_cr.affaire_id
    assert all(affaires_pvscp.isin(affaires_arrete))
    assert not all(affaires_arrete.isin(affaires_pvscp))
    # confirme la supsicion :
    # il n'y a dans pvcsp que les arrêté en cours,
    # les autres sont dans classés (table classement)

def plusieurs_infractions_reliees_a_une_meme_visite():
    infractionhisto = read_table('infractionhisto')
    infractionhisto.compterenduvisite_id.value_counts()
    # pourquoi on a plusieurs infratction reliée à une même visite ?
    # exemple infractionhisto[infractionhisto.compterenduvisite_id == 37955]
    # TODO: voir avec le SI d'où ça peut venir
    # => il peut y avoir plusieurs infractions constatées sur une même visite

