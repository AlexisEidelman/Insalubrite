# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 13:24:09 2017

"""

import os
import pandas as pd

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql


####
## infractionhisto pointe vers cr_visite qui pointe vers affaire
## infractionhisto pointe vers infraction qui pointe vers affaire
## On verifie la cohérence
####

infractionhisto = read_table('infractionhisto')

#Dans infractionhisto, la ligne d'indice 47206 correspond à
#infraction_id = 28588 compterenduvisite_id = 46876 
#ils doivent correspondre à la même affaire

cr_visite_brut = read_table('cr_visite')
infraction_brut = read_table('infraction')
cr_visite_brut[cr_visite_brut.id == 46876].affaire_id
infraction_brut[infraction_brut.id == 28588].affaire_id
##=>True
#Est-ce vrai pour toute ligne de infractionhisto?
infractionhisto_avant_merge = infractionhisto[['infraction_id','compterenduvisite_id']]
infractionhisto_avant_merge.drop_duplicates(inplace=True)
infraction_visite = infractionhisto_avant_merge.merge(cr_visite_brut,
                                                      left_on = 'compterenduvisite_id',
                                                      right_on = 'id',
                                                      how = 'left')
infraction_affaire = infractionhisto_avant_merge.merge(infraction_brut,
                                                       left_on = 'infraction_id',
                                                       right_on = 'id',
                                                       how = 'left')
parinfraction = pd.Series(infraction_affaire.affaire_id.unique())
parvisite = pd.Series(infraction_visite.affaire_id.unique())
parinfraction.isin(parvisite).value_counts() ##=> True 16937, False 1
parvisite.isin(parinfraction).value_counts() ##=> True 16937, False 125

## Comprendre les 125 affaires liées à cr_visite mais pas cohérentes avec 
## infraction
cr_visite  = cr_visite_brut[['affaire_id', 'date']].drop_duplicates()
incoherent_aff_id = parvisite[~parvisite.isin(parinfraction)]
incoherent_visits = cr_visite.loc[cr_visite.affaire_id.isin(incoherent_aff_id)]
incoherent_visits.groupby('affaire_id').size()


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


def signalement_des_affaires():
    ''' lorsque l'on fuisionne affhygiene avec signalement affaire,
        on a beaucoup de raté, comment se fait-ce ?
    '''
    affaire = read_table('affaire')
    signalement_affaire = read_table('signalement_affaire')
    test = pd.merge(affaire, signalement_affaire,
                                         left_on='id',
                                         right_on='affaire_id',
                                         how='outer',
                                         indicator='provenance')
    #test.provenance.value_counts()
    #both          30871
    #left_only     27896
    # => beaucoup d'affaire n'ont pas de signalement
    id_affaires_sans_signalement = test.loc[test.provenance == 'left_only', 'id']


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


def coherence_infractions():
    '''
     infractionhisto pointe vers cr_visite qui pointe vers affaire
     infractionhisto pointe vers infraction qui pointe vers affaire
     On verifie la cohérence
    '''
    cr_visite_brut = read_table('cr_visite')
    cr_visite  = cr_visite_brut[['affaire_id', 'date']].drop_duplicates()
    infraction_brut = read_table('infraction')
    infractionhisto = read_table('infractionhisto')
    ## infractionhisto pointe vers cr_visite qui pointe vers affaire: affaire_infraction
    ## infractionhisto pointe vers infraction qui pointe vers affaire: affaire_crvisite

    #affaire_infraction: les affaires reliées à des infractions
    affaire_infraction = cr_visite_brut[\
          cr_visite_brut.id.isin(infractionhisto.compterenduvisite_id)].affaire_id
    affaire_infraction = pd.Series(affaire_infraction.unique())
    #affaire_crvisite: les affaires liées à infractionhisto via cr_visite
    affaire_crvisite = infraction_brut[\
                infraction_brut.id.isin(infractionhisto.infraction_id)].affaire_id
    affaire_crvisite = pd.Series(affaire_crvisite.unique())

    affaire_crvisite.isin(affaire_infraction).value_counts(dropna = False)
    #True     16937
    #False        1
    affaire_infraction.isin(affaire_crvisite).value_counts(dropna = False)
    #True     16937
    #False      125
#    affhygiene = read_table('affhygiene')


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

