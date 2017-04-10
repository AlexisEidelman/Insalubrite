# -*- coding: utf-8 -*-
"""
Created on Thu Feb  9 13:24:09 2017

"""


####
#On regarde si une affaire de la table affaire et ou bien dans
#aff_hygiene ou bien dans aff_ravalement
####

from insalubrite.config_insal import path_sarah
from insalubrite.Sarah.read import read_table, read_sql

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


####
## Question sur bien_id
####
hyg.bien_id

# on va regarder les id de toutes les tables et
# selectionner celle qui contiennent les valeur de bien_id
import os

primary_key, foreign_key = read_sql()
tables_on_disk = set(x[:-4] for x in os.listdir(path_sarah))

potentiel_match = []
for name in tables_on_disk:
    tab = read_table(name, nrows=0)
    if 'id' in tab.columns:
        tab = read_table(name)
        id_tab = tab['id']
        if all(hyg.bien_id.isin(id_tab)):
            potentiel_match.append(name)

print(potentiel_match)
# return ['ficherecolem']




####
# On regarde la cohérence entre les colonnes de infraction_histo
# et celle que l'on peut avoir dans infraction_type
####

# il y a un problème de cohérence entre l'infraction type et articles, titre et
#libelle

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
