# -*- coding: utf-8 -*-
"""
Dans les affaires, on a un identifiant bien_id qui relie à la définition
(géographique du lien)
"""

hyg_id = read_table('affhygiene')['bien_id']
# 37322 lignes

ficherecolem = read_table('ficherecolem')
# 510 526 ligne

ficherecolem_subset = ficherecolem[ficherecolem['id'].isin(hyg_id)]
fiche = ficherecolem_subset

assert(fiche['tournee_id'].isnull().all()) 
# la variable tournee_id ne sert à rien sur les lignes liées à aff_hygiene
del fiche['tournee_id']


print("Il n'est pas sûr que bien_id de aff_hygiene soit bien lié à fichecolem \
    en effet on s'arrête au numéro 270 000 et on ne voit pas pourquoi ça ne \
    serait pas réparti jusqu'au bout le table qui va jusqu'à 500 000")