# -*- coding: utf-8 -*-
"""

"""

import os
import pandas as pd

from insalubrite.config_insal import path_apur, path_output
from insalubrite.match_to_ban import merge_df_to_ban


path_hm = os.path.join(path_apur + '2015', '60 hm_2015.xlsx')
hm = pd.read_excel(path_hm)

hm['hotel meublé'] = True
del hm['hm']

hm.rename(columns={'asp':'ASP'}, inplace=True)

path_hm_out = os.path.join(path_output, 'hm.csv')
hm.to_csv(path_hm_out, index=False, encoding='utf8')
