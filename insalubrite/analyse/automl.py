# -*- coding: utf-8 -*-

from time import time
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from autosklearn.classification import AutoSklearnClassifier

from data import get_data
import preprocess
from split import simple_split, split_by_date

tab_ini = get_data('batiment')
tab = preprocess.keep_cols_for_analysis(tab_ini)
float_tab = preprocess.to_float(tab)

float_tab['date'] = pd.to_datetime(tab_ini['date_creation'])
X_train, X_test, y_train, y_test = split_by_date(float_tab, 'date')

#quali_tab = preprocess.to_qualitative(tab, 4)
#
#quali_in_numerique = preprocess.to_float(quali_tab)
#
#encoded = preprocess.encode(quali_tab)
#print(quali_tab.dtypes)

pipeline_optimizer = AutoSklearnClassifier()

pipeline_optimizer = AutoSklearnClassifier()

pipeline_optimizer.fit(X_train.values, y_train.values)

pipeline_optimizer.predict_proba(X_test)
pd.Series(pipeline_optimizer.predict(X_test)).value_counts()

# on veut savoir si on claase bien les 150 premiers:
proba = pipeline_optimizer.predict_proba(X_test)
proba_etre_insalubre = [x[1] for x in proba]
proba = pd.Series(proba_etre_insalubre)

prediction = proba.rank(ascending=False) < 150 + 1
y_test.loc[prediction.values].value_counts(normalize=True)

autre_approche = pipeline_optimizer.predict(X_test) == 1
y_test.loc[autre_approche].value_counts(normalize=True)

autre_approche = pipeline_optimizer.predict(X_test) == 0
y_test.loc[autre_approche].value_counts(normalize=True)