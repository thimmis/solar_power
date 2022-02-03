from typing import List, Tuple
import pandas as pd
import numpy as np
from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.stattools import adfuller
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.model_selection import TimeSeriesSplit

"""
Eventually want to be able to train multiple different ML models here.

Going to start with training of arima models.

TODO:
    first need to check for stationarity of the dataset.
    to accomplish this I will be using the augmented dickey-fuller test for strong
    stationarity, i.e. test statisitic shows 99% confidence in stationarity.

    *will conuct this test on:
        1) base time series
        2) first order differencing
        3) second order differencing

    This will drastically reduce the number of combinations necessary to 
    determine the precise AR and MA terms through an exhaustive search.

    Once the optimal parameters for the ARIMA model are determined the next step
    is to conduct a step forward validation method that measures the out of sample
    error via MAPE.
    Want the final model to optimize both the Akaike and Bayes Information
    Criterion. In addition the test statistics on the relationship between the model
    features and the endogenous variable should show a p-value < \alpha = 0.05.

    We shall use all of these metrics to determine how well the model has understood
    the patterns within the data. We shall also attempt to add in additional
    exogenous variables to try and improve the accuracy of the model. The same criteria
    as above will be used to evaluate. 
"""

def check_series_stationarity():
    return None

def grid_search_model_parameters():
    return None

def back_testing_training():
    return None

