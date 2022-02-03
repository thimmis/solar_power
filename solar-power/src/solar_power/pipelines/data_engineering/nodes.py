from typing import List, Tuple, Dict
from numpy.lib.utils import source
import pandas as pd
import numpy as np
import datetime as dt
import random
import re
from pandas.core.arrays import boolean
from pandas.core.indexes.multi import MultiIndex


#HELPER FUNCTIONS

def _checker(frame: pd.DataFrame) -> boolean:
    """Checks to see if there are numerical errors in recorded power data"""
    checked = frame.DC_POWER.max()/frame.AC_POWER.max() - 1 > 1

    return checked

def _fix_generator_data(frame: pd.DataFrame) -> pd.DataFrame:
    frame.DC_POWER = frame.DC_POWER/10
    return frame

def _convert_to_dt(frame: pd.DataFrame) -> pd.DataFrame:
    frame.DATE_TIME = pd.to_datetime(frame.DATE_TIME).dt.floor('T')
    frame.set_index('DATE_TIME', inplace=True)
    return frame

def _seasonal_mean(series, n, lr = 5):
    random.seed(42)
    choices = set(np.linspace(-lr,lr, int((2*lr +1)/2)))
    temp = series.values.copy()
    for i, val in enumerate(temp):
        if pd.isna(val):
            ts_seas = [temp[i-1::-n]]
            if pd.isna(np.nanmean(ts_seas)):
                ts_seas = np.concatenate([temp[i-1::n],temp[i::n]])
            temp[i] = (np.nanmean(ts_seas) * ((100 + random.sample(choices,1)[0])/100))
            
    return pd.Series(index=series.index, data=temp)

def _arima_stamp(skey: str) -> Dict:
    #will be used to create catalog entries for all models
    skey0 = str(skey[0])

    yaml_entry = {
        'type': 'pickle.PickleDataSet',
        'filepath': f'data/06_models/{skey0}_ARIMA/{skey0}_arima.pkl',
        'versioned':'true'
    }
    return yaml_entry

def _flatten_midx(midx: MultiIndex) -> List:
    final_char_check = re.compile(r'[\W_]+$')
    new_cols = []
    for item in midx.values:
        if len(item) == 2:
            new_cols.extend([final_char_check.sub('', '_'.join(item).strip())])
        else:
            new_cols.extend([item])
    return new_cols



#MAIN NODE FUNCTIONS

def wea_gen_merge(gen: pd.DataFrame, wea: pd.DataFrame) -> pd.DataFrame:
    check_gen = _checker(gen)
    if check_gen:
        gen = _fix_generator_data(gen)
    
    gen = _convert_to_dt(gen).drop(['TOTAL_YIELD'], axis=1)
    wea = _convert_to_dt(wea).drop(['SOURCE_KEY'], axis=1)

    source_keys = gen.SOURCE_KEY.unique().tolist()

    #split out for each source key at the station
    split_dict = {}

    not_cols = ['PLANT_ID','DATE_TIME','SOURCE_KEY', 'IMPUTED']

    resample_cols = []

    for key in source_keys:
        data = gen[gen.SOURCE_KEY == key]
        gen_wea_merged = pd.merge(data, wea, left_on=[data.index, 'PLANT_ID'], right_on=[wea.index, 'PLANT_ID']) 
        gen_wea_merged = gen_wea_merged.set_index(pd.DatetimeIndex(gen_wea_merged['key_0'])).drop('key_0',axis=1)
        
        gen_wea_merged['IMPUTED'] = 0 #will detail instances of missing data that was filled in

        gen_wea_merged = gen_wea_merged.resample('15T',origin=gen.index.unique()\
            .tolist()[0]).asfreq()\
                .fillna({'SOURCE_KEY':key, 'PLANT_ID':gen.PLANT_ID.unique()[0]})\
                    .reset_index().rename(columns={'key_0':'DATE_TIME'}).set_index('DATE_TIME')
        

        resample_cols.extend([item for item in gen_wea_merged.columns if item not in not_cols and item not in resample_cols])
        for item in resample_cols:
            gen_wea_merged[item] = _seasonal_mean(gen_wea_merged[item], n = 96)#gen_wea_merged[item].interpolate(method='time')


        gen_wea_merged['PLANT_ID'] = gen_wea_merged['PLANT_ID'].round(1)
        gen_wea_merged['IMPUTED'] = gen_wea_merged['IMPUTED'].fillna(1) #imputed data
        split_dict[key] = gen_wea_merged.reset_index()
    resampled_station_data = pd.concat(split_dict.values(), axis=0)

    return resampled_station_data


def merge_stations_data(*args: List) -> pd.DataFrame:
    all_stations = pd.concat(args, axis=0)
    return all_stations


def extract_skey_list(frame: pd.DataFrame) -> Tuple:
    skey_list = pd.Series(frame.SOURCE_KEY.unique().tolist())
    stations_ids = pd.Series(frame.PLANT_ID.unique().tolist())
    return skey_list, stations_ids
"""
Above are the functions for resample the data with a mean value
and merging the different stations together into a single file.

The next things that need to be done are:
write to the data catalog each of the output destinations
for the models and reporting metrics.

IDEA:
produce models to predict the individual yield for each SK
as well as for the combined STID.
"""

def data_sampling_methods(frame: pd.DataFrame) -> List:
    frame.DATE_TIME = pd.to_datetime(frame.DATE_TIME)
    frame.set_index('DATE_TIME', inplace=True)

    daily_yield_max_ind = frame.groupby(['SOURCE_KEY', pd.Grouper(freq='1D')])\
        .agg({'DAILY_YIELD':'max','IRRADIATION':'sum','AMBIENT_TEMPERATURE':['max','min']})\
        .reset_index().set_index('DATE_TIME')
    print(daily_yield_max_ind.columns)
    daily_yield_max_ind.columns = _flatten_midx(daily_yield_max_ind.columns)
    
    daily_trend = {val: item+1 for item, val in enumerate(daily_yield_max_ind.index.unique().tolist())}

    daily_yield_max_ind['TREND'] = daily_yield_max_ind.index.map(daily_trend)
    daily_yield_max_ind.reset_index(inplace=True)

    dymma_left = frame.groupby(['SOURCE_KEY', pd.Grouper(freq='1D')])['DAILY_YIELD'].agg('max')\
        .reset_index().set_index('DATE_TIME').groupby(pd.Grouper(freq='1D'))['DAILY_YIELD'].mean().reset_index()

    dymma_right = frame.groupby(
        pd.Grouper(freq='1D')).agg({'IRRADIATION':'sum','AMBIENT_TEMPERATURE':['max','min']}).reset_index()
    dymma_right.columns = _flatten_midx(dymma_right.columns)
    daily_yield_max_mean_all = pd.merge(dymma_left,dymma_right,how='inner', on='DATE_TIME')
    daily_yield_max_mean_all['TREND'] = daily_yield_max_mean_all.DATE_TIME.map(daily_trend)
    
    
    dymms_left = frame.groupby(['PLANT_ID','SOURCE_KEY',pd.Grouper(freq='1D')])\
        ['DAILY_YIELD'].max().reset_index().set_index('DATE_TIME')\
        .groupby(['PLANT_ID',pd.Grouper(freq='1D')])['DAILY_YIELD'].mean().reset_index()

    dymms_right = frame.groupby(['PLANT_ID',pd.Grouper(freq='1D')])\
            .agg({'IRRADIATION':'sum','AMBIENT_TEMPERATURE':['max','min']}).reset_index()
    dymms_right.columns = _flatten_midx(dymms_right.columns)

    daily_yield_max_mean_stns = pd.merge(dymms_left,dymms_right,how='inner',on=['DATE_TIME','PLANT_ID'])
    daily_yield_max_mean_stns['TREND']= daily_yield_max_mean_stns.DATE_TIME.map(daily_trend)

    
    
    ac_power_qrthr_rolling_sum_ind = frame.groupby(['SOURCE_KEY',pd.Grouper(freq='15T')])\
        ['AC_POWER'].sum().rolling(96).sum().shift(-96).dropna().reset_index()
    
    ac_power_qtrhr_rolling_sum_all = frame.groupby(pd.Grouper(freq='15T'))\
        ['AC_POWER'].sum().rolling(96).sum().shift(-96).dropna().reset_index()


    return daily_yield_max_ind,\
         daily_yield_max_mean_stns,\
         daily_yield_max_mean_all,\
         ac_power_qrthr_rolling_sum_ind,\
         ac_power_qtrhr_rolling_sum_all


def write_to_catalog(yml_file: Dict, skey_list: pd.Series, station_ids: pd.Series) -> Dict:
    for item in skey_list.values:
        arima_key = str(f'{item[0]}_arima')
        arima_entry = _arima_stamp(item)
        if arima_key in yml_file.keys():
            yml_file[arima_key] = arima_entry
        else:
            yml_file[arima_key] = arima_entry

    for station in station_ids.values:
        station_key = str(f'{int(station[0])}_arima')
        arima_entry = _arima_stamp(station)
        if station_key in yml_file.keys():
            yml_file[station_key] = arima_entry
        else:
            yml_file[station_key] = arima_entry
    return yml_file


