#/usr/bin/python3
# @Author: Thomas Turner <thomas>
# @Date:   2020-10-13T13:24:19+02:00
# @Email:  thomas.benjamin.turner@gmail.com
# @Last modified by:   thomas
# @Last modified time: 2020-10-13T13:27:14+02:00


import pandas as pd

def clean_time1(frame, col):
    frame[col] = pd.to_datetime(frame[col], format = '%d-%m-%Y %H:%M')
    frame['DATE'] = frame[col].dt.date
    frame['TIME'] = frame[col].dt.time
    frame['HOUR'] = pd.to_datetime(frame['TIME'],format='%H:%M:%S').dt.hour
    frame['MINUTE'] = pd.to_datetime(frame['TIME'],format='%H:%M:%S').dt.minute
    frame = frame.drop(['PLANT_ID','TIME'],axis = 1)
    return frame

def clean_time2(frame, col):
    frame[col] = pd.to_datetime(frame[col], format = '%Y-%m-%d %H:%M')
    frame['DATE'] = frame[col].apply(lambda x: x.date())
    frame['TIME'] = frame[col].apply(lambda x: x.time())
    frame['HOUR'] = pd.to_datetime(frame['TIME'],format='%H:%M:%S').dt.hour
    frame['MINUTE'] = pd.to_datetime(frame['TIME'],format='%H:%M:%S').dt.minute
    frame = frame.drop(['PLANT_ID','TIME'],axis = 1)
    return frame

def inverter_comp(df_in, date_list,key_list):
    main_df = pd.DataFrame()
    for day in date_list:
        #Each inverter should be done producing by 7pm every day thus the daily yield shouldn't have increased
        df = df_in[(df_in['DATE']== day)&(df_in.HOUR == 20)&(df_in.MINUTE==0)][-len(key_list):]
        df = df.drop(['DC_POWER','AC_POWER','TOTAL_YIELD'],axis= 1)
        main_df = main_df.append(df,ignore_index=True)
    return main_df

def inverter_comp_alt(df_in, date_list,key_list):
    main_df = pd.DataFrame()
    for day in date_list:
        #Each inverter should be done producing by 7pm every day thus the daily yield shouldn't have increased
        df = df_in[(df_in['DATE']== day) &(df_in.HOUR == 20)&(df_in.MINUTE==0)][-len(key_list):]
        df = df.drop(['DC_POWER','AC_POWER','TOTAL_YIELD'],axis= 1)
        df = df[df['DAILY_YIELD']!=0]
        main_df = main_df.append(df,ignore_index=True)
    return main_df

def fourwks_n(df,nforward):
    num_days = df.DATE.unique()
    test_df = pd.DataFrame()
    prediction_df = pd.DataFrame()
    
    for i in range(0,28):
        toss_df1 = pd.DataFrame()
        toss_df1 = df[df['DATE']==num_days[i]]
        test_df = test_df.append(toss_df1)
    for j in range(28,28+nforward):
        toss_df2 = pd.DataFrame()
        toss_df2 = df[df['DATE']==num_days[j]]
        prediction_df = prediction_df.append(toss_df2)
        
    return test_df, prediction_df