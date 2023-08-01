import datetime

import pandas as pd
import datetime as dt
import time
import numpy as np

today= datetime.datetime.now()
current_yr = today.year
current_mth = today.month

def ptax(inpdata,outpath):

    lst = ["2019-20","2020-21","2021-22","2022-23"]

    ll = []
    for i in lst:
        # df = pd.concat(pd.read_excel(inpdata, sheet_name=i), ignore_index=True, sort=False)
       df =pd.read_excel(inpdata,sheet_name=i)
       ll.append(df)

    dff1= pd.DataFrame(pd.concat(ll, sort=False))
    dff1 = dff1.drop(columns= ['propertyname','ezname'])
    dff1["receiptdate"] =pd.to_datetime(dff1["receiptdate"])
    dff1['Year'] = dff1["receiptdate"].dt.year
    dff1['Month'] = dff1["receiptdate"].dt.month
    dff1['Dow'] = dff1["receiptdate"].dt.day_name()
    dff1["receiptdate"] =pd.to_datetime(dff1["receiptdate"])
    dff1.to_csv(outpath + "collection_data.csv", index=False,sep="|")

    # print('Final Excel sheet now generated at the same location:')
    # print(True)

def no_paid_tax(outpath):
    collection2018_23_df = pd.read_csv(outpath + "collection_data.csv", sep="|")

    # Find unique values and their count
    # unique_values, count = np.unique(collection2018_23_df['propertycode'], return_counts=True)
    # unique_pid_df = pd.DataFrame({'propertycode': unique_values, 'count': count})
    unique_values = collection2018_23_df.propertycode.unique()
    unique_pid_df = pd.DataFrame({'propertycode': unique_values})
    unique_pid_df['propertycode'] = unique_pid_df['propertycode'].str.replace("/", "S")

    ##find the tax_paid_year & tax_paid_month
    collection_df = collection2018_23_df.copy()
    collection_df['propertycode'] = collection_df['propertycode'].str.replace("/", "S")

    collection_df['fin_year'] = np.where(collection_df['Month'] <4,collection_df['Year']-1,collection_df['Year'])
    collection_df['fin_month'] = np.where(collection_df['Month']<4,collection_df['Month']+9,collection_df['Month']-3)

    ## find the max year or month
    collection_df_maxfyyear = collection_df.groupby(['propertycode'])['fin_year','fin_month'].max().reset_index()

    # df_new =df_new.reset_index(drop=True)
    # Apply merge using unique property id
    merge_collect_uniqpid_df = unique_pid_df.merge(collection_df_maxfyyear, on='propertycode',how='inner')

    ## drop data if fincial yr is less than 2022
    data = merge_collect_uniqpid_df[merge_collect_uniqpid_df['fin_year'] < 2022]

    filter_data_lessthan2022 = data.copy()

    ## find the defualter list might, may and major
    might_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_month'] > 9) &
                                                (filter_data_lessthan2022['fin_year'] == 2021)].reset_index(drop=True)
    may_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_month'] < 10) &
                                              (filter_data_lessthan2022['fin_year'] == 2021)].reset_index(drop=True)
    major_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_year'] < 2021)].reset_index(drop=True)

    ## set the deafault value as 1
    might_default_df['might_default'] =1
    may_default_df['may_default'] = 1
    major_default_df['major_default'] =1

    ### drop count column
    # might_default = might_default_df.drop(columns='count')
    # may_default = may_default_df.drop(columns='count')
    # major_default = major_default_df.drop(columns='count')

    ### merge above defaulterdf with main df
    # merge_collection_might_df = might_default_df.merge(collection_df,on=['propertycode','fin_year','fin_month'],how ='left')
    # merge_data_df2 = may_default_df.merge(merge_collection_might_df,on=['propertycode','fin_year','fin_month'],how ='left')
    # final_merge_data = major_default_df.merge(merge_data_df2,on=['propertycode','fin_year','fin_month'],how ='left')

    # final_merge_data.to_csv(outpath + "pbiconsolidate_test.csv", index=False, sep=",")

    # might_default.to_csv(outpath + "might_default.csv",index =False)
    # may_default.to_csv(outpath + "may_default.csv",index =False)
    # major_default.to_csv(outpath + "major_default.csv",index =False)

    aaaaa = [might_default_df,may_default_df,major_default_df]
    lst=[]
    for i in aaaaa:
        lst.append(i)
    www = pd.DataFrame(pd.concat(lst,sort=False))
    www = www.fillna(0).reset_index(drop=True)

    # asas = pd.merge(collection_df,www,on=['propertycode','fin_year','fin_month'],how ='left')
    print(True)

    # asas =asas.fillna(0).reset_index(drop=True)
    # abbb = asas[asas['may_default']==1]


if __name__ == '__main__':
    std_path= r"E:\repo\PTAx/"
    inppath = std_path + "Input/"
    outpth = std_path + "output/"

    inpdata = inppath + "collection report.xlsx"

    # consolidate = ptax(inpdata,outpth)
    #
    no_paid_tax(outpth)