import datetime

import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')


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

def property_tax(inppath,outpath):
    property_bill_df = pd.read_csv(inppath + "property_bill.csv",low_memory=False)
    property_list_df = pd.read_csv(inppath + "Property_List.csv",low_memory=False)

    ## Merge the above both df on propertykey using left join
    # property_df = property_bill_df.merge(property_list_df, on ='propertykey',how='left')
    property_df = property_bill_df

    property_df_ = property_df[['propertykey', 'zonekey', 'billdate',
                                'totalbillamount', 'totalbalanceamount',
                                'fromdate', 'todate', 'modeofpayment',
                                'paidamount', 'balanceamount', 'billamount', 'receiptdate']]

    # property_df_ = property_df[['propertykey','propertycode','zonekey','finalusetype',
    #                             'usetypekey','constructiontypekey','subusetypekey','occupancykey',
    #                             'billdate', 'totalbillamount', 'totalbalanceamount',
    #                             'fromdate', 'todate','modeofpayment','paidamount', 'balanceamount',
    #                             'billamount','receiptdate', 'permission',
    #                             'zone','gat']]

    # property_df_ = property_df[['propertykey','propertycode','financialyearkey','zonekey','finalusetype',
    #                             'usetypekey','constructiontypekey','subusetypekey','occupancykey',
    #                             'billdate', 'totalbillamount', 'totalbalanceamount',
    #                             'fromdate', 'todate','modeofpayment','paidamount', 'balanceamount',
    #                             'billamount','receiptdate', 'permission',
    #                             'zone','gat', 'giscode']]

    # property_df_ = property_df[['propertykey', 'financialyearkey',
    #                             'billdate', 'totalbillamount', 'totalbalanceamount',
    #                             'fromdate', 'todate', 'financialyear', 'modeofpayment', 'paidamount', 'balanceamount',
    #                             'billamount', 'receiptdate']]

    ### Unique PID
    unique_values = property_df_.propertykey.unique()
    unique_pid_df = pd.DataFrame({'propertykey': unique_values})

    ### Main DataFrame Property Data
    property_data = property_df_.copy()

    ## find the fromdate (Billed Date) column year or month
    # property_data['fromdate'] = pd.to_datetime(property_data['fromdate'])
    # property_data['fin_year_b'] = property_data['fromdate'].dt.year
    # property_data['fin_month_b'] = property_data['fromdate'].dt.month

    ## find the (receiptdate) column year or month
    property_data['receiptdate'] = pd.to_datetime(property_data['receiptdate'])
    property_data['fin_year_r'] = property_data['receiptdate'].dt.year
    property_data['fin_month_r'] = property_data['receiptdate'].dt.month

    #### find the financial year & Month
    property_data['fin_year_r'] = np.where(property_data['fin_month_r'] <4,property_data['fin_year_r']-1,property_data['fin_year_r'])
    property_data['fin_month_r'] = np.where(property_data['fin_month_r']<4,property_data['fin_month_r']+9,property_data['fin_month_r']-3)

    ## find the GroupBy MAX PID, financial year or month using receipt Date
    property_data_max_FY_year_r = property_data.groupby(['propertykey'])['fin_year_r','fin_month_r'].max().reset_index()

    ## find the GroupBy MAX PID, financial year or month using Billed Date
    # property_data_max_FY_year_B = property_data.groupby(['propertycode'])['fin_year_b','fin_month_b'].max().reset_index()

    # Apply merge using unique property id
    merge_collect_uniqpid_df_Year_r = property_data_max_FY_year_r.merge(unique_pid_df, on='propertykey',how='inner')

    ## drop data if fincial yr is less than 2022
    data = merge_collect_uniqpid_df_Year_r[merge_collect_uniqpid_df_Year_r['fin_year_r'] < 2022]
    filter_data_lessthan_2022 = data.copy()

    ##-------------------------------------------------------------------------------------------------------------------------
    ## find the defaulter list might, may and major
    might_default_df = filter_data_lessthan_2022[(filter_data_lessthan_2022['fin_month_r'] > 9) &
                                                (filter_data_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    may_default_df = filter_data_lessthan_2022[(filter_data_lessthan_2022['fin_month_r'] < 10) &
                                              (filter_data_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    major_default_df = filter_data_lessthan_2022[(filter_data_lessthan_2022['fin_year_r'] < 2021)].reset_index(drop=True)

    Not_paid_yet_df = merge_collect_uniqpid_df_Year_r[(merge_collect_uniqpid_df_Year_r['fin_year_r'].isnull().values)].reset_index(drop=True)
    ##------------/
    might_default_df['paid_last_qtr_LY'] = 1
    may_default_df['paid_by_dec_LY'] = 1
    major_default_df['Not_paid_LY'] = 1
    Not_paid_yet_df['Not_paid_in_4Yrs'] = 1

    # -------------------------------------------------------------------------------------------------------------
    ## Find the partially paid main dataframe PID
    filter_data_greaterthan_2022 = property_data[property_data['fin_year_r'] >= 2022]
    non_zero_balanceamt = filter_data_greaterthan_2022[filter_data_greaterthan_2022['balanceamount'] != 0]

    grp_by_sum_billamt = non_zero_balanceamt.groupby(['propertykey', 'billdate'])[
        'paidamount', 'totalbillamount'].sum().reset_index()
    # non_zero_balanceamt = non_zero_balanceamt.fillna(0)

    grp_by_sum_billamt['percentage'] = round(
        (grp_by_sum_billamt['paidamount'] / grp_by_sum_billamt['totalbillamount']) * 100)

    # ## 0 - Fully Paid
    # ## 1 - Paritially Paid
    grp_by_sum_billamt['partially_paid'] = np.where(grp_by_sum_billamt['percentage']>90,0,1)

    # grp_by_sum_billamt.to_csv(outpath + "partially_paid_pid_TY_List.csv", index=False)
    # might_default_df.to_csv(outpath + "paid_last_qtr_LY_List.csv", index=False)
    # may_default_df.to_csv(outpath + "paid_by_dec_LY_List.csv", index=False)
    # major_default_df.to_csv(outpath + "Not_paid_LY_List.csv", index=False)

   ##-------------------------------------------------------------------------------------------------------------------------

    list_df = [might_default_df,may_default_df,major_default_df]
    lst=[]
    for i in list_df:
        lst.append(i)
    may_might_major_df = pd.DataFrame(pd.concat(lst,sort=False))
    may_might_major_not_df_ = may_might_major_df.fillna(0).reset_index(drop=True)

    final_output = property_data.merge(may_might_major_not_df_, on=['propertykey', 'fin_year_r', 'fin_month_r'], how='left')

    #####---------------------
    final_output_merge = property_data.merge(property_list_df, on ='propertykey',how='left')

    ### drop of year & month column from not paid yet df
    Not_paid_yet_df  =Not_paid_yet_df.drop(columns = ['fin_year_r', 'fin_month_r'])
    # Not_paid_yet_df.to_csv(outpath + "not_paid_in_4Yrs_property_list", index=False)

    ### merge final output with not paid yet in LY 4 yrs
    final_merge = final_output_merge.merge(Not_paid_yet_df, on=['propertykey'], how='left')

    final_output_22 = final_merge.drop(columns='billamount')

    ### renaming the columns
    final_output = final_output_22.rename(
        columns={'totalbillamount': 'billamount',
                 'fin_year_r': 'fin_year',
                 'fin_month_r': 'fin_month'})

    usertype = pd.read_csv(inppath + "usetype.csv")
    uuu = dict(zip(usertype['usetypekey'],usertype['eng_usename']))
    final_output['User_Type'] = final_output['usetypekey'].map(uuu)

    consttype = pd.read_csv(inppath + "constructiontype.csv")
    ccc = dict(zip(consttype['constructiontypekey'],consttype['eng_constructiontypename']))
    final_output['Construction_Type'] = final_output['constructiontypekey'].map(ccc)

    occptype=  pd.read_csv(inppath + "occupancy.csv")
    ooo = dict(zip(occptype['occupancykey'],occptype['occupancyname']))
    final_output['Occupancy_Type'] = final_output['occupancykey'].map(ooo)

    subusetype= pd.read_csv(inppath + "subusetype.csv")
    sss = dict(zip(subusetype['subusetypekey'],subusetype['eng_subusename']))
    final_output['Subset_Type'] = final_output['subusetypekey'].map(sss)

    zonetype =pd.read_csv(inppath + "zone.csv")
    zzz = dict(zip(zonetype['zonekey'],zonetype['eng_zonename']))
    final_output['Zone_Type'] = final_output['zonekey'].map(zzz)

    fnal = final_output[['propertykey', 'propertycode', 'Zone_Type', 'gat', 'modeofpayment','User_Type',
      'billdate', 'receiptdate', 'billamount', 'paidamount', 'Subset_Type','Construction_Type',
                         'Occupancy_Type','Not_paid_in_4Yrs']]

    nnpaid_4yrs_ly = fnal[fnal['Not_paid_in_4Yrs'] == 1]

    sum_grpby111 = nnpaid_4yrs_ly.groupby(
        ['propertykey', 'propertycode', 'gat', 'Construction_Type', 'User_Type', 'Occupancy_Type'])[
        'billamount'].sum().reset_index()

    sum_grpby111.to_csv(outpath + "test0345.csv", index=False)

    #### Rearrange the columns
    # final_output_ = final_output[['propertykey', 'propertycode', 'zone', 'gat', 'giscode', 'modeofpayment',
    #                              'billdate', 'receiptdate', 'billamount', 'paidamount',
    #                              'fin_year', 'fin_month', 'paid_last_qtr_LY', 'paid_by_dec_LY', 'Not_paid_LY',
    #                              'Not_paid_in_4Yrs']]
    # final_output_ = final_output[['propertykey', 'propertycode', 'zone', 'gat', 'giscode', 'modeofpayment',
    #                               'billdate', 'receiptdate', 'billamount', 'paidamount', 'usetypekey',
    #                               'constructiontypekey', 'permission',
    #                               'fin_year', 'fin_month', 'paid_last_qtr_LY', 'paid_by_dec_LY', 'Not_paid_LY',
    #                               'Not_paid_in_4Yrs']]

    # not_paid_4yrs_ly =final_output_[final_output_['Not_paid_in_4Yrs']==1]
    # sum_grpby =  not_paid_4yrs_ly.groupby(['propertykey','propertycode', 'zone', 'gat'])['billamount'].sum().reset_index()
    # sum_grpby111 = kkk.groupby(['propertykey', 'propertycode', 'zone', 'gat','constructiontypename','username','permission'])['billamount'].sum().reset_index()

    # notpresn = pd.read_csv(inppath + "notpresentkey.csv")
    # notpresn['key']=1
    # mmm = final_output.merge(notpresn,on='propertykey',how='left')

    # [['propertykey', 'propertycode', 'zone', 'gat', 'giscode', 'modeofpayment',
    #   'billdate', 'receiptdate', 'billamount', 'paidamount', 'usetypekey',
    #   'constructiontypekey', 'permission']]
    # sum_grpby.to_csv(outpath + "Not_paid_in_4Yrs_list.csv", index=False)

    # final_output_ = final_output[['propertykey', 'modeofpayment',
    #                              'billdate', 'receiptdate', 'billamount', 'paidamount',
    #                              'fin_year', 'fin_month', 'paid_last_qtr_LY', 'paid_by_dec_LY', 'Not_paid_LY',
    #                              'Not_paid_in_4Yrs']]

    ## final df dump in to csv
    # final_output_.to_csv(outpath + "pbiconsolidate_pbill.csv", sep="|", index=False)

    # print(len(final_output_.info()))




    # # Find unique values and their count
    # # unique_values, count = np.unique(collection2018_23_df['propertycode'], return_counts=True)
    # # unique_pid_df = pd.DataFrame({'propertycode': unique_values, 'count': count})
    # unique_values = collection2018_23_df.propertycode.unique()
    # unique_pid_df = pd.DataFrame({'propertycode': unique_values})
    # unique_pid_df['propertycode'] = unique_pid_df['propertycode'].str.replace("/", "S")
    #
    # ##find the tax_paid_year & tax_paid_month
    # collection_df = collection2018_23_df.copy()
    # collection_df['propertycode'] = collection_df['propertycode'].str.replace("/", "S")
    #
    # collection_df['fin_year'] = np.where(collection_df['Month'] <4,collection_df['Year']-1,collection_df['Year'])
    # collection_df['fin_month'] = np.where(collection_df['Month']<4,collection_df['Month']+9,collection_df['Month']-3)
    #
    # ## find the max year or month
    # collection_df_maxfyyear = collection_df.groupby(['propertycode'])['fin_year','fin_month'].max().reset_index()
    #
    # # df_new =df_new.reset_index(drop=True)
    # # Apply merge using unique property id
    # merge_collect_uniqpid_df = unique_pid_df.merge(collection_df_maxfyyear, on='propertycode',how='inner')
    #
    # ## drop data if fincial yr is less than 2022
    # data = merge_collect_uniqpid_df[merge_collect_uniqpid_df['fin_year'] < 2022]
    #
    # filter_data_lessthan2022 = data.copy()
    #
    # ## find the defualter list might, may and major
    # might_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_month'] > 9) &
    #                                             (filter_data_lessthan2022['fin_year'] == 2021)].reset_index(drop=True)
    # may_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_month'] < 10) &
    #                                           (filter_data_lessthan2022['fin_year'] == 2021)].reset_index(drop=True)
    # major_default_df = filter_data_lessthan2022[(filter_data_lessthan2022['fin_year'] < 2021)].reset_index(drop=True)
    #
    # ## set the deafault value as 1
    # might_default_df['might_default'] =1
    # may_default_df['may_default'] = 1
    # major_default_df['major_default'] =1

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

    # aaaaa = [might_default_df,may_default_df,major_default_df]
    # lst=[]
    # for i in aaaaa:
    #     lst.append(i)
    # www = pd.DataFrame(pd.concat(lst,sort=False))
    # www = www.fillna(0).reset_index(drop=True)

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

    property_tax(inppath,outpth)