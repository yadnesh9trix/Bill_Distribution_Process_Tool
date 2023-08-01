import pandas as pd
import numpy as np
from datetime import datetime,timedelta
import warnings
warnings.filterwarnings('ignore')

## Define the today's date
today = datetime.today().date()
last = today - timedelta(days=6)
tdy_fmt  =last.strftime("%d%m%Y")


def collection_data(inppath):
    ## Read YTD data
    ytddata = pd.read_excel(inppath + f"Paidamount_list_{tdy_fmt}.xlsx", sheet_name="Total")
    ## select only pid column from ytd data
    # ytddata = ytddata[['propertycode']]

    ## Replace the property code values in ytd data
    ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
    ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)
    ytddata = ytddata.drop_duplicates('propertycode')
    ytddata.dropna(subset=['propertycode'], how='all', inplace=True)

    return ytddata


def new_arrearsBilllist(outpth,inppath,ytddata_df, property_bill_df, property_list_df,
                    zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap):

    property_list_df.dropna(subset=['propertycode'], how='all', inplace=True)
    property_list_df.dropna(subset=['propertykey'], how='all', inplace=True)
    # ###------------------------------------------------------------------------------------------------------------------------------------------------------------
    ## Starting the Property bill data
    property_bill_df_selcted = property_bill_df[['propertykey', 'propertybillkey', 'financialyearkey', 'fromdate',
                             'billamount', 'balanceamount']]
    property_bill_df_NonZero = property_bill_df_selcted[property_bill_df_selcted['balanceamount'] > 0]
    property_bill_df_NonZeropkey = property_bill_df_NonZero[property_bill_df_NonZero['propertykey'] > 0]

    # zzz = property_bill_df[property_bill_df['propertykey'] == 550203]
    # ###------------------------------------------------------------------------------------------------------------------------------------------------------------
    ##Current Arrears till 2021
    Not_TY = property_bill_df_NonZeropkey[property_bill_df_NonZeropkey['financialyearkey'] != 152]
    arrears_TY = Not_TY.groupby(['propertykey'])['balanceamount'].sum().reset_index()
    arrears_TY = arrears_TY.rename(columns={'balanceamount': 'Arrears'})
    arrears_TY['Arrears_Flag'] = 1
    # 'Arrears', 'Arrearsfind1', 'Current_Bill', 'CurrentBillfind1'
    #Current Year arrears
    only_TY = property_bill_df_NonZeropkey[property_bill_df_NonZeropkey['financialyearkey'] == 152]
    current_TY = only_TY.groupby(['propertykey'])['balanceamount'].sum().reset_index()
    current_TY = current_TY.rename(columns={'balanceamount': 'Current_Bill'})
    current_TY['CurrentBill_Flag'] = 1
    ##Read Property list
    plist_df = property_list_df[['propertykey', 'propertycode']]
    plist_df['propertykey'] = plist_df['propertykey'].drop_duplicates()
    plist_df['propertycode'] = plist_df['propertycode'].drop_duplicates()

    ##Merge Property list with cuurent arrears & arrears
    merge_plist_arrearsTY = plist_df.merge(arrears_TY, on='propertykey', how='left')
    merge_plist_arrearsTY1 = merge_plist_arrearsTY.merge(current_TY, on='propertykey', how='left')
    # ###------------------------------------------------------------------------------------------------------------------------------------------------------------
    # ## Read YTD data
    # ytddata = pd.read_excel(inppath + f"Paidamount_list_{tdy_fmt}.xlsx", sheet_name="Total")
    # ## select only pid column from ytd data
    # ## Replace the property code values in ytd data
    # ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
    # ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    # ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)
    # ytddata = ytddata.drop_duplicates('propertycode')
    # ytddata.dropna(subset=['propertycode'], how='all', inplace=True)
    ytd_collection_data = ytddata_df.copy()
    ytd_collection_data = ytd_collection_data[['propertycode']]
    ytd_collection_data['paid_in_TY'] = 1

    merge_plist_arrearsTY1.dropna(subset=['propertykey'], how='all', inplace=True)
    ## Merge ytd data
    merge_ytddata_mergepdaatplist = merge_plist_arrearsTY1.merge(ytd_collection_data, on='propertycode',
                                                                                        how='left')
    ## find PID without paid in TY
    wioutpaidinTY_data1 = merge_ytddata_mergepdaatplist[merge_ytddata_mergepdaatplist['paid_in_TY'] != 1]
    wioutpaidinTY_data1['Not_paid_in_TY'] = 1

    return wioutpaidinTY_data1

    # ==========================================================================================================================================================================
def newlist_receiptdate(outpth,inppath,ytddata_df, property_receipt_df,property_list_df,unique_pkey_df,AllBillArrears,
                        zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap):

    plist_arrange = property_list_df[['propertykey', 'propertycode']]
    plist_arrange['propertykey'] = plist_arrange['propertykey'].drop_duplicates()
    plist_arrange['propertycode'] = plist_arrange['propertycode'].drop_duplicates()

    # ###------------------------------------------------------------------------------------------------------------------------------------------------------------
    property_fin_yrmth_df = property_receipt_df.copy()
    ## find the (receiptdate) column year or month
    property_fin_yrmth_df['receiptdate'] = pd.to_datetime(property_fin_yrmth_df['receiptdate'],errors = 'coerce',format='%Y-%m-%d')
    property_fin_yrmth_df['fin_year_r'] = property_fin_yrmth_df['receiptdate'].dt.year
    property_fin_yrmth_df['fin_month_r'] = property_fin_yrmth_df['receiptdate'].dt.month

    property_finmth_df = property_fin_yrmth_df.copy()
    property_finmth_df['fin_year_r'] = np.where(property_finmth_df['fin_month_r'] < 4,
                                                property_finmth_df['fin_year_r'] - 1,
                                                property_finmth_df['fin_year_r'])
    property_finmth_df['fin_month_r'] = np.where(property_finmth_df['fin_month_r'] < 4,
                                                 property_finmth_df['fin_month_r'] + 9,
                                                 property_finmth_df['fin_month_r'] - 3)
    # property_finmth_df = property_finmth_df.fillna(0)
    property_finmth_df['fin_year_r'] = property_finmth_df['fin_year_r'].fillna(1900)
    property_finmth_df['fin_month_r'] = property_finmth_df['fin_month_r'].fillna(0)

    property_finmth_df = property_finmth_df.astype({'fin_year_r': 'int', 'fin_month_r': 'int'})
    property_finmth_df['fin_month_r'] = property_finmth_df['fin_month_r'] / 100

    property_finmth_df['fin_yearmth_r'] = property_finmth_df['fin_year_r'] + property_finmth_df['fin_month_r']
    ## Find max fin year & month r
    property_fyrmth_r_max = property_finmth_df.groupby(['propertykey'])['fin_yearmth_r'].max().reset_index()
    # property_fyrmth_r_max = property_fin_yrmth_df.groupby(['propertykey'])[
    #     'fin_year_r', 'fin_month_r'].max().reset_index()

    # Apply merge using unique property id
    merge_maxfyrmth_uniqpid_df_Year_r = property_fyrmth_r_max.merge(unique_pkey_df, on='propertykey', how='inner')

    ## 02/03/2023 We added the fin year & month replaced by 1999 & 0    as suggested by SK sir
    # merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] = merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'].fillna(1900)
    # merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'] = merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'].fillna(0)

    merge_maxfyrmth_uniqpid_df_Year_r['fin_yearmth_r'] = merge_maxfyrmth_uniqpid_df_Year_r['fin_yearmth_r'].astype(str)
    merge_maxfyrmth_uniqpid_df_Year_r[['fin_year_r', 'fin_month_r']] = merge_maxfyrmth_uniqpid_df_Year_r[
        'fin_yearmth_r'].str.split(".", expand=True)
    merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'] = np.where(merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'] == '1',
                                                                '10', merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'])
    merge_maxfyrmth_uniqpid_df_Year_r = merge_maxfyrmth_uniqpid_df_Year_r.astype(
        {'fin_year_r': 'int', 'fin_month_r': 'int'})

    ## drop data if financial year is less than 2022
    filterdata_lessthan_2022 = merge_maxfyrmth_uniqpid_df_Year_r[
        merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] != 2022]
    filterdata_lessthan_2022[['fin_year_r', 'fin_month_r']] = filterdata_lessthan_2022[
        'fin_yearmth_r'].str.split(".", expand=True)
    filterdata_lessthan_2022['fin_month_r'] = np.where(filterdata_lessthan_2022['fin_month_r'] == '1',
                                                                '10', filterdata_lessthan_2022['fin_month_r'])
    filterdata_lessthan_2022 = filterdata_lessthan_2022.astype(
        {'fin_year_r': 'int', 'fin_month_r': 'int'})

    ###==================================================================================================================================
    ## Read YTD data
    # ytddata = pd.read_excel(inppath + f"Paidamount_list_{tdy_fmt}.xlsx", sheet_name="Total")
    # ## select only pid column from ytd data
    # ytddata = ytddata[['propertycode']]
    #
    # ## Replace the property code values in ytd data
    # ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
    # ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    # ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)
    # ytddata = ytddata.drop_duplicates('propertycode')
    # ytddata.dropna(subset=['propertycode'], how='all', inplace=True)

    collection_data = ytddata_df.copy()
    collection_data = collection_data[['propertycode']]
    collection_data['paid_in_TY'] = 1
    # merge_maxfyrmth_uniqpid_df_Year_r.dropna(subset=['propertycode'], how='all', inplace=True)
    merge_maxfyrmth_uniqpid_df_Year_r.dropna(subset=['propertykey'], how='all', inplace=True)
    ## merge property list with max finacial yr & month
    merge_plist_merge_maxfyrmth_uniqpid_df_Year_r = plist_arrange.merge(merge_maxfyrmth_uniqpid_df_Year_r,
                                                                        on='propertykey', how='left')
    # merge
    merge_ytddata_mergepdaatplist = merge_plist_merge_maxfyrmth_uniqpid_df_Year_r.merge(collection_data, on='propertycode',
                                                                                        how='left')
    ## find PID without paid in TY
    wioutpaidinTY_data1 = merge_ytddata_mergepdaatplist[merge_ytddata_mergepdaatplist['paid_in_TY'] != 1]
    paidinTY_data1 = merge_ytddata_mergepdaatplist[merge_ytddata_mergepdaatplist['paid_in_TY'] == 1]

    wioutpaidinTY_data1['Not_paid_in_TY'] = 1
    ## identify not paid in TY
    not_paid_InTY = wioutpaidinTY_data1[wioutpaidinTY_data1['Not_paid_in_TY'] == 1]

    # find since 2019 sum of totalbillamount using billyear
    yr2019_2021_billamt = not_paid_InTY[(not_paid_InTY['fin_year_r'] >= 2019) & (not_paid_InTY['fin_year_r'] <= 2021)]
    yr2019_2021_billamt = yr2019_2021_billamt.drop(
        columns=['fin_year_r', 'fin_month_r', 'fin_yearmth_r','paid_in_TY', 'Not_paid_in_TY', 'propertycode'])
    yr2019_2021_billamt['Arrears_from2019'] = 1

    # find since 2017 sum of totalbillamount using billyear
    yr2017_2021_billamt = not_paid_InTY[(not_paid_InTY['fin_year_r'] >= 2017) & (not_paid_InTY['fin_year_r'] <= 2021)]
    yr2017_2021_billamt = yr2017_2021_billamt.drop(
        columns=['fin_year_r', 'fin_month_r', 'fin_yearmth_r', 'paid_in_TY', 'Not_paid_in_TY', 'propertycode'])
    yr2017_2021_billamt['Arrears_from2017'] = 1

    # wioutpaidinTY_data1 = wioutpaidinTY_data1.drop(
    #     columns=['fin_year_r', 'fin_month_r', 'paid_in_TY', 'propertycode'])

    paidinTY_data1 = paidinTY_data1.drop(
        columns=['fin_year_r', 'fin_month_r','fin_yearmth_r','propertycode'])

    plist_dff = property_list_df[['propertykey', 'propertycode']]
    merege_defaulters_df4 = plist_dff.merge(yr2019_2021_billamt, on='propertykey', how='left')
    merege_defaulters_df5 = merege_defaulters_df4.merge(yr2017_2021_billamt, on='propertykey', how='left')
    merege_defaulters_df6 = merege_defaulters_df5.merge(paidinTY_data1, on='propertykey', how='left')

    ## Paid in March LY
    paid_bymarch_LY = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_month_r'] == 12) &
         (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    paid_bymarch_LY['paid_in_March_LY'] = 1
    paid_bymarch_LY = paid_bymarch_LY.drop(columns=['fin_year_r', 'fin_month_r','fin_yearmth_r'])
    paid_bymarch_LY['propertykey'] = paid_bymarch_LY['propertykey'].drop_duplicates()

    ## Paid in LY
    paid_by_LY = filterdata_lessthan_2022 \
        [filterdata_lessthan_2022['fin_year_r'] == 2021].reset_index(drop=True)
    paid_by_LY['paid_in_LY'] = 1
    paid_by_LY = paid_by_LY.drop(columns=['fin_year_r', 'fin_month_r','fin_yearmth_r'])
    paid_by_LY['propertykey'] = paid_by_LY['propertykey'].drop_duplicates()

    ## find not paid in LY
    Not_paid_LY_df = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_year_r'] < 2021)].reset_index(drop=True)
    Not_paid_LY_df['not_paid_LY'] = 1
    Not_paid_LY_df = Not_paid_LY_df.drop(columns=['fin_year_r', 'fin_month_r','fin_yearmth_r'])
    Not_paid_LY_df['propertykey'] = Not_paid_LY_df['propertykey'].drop_duplicates()

    ### find paid by dec LY
    paid_by_dec_LY = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_month_r'] < 10) &
         (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    paid_by_dec_LY['propertykey'] = paid_by_dec_LY['propertykey'].drop_duplicates()
    paid_by_dec_LY['paid_by_dec_LY'] = 1
    paid_by_dec_LY = paid_by_dec_LY.drop(columns=['fin_year_r', 'fin_month_r','fin_yearmth_r'])

    finL_output_df1 = merege_defaulters_df6.merge(paid_bymarch_LY, on='propertykey', how='left')
    finL_output_df2_2 = finL_output_df1.merge(Not_paid_LY_df, on='propertykey', how='left')
    finL_output_df2 = finL_output_df2_2.merge(paid_by_LY, on='propertykey', how='left')
    finL_output_df3 = finL_output_df2.merge(paid_by_dec_LY, on='propertykey', how='left')
    # -------------------------------------------------------------------------------------------------
    # Total Arrears & current Bill
    all_Bill_arrers = AllBillArrears[['propertykey', 'Arrears', 'Arrearsfind1',
                                      'Current_Bill', 'CurrentBillfind1']]
    finLDUMP = finL_output_df3.merge(all_Bill_arrers, on='propertykey', how='left')
    finLDUMP =  finLDUMP.fillna(0)
    # -------------------------------------------------------------------------------------------------
    property_finmth_df11 = property_finmth_df[['propertykey','receiptdate','paidamount']]
    # maxreceiptdate = property_finmth_df11.groupby(['propertykey'])['receiptdate'].max().reset_index()
    maxreceiptdate = property_finmth_df11.sort_values(['propertykey', 'receiptdate']).drop_duplicates('propertykey', keep='last')
    merge_maxreciptdate_finaldump = finLDUMP.merge(maxreceiptdate, on='propertykey', how='left')
    merge_maxreciptdate_finaldump['receiptdate'] = merge_maxreciptdate_finaldump['receiptdate'].dt.date
    ## 05-04-2023
    # merge_maxreciptdate_finaldump['receiptdate'] = merge_maxreciptdate_finaldump['receiptdate'].fillna("1982-01-01")

    property_assessment_date =pd.read_csv(inppath + "Property_Assessment_Date.csv",low_memory=False)
    merge_pdata_assmentsdate = merge_maxreciptdate_finaldump.merge(property_assessment_date,on='propertykey',how='left')
    merge_pdata_assmentsdate['receiptdate'] = merge_pdata_assmentsdate['receiptdate'].fillna(merge_pdata_assmentsdate['assessmentdate'])

###==================================================================================================================================================
    collection_df =  ytddata_df.copy()
    # ytddata_df = pd.read_excel(inppath + f"Paidamount_list_{tdy_fmt}.xlsx", sheet_name="Total")
    # ## Replace the property code values in ytd data
    # ytddata_df["propertycode"] = ytddata_df["propertycode"].replace("1100900002.10.10", "1100900002.20")
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].astype(float)
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].apply("{:.02f}".format)
    # ytddata_df = ytddata_df.drop_duplicates('propertycode')
    # ytddata_df.dropna(subset=['propertycode'], how='all', inplace=True)
    collection_df['receiptdate'] = collection_df['receiptdate'].astype(str)
    merge_pdata_assmentsdate['receiptdate'] = merge_pdata_assmentsdate['receiptdate'].astype(str)
    ## select only pid column from ytd data
    collection_df = collection_df.rename(columns={'receiptdate': 'receiptdate_typaid', 'paidamount': 'paidamount_TY'})
    ytddata1 = collection_df[['propertycode', 'receiptdate_typaid', 'paidamount_TY']]
    # plist_rearrange_data.update(ytddata)
    # ytddata_selected = ytddata_df[['propertycode', 'paidamount'] ]
    plist_rearrange_data1 = merge_pdata_assmentsdate.merge(ytddata1, on='propertycode', how='left')

    plist_rearrange_data1['new update date'] = plist_rearrange_data1['receiptdate_typaid'].fillna(plist_rearrange_data1['receiptdate'])
    plist_rearrange_data1['new update paidamount'] = plist_rearrange_data1['paidamount_TY'].fillna(plist_rearrange_data1['paidamount'])

    # plist_rearrange_data1['new update date'] = pd.to_datetime(plist_rearrange_data1['new update date'],
    #                                                           errors='coerce').dt.date
    #------------------------------------------------------------------------------------------------------------------------------------------
    # NoArrears_OnlyCurrentBills = pd.read_excel(inppath + "NoRemmaningArrears_OnlyCurrentBill_Plist(75k).xlsx",
    #                                            sheet_name='TotalCurrentBillPlist')
    # NoArrears_OnlyCurrentBills['propertycode'] = NoArrears_OnlyCurrentBills['propertycode'].str.replace("/", ".")
    # NoArrears_OnlyCurrentBills = NoArrears_OnlyCurrentBills.drop_duplicates('propertycode')
    # NoArrears_OnlyCurrentBills['NoArrears_OnlyCurrentBills_Flag'] = 1
    #
    # NoArrears_OnlyCurrentBills = NoArrears_OnlyCurrentBills[['propertycode', 'current',
    #                                                          'NoArrears_OnlyCurrentBills_Flag']]
    # mereg_NoArrears_OnlyCurrentBills_ = plist_rearrange_data1.merge(NoArrears_OnlyCurrentBills, on='propertycode', how='left')
    #------------------------------------------------------------------------------------------------------------------------------------------
    ppp = property_list_df[
        ['propertykey', 'zone', 'gat', 'propertyname', 'propertyaddress', 'usetypekey', 'finalusetype',
         'subusetypekey', 'constructiontypekey', 'occupancykey', 'own_mobile']]
    ppp['propertykey'] = ppp['propertykey'].drop_duplicates()

    plist_rearrange_data = ppp.merge(plist_rearrange_data1, on='propertykey', how='inner')

    plist_rearrange_data['own_mobile'] = plist_rearrange_data['own_mobile'].fillna('Not Available')
    plist_rearrange_data['Use_Type'] = plist_rearrange_data['usetypekey'].map(usemap)
    plist_rearrange_data['Construction_Type'] = plist_rearrange_data['constructiontypekey'].map(construcmap)
    plist_rearrange_data['Occupancy_Type'] = plist_rearrange_data['occupancykey'].map(occpmap)
    plist_rearrange_data['Subuse_Type'] = plist_rearrange_data['subusetypekey'].map(subusemap)
    plist_rearrange_data['Zone_Type'] = plist_rearrange_data['zone'].map(zonemap)
    plist_rearrange_data['gat_name'] = plist_rearrange_data['gat'].map(gatnamemap)
    #------------------------------------------------------------------------------------------------------------------------------------------
    # plist_data = plist_rearrange_data[['propertykey', 'Zone_Type',
    #                                                'gat_name', 'propertycode', 'receiptdate', 'paidamount',
    #                                                'receiptdate_typaid', 'paidamount_TY','new update date','new update paidamount', 'Arrears', 'Arrearsfind1',
    #                                                'Current_Bill','CurrentBillfind1','current', 'NoArrears_OnlyCurrentBills_Flag',
    #                                                'Arrears_from2019', 'Arrears_from2017', 'paid_in_March_LY',
    #                                                'paid_in_LY','paid_in_TY', 'not_paid_LY', 'paid_by_dec_LY', 'Use_Type',
    #                                                'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'propertyname',
    #                                                'own_mobile', 'propertyaddress']]
    plist_data = plist_rearrange_data[['propertykey', 'Zone_Type',
                                                   'gat_name', 'propertycode', 'receiptdate', 'paidamount',
                                                   'receiptdate_typaid', 'paidamount_TY','new update date','new update paidamount', 'Arrears', 'Arrearsfind1',
                                                   'Current_Bill','CurrentBillfind1',
                                                   'Arrears_from2019', 'Arrears_from2017', 'paid_in_March_LY',
                                                   'paid_in_LY','paid_in_TY', 'not_paid_LY', 'paid_by_dec_LY', 'Use_Type',
                                                   'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'propertyname',
                                                   'own_mobile', 'propertyaddress']]
    # ====================================================================================================================================================================
    # plist_data.to_excel(outpth + "/" + f"OnlyCurrentBills_Plist_Data(75K).xlsx", index=False,
    #                                sheet_name="OnlyCurrentBills_Plist_Data")
    # property_assessment_date =pd.read_csv(inppath + "Property_Assessment_Date.csv",low_memory=False)
    # merge_pdata_assmentsdate = plist_data.merge(property_assessment_date,on='propertykey',how='left')

    plist_data.to_excel(outpth + "/" + f"Master_Plist_Data1{tdy_fmt}.xlsx", index=False,
                                   sheet_name="MasterArrearsPlist")
    # #---------------------------------------------------------------------------------------------------------------------------------------
    #------------------------------------------------------------------------------------------------------------------------------------------
    # illegal_fine_plist = pd.read_excel(inppath + "illegal Property data.(98223).xlsx")
    # illegal_fine_plist['propertycode'] = illegal_fine_plist['propertycode'].str.replace("/", ".")
    # illegal_fine_plist = illegal_fine_plist.drop_duplicates('propertycode')
    #
    # illegal_fine_plist['illegal_flag'] = "illegal_property"
    # # illegal_fine_plist.columns
    # illegal_fine_plist = illegal_fine_plist[
    #     ['propertycode', 'illegalarrearsbal', 'illegalcurrentbal', 'illegaltotalbal', 'otherstotalbal',
    #      'illegal_flag']]
    # merge_df = merge_maxreciptdate_finaldump.merge(illegal_fine_plist, on='propertycode', how='left')
    # #------------------------------------------------------------------------------------------------------------------------------------------
    # NoArrears_OnlyCurrentBills = pd.read_excel(inppath + "NoRemmaningArrears_OnlyCurrentBill_Plist(75k).xlsx",sheet_name='TotalCurrentBillPlist')
    # NoArrears_OnlyCurrentBills['propertycode'] = NoArrears_OnlyCurrentBills['propertycode'].str.replace("/", ".")
    # NoArrears_OnlyCurrentBills = NoArrears_OnlyCurrentBills.drop_duplicates('propertycode')
    # NoArrears_OnlyCurrentBills['NoArrears_OnlyCurrentBills_Flag'] = 1
    #
    # NoArrears_OnlyCurrentBills = NoArrears_OnlyCurrentBills[['propertycode', 'current',
    #                                                          'NoArrears_OnlyCurrentBills_Flag']]
    # mereg_NoArrears_OnlyCurrentBills_ = merge_df.merge(NoArrears_OnlyCurrentBills, on='propertycode', how='left')
    # #------------------------------------------------------------------------------------------------------------------------------------------
    #
    # ppp = property_list_df[
    #     ['propertykey', 'zone', 'gat', 'propertyname', 'propertyaddress', 'usetypekey', 'finalusetype',
    #      'subusetypekey', 'constructiontypekey', 'occupancykey', 'own_mobile']]
    # ppp['propertykey'] = ppp['propertykey'].drop_duplicates()
    #
    # plist_rearrange_data = ppp.merge(mereg_NoArrears_OnlyCurrentBills_, on='propertykey', how='inner')
    # plist_rearrange_data['own_mobile'] = plist_rearrange_data['own_mobile'].fillna('Not Available')
    # plist_rearrange_data['Use_Type'] = plist_rearrange_data['usetypekey'].map(usemap)
    # plist_rearrange_data['Construction_Type'] = plist_rearrange_data['constructiontypekey'].map(construcmap)
    # plist_rearrange_data['Occupancy_Type'] = plist_rearrange_data['occupancykey'].map(occpmap)
    # plist_rearrange_data['Subuse_Type'] = plist_rearrange_data['subusetypekey'].map(subusemap)
    # plist_rearrange_data['Zone_Type'] = plist_rearrange_data['zone'].map(zonemap)
    # plist_rearrange_data['gat_name'] = plist_rearrange_data['gat'].map(gatnamemap)
    # # plist_rearrange_data['specialowner_Type'] = plist_rearrange_data['specialownership'].map(splownmap)
    # # plist_rearrange_data['specialoccupant_Type'] = plist_rearrange_data['specialoccupantkey'].map(splaccmap)
    #
    # ytddata_df = pd.read_excel(inppath + "Paidamount_list_16032023.xlsx", sheet_name="Total")
    # ## Replace the property code values in ytd data
    # ytddata_df["propertycode"] = ytddata_df["propertycode"].replace("1100900002.10.10", "1100900002.20")
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].astype(float)
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].apply("{:.02f}".format)
    # ytddata_df = ytddata_df.drop_duplicates('propertycode')
    # ytddata_df.dropna(subset=['propertycode'], how='all', inplace=True)
    # ytddata_df['receiptdate'] = ytddata_df['receiptdate'].astype(str)
    # plist_rearrange_data['receiptdate'] = plist_rearrange_data['receiptdate'].astype(str)
    # ## select only pid column from ytd data
    # ytddata_df = ytddata_df.rename(columns={'receiptdate': 'receiptdate_typaid', 'paidamount': 'paidamount_TY'})
    # ytddata = ytddata_df[['propertycode', 'receiptdate_typaid', 'paidamount_TY']]
    # # plist_rearrange_data.update(ytddata)
    # # ytddata_selected = ytddata_df[['propertycode', 'paidamount']]
    # plist_rearrange_data1 = plist_rearrange_data.merge(ytddata, on='propertycode', how='left')
    #
    # # aa = plist_rearrange_data1[plist_rearrange_data1['illegal_flag'] == 'illegal_property']
    #
    # plist_rearrange_data1 = plist_rearrange_data1[['propertykey', 'Zone_Type',
    #                                                'gat_name', 'propertycode', 'receiptdate', 'paidamount',
    #                                                'receiptdate_typaid', 'paidamount_TY', 'Arrears', 'Arrearsfind1',
    #                                                'Current_Bill','CurrentBillfind1','illegal_flag', 'illegalarrearsbal', 'illegalcurrentbal',
    #                                                'illegaltotalbal', 'otherstotalbal','current', 'NoArrears_OnlyCurrentBills_Flag',
    #                                                'Arrears_from2019', 'Arrears_from2017', 'paid_in_March_LY',
    #                                                'paid_in_LY','paid_in_TY', 'not_paid_LY', 'paid_by_dec_LY', 'Use_Type',
    #                                                'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'propertyname',
    #                                                'own_mobile', 'propertyaddress']]
    # # ====================================================================================================================================================================
    # plist_rearrange_data1.to_excel(outpth + "/" + f"Master_Plist_Data(17032023).xlsx", index=False,
    #                                sheet_name="MasterArrearsPlist")

    # #---------------------------------------------------------------------------------------------------------------------------------------
    # # Illegal Property list 18706 merge
    # illegal_fine_plist = pd.read_excel(inppath + "Illegal_Shasti_Plist(18706).xlsx")
    # illegal_fine_plist['propertycode'] = illegal_fine_plist['propertycode'].str.replace("/", ".")
    # illegal_fine_plist['illegal_flag'] = "illegal_property"
    # illegal_fine_plist = illegal_fine_plist[
    #     ['propertycode', 'illegaltotalbal', 'otherstotalbal', 'illegal_flag']]
    # merge_illegalplist_fnal = merge_maxreciptdate_finaldump.merge(illegal_fine_plist, on='propertycode', how='left')
    # merge_illegalplist_fnal =merge_illegalplist_fnal.rename(columns={'otherstotalbal':'Actual_illegalBalance'})
    # #---------------------------------------------------------------------------------------------------------------------------------------
    # ppp = property_list_df[['propertykey', 'zone','gat','propertyname', 'propertyaddress', 'usetypekey', 'finalusetype',
    #                         'subusetypekey', 'constructiontypekey', 'occupancykey', 'own_mobile']]
    # # plist_rearrange_data = ppp.merge(finLDUMP, on='propertykey', how='left')
    # #---------------------------------------------------------------------------------------------------------------------------------------
    # # plist_rearrange_data = ppp.merge(merge_maxreciptdate_finaldump, on='propertykey', how='left')
    # plist_rearrange_data = ppp.merge(merge_illegalplist_fnal, on='propertykey', how='left')
    #
    # #-------------------------------------------------------------------------------------------------
    # plist_rearrange_data['own_mobile'] = plist_rearrange_data['own_mobile'].fillna('Not Available')
    # plist_rearrange_data['Use_Type'] = plist_rearrange_data['usetypekey'].map(usemap)
    # plist_rearrange_data['Construction_Type'] = plist_rearrange_data['constructiontypekey'].map(construcmap)
    # plist_rearrange_data['Occupancy_Type'] = plist_rearrange_data['occupancykey'].map(occpmap)
    # plist_rearrange_data['Subuse_Type'] = plist_rearrange_data['subusetypekey'].map(subusemap)
    # plist_rearrange_data['Zone_Type'] = plist_rearrange_data['zone'].map(zonemap)
    # plist_rearrange_data['gat_name'] = plist_rearrange_data['gat'].map(gatnamemap)
    # # plist_rearrange_data['specialowner_Type'] = plist_rearrange_data['specialownership'].map(splownmap)
    # # plist_rearrange_data['specialoccupant_Type'] = plist_rearrange_data['specialoccupantkey'].map(splaccmap)
    #
    # ytddata_df = pd.read_excel(inppath + "Paidamount_list_14032023.xlsx", sheet_name="Total")
    # ## Replace the property code values in ytd data
    # ytddata_df["propertycode"] = ytddata_df["propertycode"].replace("1100900002.10.10", "1100900002.20")
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].astype(float)
    # ytddata_df['propertycode'] = ytddata_df['propertycode'].apply("{:.02f}".format)
    # # ytddata = ytddata.drop_duplicates('propertycode')
    # ytddata_df.dropna(subset=['propertycode'], how='all', inplace=True)
    # ytddata_df['receiptdate'] = ytddata_df['receiptdate'].astype(str)
    # plist_rearrange_data['receiptdate'] = plist_rearrange_data['receiptdate'].astype(str)
    # ## select only pid column from ytd data
    # ytddata_df = ytddata_df.rename(columns= {'receiptdate':'receiptdate_typaid','paidamount':'paidamount_TY'})
    # ytddata = ytddata_df[['propertycode','receiptdate_typaid','paidamount_TY']]
    # # plist_rearrange_data.update(ytddata)
    # # ytddata_selected = ytddata_df[['propertycode', 'paidamount']]
    # plist_rearrange_data1 = plist_rearrange_data.merge(ytddata, on='propertycode', how='left')
    #
    # plist_rearrange_data1 =  plist_rearrange_data1[['propertykey', 'Zone_Type',
    #                       'gat_name', 'propertycode','receiptdate','paidamount','receiptdate_typaid','paidamount_TY', 'Arrears', 'Arrearsfind1', 'Current_Bill',
    #                     'CurrentBillfind1','illegaltotalbal', 'Actual_illegalBalance', 'illegal_flag',
    #                         'Arrears_from2019', 'Arrears_from2017','paid_in_March_LY','paid_in_LY',
    #                       'paid_in_TY',  'not_paid_LY', 'paid_by_dec_LY','Use_Type',
    #                       'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'propertyname','own_mobile','propertyaddress']]
    # #====================================================================================================================================================================
    # plist_rearrange_data1.to_excel(outpth + "/" + f"TotalArrears_Plist_Data14032023.xlsx",index=False,sheet_name="TotalArrearsPlist")

    #====================================================================================================================================================================
    # lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
    #        'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi',
    #        'Moshi']
    #------------------------------------------------------------------------------------
    ## One lac & above
    # onwlkk = plist_rearrange_data[plist_rearrange_data['Arrears'] > 100000]
    #
    # writer = pd.ExcelWriter(outpth + "/" + "OneLac&AboveArrears_PropertyList_ZoneWise.xlsx", engine="xlsxwriter")
    # onwlkk.to_excel(writer, index=False, sheet_name="TotalPlistOneLacList")
    # for i in lst:
    #     zzz = onwlkk[onwlkk['Zone_Type'] == i]
    #     zzz.to_excel(writer, index=False, sheet_name=i)
    # writer.save()
    # writer.close()
    #====================================================================================================================================================================
