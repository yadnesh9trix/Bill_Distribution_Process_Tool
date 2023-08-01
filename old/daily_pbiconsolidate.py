import pandas as pd
import datetime
import warnings
warnings.filterwarnings('ignore')



def daily_consolidate(inppath,outpth,property_list_df,today,zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap ):
    ytddata = pd.read_excel(inppath + "Paidamount_list.xlsx", sheet_name="Total")
    # Property_List = pd.read_csv(inppath + "Property_List.csv",low_memory=False)

    plist = property_list_df[['propertykey', 'propertycode','zone','gat','usetypekey',
                              'finalusetype', 'subusetypekey','constructiontypekey', 'occupancykey', 'specialoccupantkey','specialownership']]

    plistdropdupli = plist.drop_duplicates()

    ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10","1100900002.20")

    ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)

    ### drop duplicates on property code
    megre_p = ytddata.merge(plistdropdupli, on='propertycode', how='left')

    property_fin_yrmth_df = megre_p.copy()

    property_fin_yrmth_df['receiptdate'] = pd.to_datetime(property_fin_yrmth_df['receiptdate'])
    property_fin_yrmth_df['fin_year'] = property_fin_yrmth_df['receiptdate'].dt.year
    property_fin_yrmth_df['fin_month'] = property_fin_yrmth_df['receiptdate'].dt.month


    # final_pbiconsolidated = property_fin_yrmth_df[['propertykey', 'modeofpayment',
    #                             'billdate', 'receiptdate', 'billamount', 'paidamount',
    #                              'fin_year', 'fin_month']]
    # daily_pbiconsolidated = property_fin_yrmth_df[['propertykey','modeofpayment','receiptdate', 'billamount', 'paidamount',
    #                              'fin_year', 'fin_month']]

    daily_pbiconsolidated = pd.DataFrame(property_fin_yrmth_df,
                                          columns=['propertykey', 'modeofpayment', 'billdate', 'receiptdate',
                                                   'billamount', 'paidamount',
                                                   'fin_year', 'fin_month', 'paid_last_qtr_LY', 'paid_by_dec_LY',
                                                   'Not_paid_LY',
                                                   'Not_paid_in_4Yrs'])

    # final df dump in to csv
    daily_pbiconsolidated.to_csv(outpth + "/" +f"YTD_pbiconsolidate_{today}.csv", sep="|", index=False)

    # ddd = ytddata[ytddata["propertycode"].str.contains(fr'\b.\b..\b', regex=True, case=False)]
    # df=ytddata[ytddata['propertycode'].isna().values]
    # ytddata["propertycode"] = ytddata["propertycode"].astype(str)
    # aaa = ytddata[ytddata["propertycode"].str.contains(fr'\b.\b...\b', regex=True, case=False)]
    # zz  =megre_p[megre_p['paidamount'] != 0]
    # aa = megre_p[['propertykey', 'propertycode','zone', 'gat','usetypekey', 'finalusetype', 'subusetypekey','constructiontypekey', 'occupancykey', 'specialoccupantkey','specialownership', 'receiptdate','magil', 'chalu', 'paidamount']]
    # ['ezname', 'gatname', 'propertycode', 'propertyname', 'receiptdate',
    #        'magil', 'chalu', 'paidamount']