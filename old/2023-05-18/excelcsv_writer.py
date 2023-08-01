import datetime
import pandas as pd
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import xlsxwriter

def plistdetails(outpth,merge_pdata_plist,property_list_df,
                    zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap):
    final_output =merge_pdata_plist.copy()

    final_output['Use_Type'] = final_output['usetypekey'].map(usemap)
    final_output['Construction_Type'] = final_output['constructiontypekey'].map(construcmap)
    final_output['Occupancy_Type'] = final_output['occupancykey'].map(occpmap)
    final_output['Subuse_Type'] = final_output['subusetypekey'].map(subusemap)
    final_output['Zone_Type'] = final_output['zone'].map(zonemap)
    final_output['gat_name'] = final_output['gat'].map(gatnamemap)
    final_output['specialowner_Type'] = final_output['specialownership'].map(splownmap)
    final_output['specialoccupant_Type'] = final_output['specialoccupantkey'].map(splaccmap)

    fnal = final_output[['propertykey', 'zone', 'gat',
                         'specialownership', 'Use_Type', 'Construction_Type', 'Occupancy_Type',
                         'Subuse_Type', 'Zone_Type', 'gat_name', 'specialowner_Type',
                         'specialoccupant_Type']]

    # fnal = final_output[['propertykey', 'zone', 'gat',
    #                      'permission', 'usetypekey', 'finalusetype', 'subusetypekey',
    #                      'constructiontypekey', 'occupancykey', 'specialoccupantkey',
    #                      'specialownership', 'occupantname', 'Use_Type', 'Construction_Type', 'Occupancy_Type',
    #                      'Subuse_Type', 'Zone_Type', 'gat_name', 'specialowner_Type',
    #                      'specialoccupant_Type']]

    # fnal.to_csv(outpth +"/"+"PropertyListDetails.csv", index=False)

    # final_output_ = final_output[['propertykey', 'modeofpayment',
    #                              'billdate', 'receiptdate', 'billamount', 'paidamount',
    #                              'fin_year', 'fin_month', 'paid_last_qtr_LY', 'paid_by_dec_LY', 'Not_paid_LY',
    #                              'Not_paid_in_4Yrs']]

    ## final df dump in to csv
    # final_output_.to_csv(outpath + "pbiconsolidate_pbill.csv", sep="|", index=False)

    pass


def Final_not_paid_in4LY(not_paid_yet_in4ly_df,property_list_df,inppath,outpth,
            zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap,merge_propertydata_npaidin4ly):
    # merge_npaid_in4ly_plist = not_paid_yet_in4ly_df.merge(property_list_df, on='propertykey', how='inner')
    # zonewise = merge_npaid_in4ly_plist.groupby(['zone', 'Zone_Type', 'gat_name', 'Use_Type', 'Subuse_Type']).agg(
    #     {'billamount': 'sum', 'propertykey': 'count'}).reset_index()
    # zonewise.to_csv(outpth + "PCMC_PTAX_Summary.csv", index=False)

    property_assessment_date =pd.read_csv(inppath + "Property_Assessment_Date.csv",low_memory=False)

    merge2_propertydata_npaidin4ly = merge_propertydata_npaidin4ly.merge(property_assessment_date,on='propertykey',how='left')

    plist_rearrange_data = merge2_propertydata_npaidin4ly[merge2_propertydata_npaidin4ly['Not_paid_in_4Yrs'] == 1].reset_index(drop=True)
    plist_rearrange_data = plist_rearrange_data.fillna(0)

    plist_rearrange_datanew =plist_rearrange_data[['propertykey', 'totalbillamount','zone','gat',
           'paidamount','fin_year_r', 'fin_month_r', 'propertycode', 'propertyname','propertyaddress','usetypekey', 'finalusetype',
           'subusetypekey', 'constructiontypekey', 'occupancykey','assessmentdate','billdate',
           'specialoccupantkey', 'specialownership' ,'own_mobile', 'Not_paid_in_4Yrs']]

    grpvysummto = plist_rearrange_datanew.groupby(['propertykey', 'zone', 'gat',
                                                   'paidamount', 'fin_year_r', 'fin_month_r', 'propertycode',
                                                   'propertyname', 'propertyaddress', 'usetypekey', 'finalusetype',
                                                   'subusetypekey', 'constructiontypekey', 'occupancykey',
                                                   'specialoccupantkey', 'specialownership', 'own_mobile',
                                                   'Not_paid_in_4Yrs']).agg(
        {'totalbillamount': 'sum', 'billdate': 'max', 'assessmentdate': 'max'}).reset_index()

    # grpvysummto = plist_rearrange_datanew.groupby(['propertykey', 'zone', 'gat',
    #                                                'paidamount', 'fin_year_r', 'fin_month_r', 'assessmentdate',
    #                                                'billdate', 'propertycode', 'propertyname', 'propertyaddress',
    #                                                'usetypekey', 'finalusetype',
    #                                                'subusetypekey', 'constructiontypekey', 'occupancykey',
    #                                                'specialoccupantkey', 'specialownership', 'own_mobile',
    #                                                'Not_paid_in_4Yrs']).agg({'totalbillamount': 'sum'}).reset_index()

    grpvysummto['Use_Type'] = grpvysummto['usetypekey'].map(usemap)
    grpvysummto['Construction_Type'] = grpvysummto['constructiontypekey'].map(construcmap)
    grpvysummto['Occupancy_Type'] = grpvysummto['occupancykey'].map(occpmap)
    grpvysummto['Subuse_Type'] = grpvysummto['subusetypekey'].map(subusemap)
    grpvysummto['Zone_Type'] = grpvysummto['zone'].map(zonemap)
    grpvysummto['gat_name'] = grpvysummto['gat'].map(gatnamemap)
    grpvysummto['specialowner_Type'] = grpvysummto['specialownership'].map(splownmap)
    grpvysummto['specialoccupant_Type'] = grpvysummto['specialoccupantkey'].map(splaccmap)

    ### property code without zero
    grpvysummto_woutzero = grpvysummto[grpvysummto['propertycode'] != 0]
    grpvysummto_woutzero = grpvysummto[grpvysummto['totalbillamount'] != 0]


    fnalgrp = grpvysummto_woutzero[['propertykey','assessmentdate','billdate',
                           'propertycode', 'propertyname','propertyaddress','own_mobile',
                           'Not_paid_in_4Yrs', 'totalbillamount', 'Use_Type', 'Construction_Type',
                           'Occupancy_Type', 'Subuse_Type', 'Zone_Type', 'gat_name',
                           'specialowner_Type', 'specialoccupant_Type']]
    fnalgrp['specialowner_Type'] = fnalgrp['specialowner_Type'].fillna('Unknown')
    # -------------------------------------------------------------------------------------------------------------------------------------
    fnalgrp.to_excel(outpth + "/" + "proeprtydefaulterlistIn4Ly.xlsx", index=False)

    # lkh1_above = fnalgrp[fnalgrp['totalbillamount'] >= 100000]
    # # lkh1_above['Zone_Type'] = lkh1_above['Zone_Type'].dropna()
    # lkh1_above = lkh1_above.drop(columns='Not_paid_in_4Yrs')

    # lst = lkh1_above['Zone_Type'].drop_duplicates().to_list()
    # lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
    #  'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi', 'Moshi']

    # writer = pd.ExcelWriter(outpth + "/" + "TotalPropertyDefaulterList_ZoneWise.xlsx", engine="xlsxwriter")
    # for i in lst:
    #     zzz = fnalgrp[fnalgrp['Zone_Type']==i]
    #     zzz.to_excel(writer, index=False, sheet_name =i)
    # writer.save()
    # writer.close()
    # print(True)
    # -------------------------------------------------------------------------------------------------------------------------------------
    # between_1k_10k = fnalgrp[(fnalgrp['totalbillamount'] >= 1000) & (fnalgrp['totalbillamount'] <= 10000)]
    # between_10k_50k = fnalgrp[(fnalgrp['totalbillamount'] >= 10000) & (fnalgrp['totalbillamount'] <= 50000)]
    # between_50k_1lakh = fnalgrp[(fnalgrp['totalbillamount'] >= 50000) & (fnalgrp['totalbillamount'] <= 100000)]
    ##-------------------------------------------------------------------------------------------------------------------------------------
    ## latest
    # lkh1_above = fnalgrp[fnalgrp['totalbillamount'] >= 100000]
    ##-------------------------------------------------------------------------------------------------------------------------------------
    # between_10kAbove = fnalgrp[(fnalgrp['totalbillamount'] >= 10000)]

    # between_1k_10k.to_excel(outpth + "/" + "Total_between1k&10.xlsx", index=False)
    # between_10k_50k.to_excel(outpth + "/" + "Total_between_10k&50k.xlsx", index=False)
    # between_50k_1lakh.to_excel(outpth + "/" + "Total_between_50k&1lakh.xlsx", index=False)
    # lkh1_above.to_excel(outpth + "/" + "Total_lakh1_above.xlsx", index=False)
    # between_10kAbove.to_excel(outpth + "/" + "Total_between_10kabove.xlsx", index=False)

    ###=====================================================================================================================================
    #### Start
    # fnalgrp = lkh1_above[['propertycode','assessmentdate','billdate','propertyname','propertyaddress', 'own_mobile',
    #                    'totalbillamount', 'Use_Type', 'Construction_Type',
    #                    'Subuse_Type', 'Zone_Type', 'gat_name']]
    # fnalgrp['Zone_Type_Eng'] = fnalgrp['Zone_Type']
    # fnalgrp1 = fnalgrp.rename(
    #     columns={'propertycode': 'मालमत्ताधारकांचे क्रमांक', 'propertyname': 'मालमत्ताधारकांचे नाव',
    #              'propertyaddress':'पत्ता','assessmentdate':'आकारणी दिनांक','billdate':'बिल केलेली दिनांक',
    #              'own_mobile': 'मोबाईल नंबर', 'totalbillamount': 'थकबाकी',
    #              'Use_Type': 'वापर', 'Construction_Type': 'बांधकाम प्रकार',
    #              'Subuse_Type': 'वापर प्रकार', 'Zone_Type': 'विभागीय कार्यालय', 'gat_name': 'गट नंबर'})
    #
    # fnalgrp1['शेरा'] = ''
    #
    # fnalgrp1.to_excel(outpth + "/" + "Total_lakh1_above.xlsx", index=False)
    #
    # lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
    #        'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi',
    #        'Moshi']
    #
    # writer = pd.ExcelWriter(outpth + "/" + "NEW_PropertyDefaulterList_ZoneWise.xlsx", engine="xlsxwriter")
    # for i in lst:
    #     zzz = fnalgrp1[fnalgrp1['Zone_Type_Eng'] == i]
    #     zzz1 = zzz.drop(columns='Zone_Type_Eng')
    #     zzz1.to_excel(writer, index=False, sheet_name=i)
    # writer.save()
    # writer.close()
    ###=====================================================================================================================================
    return fnalgrp

def Final_not_paid_in8LY_since2015(not_paid_yet_in8ly_since2015,property_list_df,inppath,outpth,
            zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap,merge_propertydata_npaidin8ly_2015):
    # merge_npaid_in4ly_plist = not_paid_yet_in4ly_df.merge(property_list_df, on='propertykey', how='inner')
    # zonewise = merge_npaid_in4ly_plist.groupby(['zone', 'Zone_Type', 'gat_name', 'Use_Type', 'Subuse_Type']).agg(
    #     {'billamount': 'sum', 'propertykey': 'count'}).reset_index()
    # zonewise.to_csv(outpth + "PCMC_PTAX_Summary.csv", index=False)

    property_assessment_date =pd.read_csv(inppath + "Property_Assessment_Date.csv",low_memory=False)

    merge2_propertydata_npaidin8ly = merge_propertydata_npaidin8ly_2015.merge(property_assessment_date,on='propertykey',how='left')

    plist_rearrange_data = merge2_propertydata_npaidin8ly[merge2_propertydata_npaidin8ly['Not_paid_in_8Yrs'] == 1].reset_index(drop=True)
    plist_rearrange_data = plist_rearrange_data.fillna(0)

    # plist_rearrange_datanew =plist_rearrange_data[['propertykey', 'totalbillamount','zone','gat',
    #        'paidamount','fin_year_r', 'fin_month_r', 'propertycode', 'propertyname','propertyaddress','usetypekey', 'finalusetype',
    #        'subusetypekey', 'constructiontypekey', 'occupancykey','assessmentdate','billdate',
    #        'specialoccupantkey', 'specialownership' ,'own_mobile', 'Not_paid_in_8Yrs']]

    plist_rearrange_datanew = plist_rearrange_data[['propertykey', 'billdate', 'totalbillamount',
       'totalbalanceamount', 'fromdate', 'todate', 'modeofpayment','billamount', 'receiptdate',
       'fin_year_r', 'fin_month_r', 'propertycode', 'propertyname',
       'propertyaddress', 'ratablevalue', 'area', 'zone', 'gat', 'totalarea',
       'annualrentsum', 'taxable', 'permission', 'usetypekey', 'finalusetype',
       'subusetypekey', 'constructiontypekey', 'occupancykey',
       'specialoccupantkey', 'specialownership', 'occupantname', 'occ_mobile',
       'ownername', 'own_mobile']]

    # grpvysummto = plist_rearrange_datanew.groupby(['propertykey', 'zone', 'gat',
    #                                                'paidamount', 'fin_year_r', 'fin_month_r', 'propertycode',
    #                                                'propertyname', 'propertyaddress', 'usetypekey', 'finalusetype',
    #                                                'subusetypekey', 'constructiontypekey', 'occupancykey',
    #                                                'specialoccupantkey', 'specialownership', 'own_mobile',
    #                                                'Not_paid_in_8Yrs']).agg(
    #     {'totalbillamount': 'sum', 'billdate': 'max', 'assessmentdate': 'max'}).reset_index()

    grpvysummto = plist_rearrange_datanew.groupby(['propertykey', 'propertycode', 'propertyname',
       'propertyaddress', 'ratablevalue', 'area', 'zone', 'gat', 'totalarea',
       'annualrentsum', 'taxable', 'permission', 'usetypekey', 'finalusetype',
       'subusetypekey', 'constructiontypekey', 'occupancykey',
       'specialoccupantkey', 'specialownership', 'occupantname', 'occ_mobile',
       'ownername', 'own_mobile']).agg(
        {'totalbillamount': 'sum'}).reset_index()


    # grpvysummto = plist_rearrange_datanew.groupby(['propertykey', 'zone', 'gat',
    #                                                'paidamount', 'fin_year_r', 'fin_month_r', 'assessmentdate',
    #                                                'billdate', 'propertycode', 'propertyname', 'propertyaddress',
    #                                                'usetypekey', 'finalusetype',
    #                                                'subusetypekey', 'constructiontypekey', 'occupancykey',
    #                                                'specialoccupantkey', 'specialownership', 'own_mobile',
    #                                                'Not_paid_in_4Yrs']).agg({'totalbillamount': 'sum'}).reset_index()

    grpvysummto['Use_Type'] = grpvysummto['usetypekey'].map(usemap)
    grpvysummto['Construction_Type'] = grpvysummto['constructiontypekey'].map(construcmap)
    grpvysummto['Occupancy_Type'] = grpvysummto['occupancykey'].map(occpmap)
    grpvysummto['Subuse_Type'] = grpvysummto['subusetypekey'].map(subusemap)
    grpvysummto['Zone_Type'] = grpvysummto['zone'].map(zonemap)
    # grpvysummto['gat_name'] = grpvysummto['gat'].map(gatnamemap)
    grpvysummto['specialowner_Type'] = grpvysummto['specialownership'].map(splownmap)
    grpvysummto['specialoccupant_Type'] = grpvysummto['specialoccupantkey'].map(splaccmap)

    ### property code without zero
    grpvysummto_pidwoutzero = grpvysummto[grpvysummto['propertycode'] != 0]
    # grpvysummto_tbillwoutzero = grpvysummto_pidwoutzero[grpvysummto_pidwoutzero['totalbillamount'] > 500]


    n_paidinL8Y_since2015 = grpvysummto_pidwoutzero[['propertykey','assessmentdate','billdate',
                           'propertycode', 'propertyname','propertyaddress','own_mobile',
                           'Not_paid_in_8Yrs', 'totalbillamount', 'Use_Type', 'Construction_Type',
                           'Occupancy_Type', 'Subuse_Type', 'Zone_Type', 'gat_name',
                           'specialowner_Type', 'specialoccupant_Type']]
    n_paidinL8Y_since2015['specialowner_Type'] = n_paidinL8Y_since2015['specialowner_Type'].fillna('Unknown')
    # -------------------------------------------------------------------------------------------------------------------------------------
    n_paidinL8Y_since2015.to_excel(outpth + "/" + "PID_DefaulterlistIn8LySince2015.xlsx", index=False)
    ### =================================================== ============================================================
    # For Pivot summary
    n_paidinL8Y_since2015['gat_name'] = n_paidinL8Y_since2015['gat_name'].replace('0', "unknown")
    n_paidinL8Y_since2015['gat_name'] = n_paidinL8Y_since2015['gat_name'].replace(np.nan, "unknown")
    ggg = n_paidinL8Y_since2015['gat_name'].unique().tolist()

    ggg1 = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18',
            'unknown']

    pvotable = n_paidinL8Y_since2015.pivot_table(index=['Zone_Type'], columns='gat_name', values='totalbillamount',
                                                 aggfunc='count')
    # aa_1111 = n_paidinL8Y_since2015.melt(id_vars=['Zone_Type', 'gat_name'], var_name='groubysum', value_name='totalbillamount')
    # bbb = list(range(1, 19))
    dfff = pd.DataFrame(pvotable, columns=ggg1)
    pp = dfff.reset_index()

    lissy = ['Nigdi Pradhikaran', 'Akurdi', 'Chinchwad', 'Thergaon', 'Sangvi', 'Pimpri Waghere',
             'Pimpri Nagar', 'MNP Bhavan', 'Fugewadi Dapodi', 'Bhosari', 'Charholi',
             'Moshi', 'Chikhali', 'Talvade', 'Kivle', 'Dighi Bopkhel', 'Wakad']

    d = {v: k for k, v in enumerate(lissy)}
    df111 = pp.sort_values('Zone_Type', key=lambda x: x.map(d), ignore_index=True)

    collen = df111.columns.to_list()[1:]
    df111['Grand Total'] = df111[collen].sum(axis=1)
    df111 = df111.sort_values("Grand Total", ascending=False)
    df111.loc["Grand Total"] = df111.sum(numeric_only=True)

    df111.to_excel(outpth + "/" + "listforhording.xlsx", index=False)


    ### ============================================================================================================
    # lkh1_above = fnalgrp[fnalgrp['totalbillamount'] >= 100000]
    # # lkh1_above['Zone_Type'] = lkh1_above['Zone_Type'].dropna()
    # lkh1_above = lkh1_above.drop(columns='Not_paid_in_4Yrs')

    # lst = lkh1_above['Zone_Type'].drop_duplicates().to_list()
    # lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
    #  'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi', 'Moshi']

    # writer = pd.ExcelWriter(outpth + "/" + "TotalPropertyDefaulterList_ZoneWise.xlsx", engine="xlsxwriter")
    # for i in lst:
    #     zzz = fnalgrp[fnalgrp['Zone_Type']==i]
    #     zzz.to_excel(writer, index=False, sheet_name =i)
    # writer.save()
    # writer.close()
    # print(True)
    # -------------------------------------------------------------------------------------------------------------------------------------
    # between_1k_10k = fnalgrp[(fnalgrp['totalbillamount'] >= 1000) & (fnalgrp['totalbillamount'] <= 10000)]
    # between_10k_50k = fnalgrp[(fnalgrp['totalbillamount'] >= 10000) & (fnalgrp['totalbillamount'] <= 50000)]
    # between_50k_1lakh = fnalgrp[(fnalgrp['totalbillamount'] >= 50000) & (fnalgrp['totalbillamount'] <= 100000)]
    ##-------------------------------------------------------------------------------------------------------------------------------------
    ## latest
    # lkh1_above = fnalgrp[fnalgrp['totalbillamount'] >= 100000]
    ##-------------------------------------------------------------------------------------------------------------------------------------
    # between_10kAbove = fnalgrp[(fnalgrp['totalbillamount'] >= 10000)]

    # between_1k_10k.to_excel(outpth + "/" + "Total_between1k&10.xlsx", index=False)
    # between_10k_50k.to_excel(outpth + "/" + "Total_between_10k&50k.xlsx", index=False)
    # between_50k_1lakh.to_excel(outpth + "/" + "Total_between_50k&1lakh.xlsx", index=False)
    # lkh1_above.to_excel(outpth + "/" + "Total_lakh1_above.xlsx", index=False)
    # between_10kAbove.to_excel(outpth + "/" + "Total_between_10kabove.xlsx", index=False)

    ###=====================================================================================================================================
    #### Start
    # fnalgrp = lkh1_above[['propertycode','assessmentdate','billdate','propertyname','propertyaddress', 'own_mobile',
    #                    'totalbillamount', 'Use_Type', 'Construction_Type',
    #                    'Subuse_Type', 'Zone_Type', 'gat_name']]
    # fnalgrp['Zone_Type_Eng'] = fnalgrp['Zone_Type']
    # fnalgrp1 = fnalgrp.rename(
    #     columns={'propertycode': 'मालमत्ताधारकांचे क्रमांक', 'propertyname': 'मालमत्ताधारकांचे नाव',
    #              'propertyaddress':'पत्ता','assessmentdate':'आकारणी दिनांक','billdate':'बिल केलेली दिनांक',
    #              'own_mobile': 'मोबाईल नंबर', 'totalbillamount': 'थकबाकी',
    #              'Use_Type': 'वापर', 'Construction_Type': 'बांधकाम प्रकार',
    #              'Subuse_Type': 'वापर प्रकार', 'Zone_Type': 'विभागीय कार्यालय', 'gat_name': 'गट नंबर'})
    #
    # fnalgrp1['शेरा'] = ''
    #
    # fnalgrp1.to_excel(outpth + "/" + "Total_lakh1_above.xlsx", index=False)
    #
    # lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
    #        'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi',
    #        'Moshi']
    #
    # writer = pd.ExcelWriter(outpth + "/" + "NEW_PropertyDefaulterList_ZoneWise.xlsx", engine="xlsxwriter")
    # for i in lst:
    #     zzz = fnalgrp1[fnalgrp1['Zone_Type_Eng'] == i]
    #     zzz1 = zzz.drop(columns='Zone_Type_Eng')
    #     zzz1.to_excel(writer, index=False, sheet_name=i)
    # writer.save()
    # writer.close()
    ###=====================================================================================================================================
    return n_paidinL8Y_since2015,plist_rearrange_datanew

def find_nlink_mobile(outpth,property_list_df,not_paid_yet_in4ly_df,notpaid_ly,
                      zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap,merge_propertydata_npaidin4ly):
    # Identify the defaulter list of properties which is linked mobile number or without mobile
    property_list_data = property_list_df.copy()

    plist_rearrange_data = property_list_data[['propertykey', 'propertycode', 'propertyname', 'zone', 'gat',
                                          'permission', 'usetypekey', 'finalusetype', 'subusetypekey',
                                          'constructiontypekey', 'occupancykey', 'specialoccupantkey',
                                          'specialownership', 'occupantname', 'own_mobile']]
    plist_rearrange_data['own_mobile'] = plist_rearrange_data['own_mobile'].fillna(0)

    plist_rearrange_data['Use_Type'] = plist_rearrange_data['usetypekey'].map(usemap)
    plist_rearrange_data['Construction_Type'] = plist_rearrange_data['constructiontypekey'].map(construcmap)
    plist_rearrange_data['Occupancy_Type'] = plist_rearrange_data['occupancykey'].map(occpmap)
    plist_rearrange_data['Subuse_Type'] = plist_rearrange_data['subusetypekey'].map(subusemap)
    plist_rearrange_data['Zone_Type'] = plist_rearrange_data['zone'].map(zonemap)
    plist_rearrange_data['gat_name'] = plist_rearrange_data['gat'].map(gatnamemap)
    plist_rearrange_data['specialowner_Type'] = plist_rearrange_data['specialownership'].map(splownmap)
    plist_rearrange_data['specialoccupant_Type'] = plist_rearrange_data['specialoccupantkey'].map(splaccmap)

    # fnal = plist_rearrange_data[['propertykey', 'zone',
    #                      'specialownership', 'Use_Type', 'Construction_Type', 'Occupancy_Type',
    #                      'Subuse_Type', 'Zone_Type', 'gat_name', 'specialowner_Type',
    #                      'specialoccupant_Type']]
    plist_ownmobile = plist_rearrange_data[['propertykey', 'propertycode', 'own_mobile',
                                 'Use_Type', 'Construction_Type', 'Occupancy_Type',
                                 'Subuse_Type', 'Zone_Type', 'gat_name', 'specialowner_Type',
                                 'specialoccupant_Type']]


    plist_linkmbile = plist_ownmobile[plist_ownmobile['own_mobile'] != 0]
    plist_notlinkmbile = plist_ownmobile[plist_ownmobile['own_mobile'] == 0]

    plist_linkmbile.to_excel(outpth +"/"+"plist_linkmbile.xlsx",index=False)
    plist_notlinkmbile.to_excel(outpth + "/" + "plist_notlinkmobile.xlsx", index=False)

    ####---------------------------------------------------------------------------------------------------------------------------------
    ## Merge property list with not paid in last 4 year
    # Identify the defaulter list of properties which is linked mobile number or without mobile
    defaulterlist_linkwmobile = plist_linkmbile.merge(not_paid_yet_in4ly_df, on='propertykey', how='left')
    dfff_111 = defaulterlist_linkwmobile[defaulterlist_linkwmobile['Not_paid_in_4Yrs'] == 1]
    dfff_111['specialowner_Type'] = dfff_111['specialowner_Type'].fillna('Unknown')

    ## Final Dump
    # dfff_111.to_excel(outpth + "/" + "pdefaulterlist_linkwmobile.xlsx", index=True)

####============================================================================================================================================================
def bill_receipt(inppath,outpth,property_list_df):
    ## The line of code below indicates how often the taxpayer has paid the tax last years.
    df_receipt_bill = pd.read_csv(inppath + "Bill_Receipt_Data.csv",low_memory=False)
    # sort_value_df = df_receipt_bill.sort_values("propertykey")
    property_list_df1 = property_list_df.copy()

    property_list_df1 = property_list_df1.drop_duplicates('propertykey')
    property_list_df1 = property_list_df1.drop_duplicates('propertycode')

    # dropna using propertykey
    property_list_df1['propertykey'] = property_list_df1['propertykey'].dropna()
    df_receipt_bill['propertykey'] = df_receipt_bill['propertykey'].dropna()

    merge_receiptbill_plist = df_receipt_bill.merge(property_list_df1, on='propertykey', how='left')
    merge_receiptbill_plist['receiptdate'] = pd.to_datetime(merge_receiptbill_plist['receiptdate'])
    merge_receiptbill_plist['fin_year_r'] = merge_receiptbill_plist['receiptdate'].dt.year
    merge_receiptbill_plist['fin_month_r'] = merge_receiptbill_plist['receiptdate'].dt.month

    ### Rearrange the columns
    merge_receiptbill_plist_arrange = merge_receiptbill_plist[
        ['propertykey', 'pbdr_billamount', 'pbdr_paidamount', 'propertycode', 'zone', 'gat', 'usetypekey',
         'finalusetype', 'subusetypekey',
         'constructiontypekey', 'occupancykey', 'specialoccupantkey',
         'specialownership', 'fin_year_r', 'fin_month_r']]

    # Filtering
    filter_receiptbill_plist_arrange = merge_receiptbill_plist_arrange[
        (merge_receiptbill_plist_arrange['fin_month_r'] > 9) & (merge_receiptbill_plist_arrange['fin_year_r'] < 2022)]

    ### Filter the data only for 2021 payee
    # filter_receiptbill_plist_arrange_2021 = filter_receiptbill_plist_arrange[
    #     filter_receiptbill_plist_arrange['fin_year_r'] == 2021]

    ## Find the count of propertykey
    pkry_pcode_count = filter_receiptbill_plist_arrange.groupby(['propertycode']).agg(
        {'propertykey': 'count'}).reset_index()

    ## Read YTD data PAID Amount DATA
    ytddata = pd.read_excel(inppath + "Paidamount_list.xlsx", sheet_name="Total")

    ## Replace the property code values
    ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
    ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)

    ## Merge YTD data with pkey pcount count
    merge_ytddata = ytddata.merge(pkry_pcode_count, on='propertycode', how='left')

    merge_ytddata = merge_ytddata.fillna(0)

    ###Last Quarter Payee
    # merge_ytddata.to_csv(outpth + '/LastQuarter_Payee.csv',index=False)

    # grpbysumpaidamt = receiptbill_df.groupby('propertykey', 'propertycode',
    # 'modeofpayment','receiptdate','pbdr_billamount', 'pbdr_paidamount', 'fin_year_r', 'fin_month_r').agg({'pbdr_paidamount':'sum'}).reset_index()

    # pvottble = grpbysumpaidamt.pivot_table(index=['propertycode'], columns='fin_year_r', values='pbdr_paidamount')

    # ['propertykey', 'propertycode', 'propertyname', 'propertyaddress',
    #        'ratablevalue', 'area', 'zone', 'gat', 'totalarea', 'annualrentsum',
    #        'taxable', 'permission', 'usetypekey', 'finalusetype', 'subusetypekey',
    #        'constructiontypekey', 'occupancykey', 'specialoccupantkey',
    #        'specialownership', 'occupantname', 'occ_mobile', 'ownername',
    #        'own_mobile', 'propertybillreceiptkey', 'propertybillkey', 'receiptno',
    #        'modeofpayment', 'chequestatus', 'honoureddate', 'receiptdate',
    #        'financialyearkey', 'paidamount', 'billamount', 'balanceamount',
    #        'propertybillreceiptkey-2', 'pbdr_billamount', 'pbdr_paidamount',
    #        'pbdr_balanceamount', 'fin_year_r', 'fin_month_r']

    # ['propertykey', 'propertybillreceiptkey', 'propertybillkey','receiptno',
    #        'modeofpayment', 'chequestatus', 'honoureddate', 'receiptdate',
    #        'financialyearkey', 'paidamount', 'billamount', 'balanceamount',
    #        'propertybillreceiptkey-2', 'pbdr_billamount', 'pbdr_paidamount',
    #        'pbdr_balanceamount', 'fin_year_r', 'fin_month_r']
    print(True)
####============================================================================================================================================================

def excel_writer_dump(outpth,notpaid_ly,not_paid_yet_in4ly_df,paid_bydec_ly,paid_lastquarter_ly,partially_paid_ty):
    writer = pd.ExcelWriter(outpth + "/" + "FinalReport.xlsx", engine="xlsxwriter")
    notpaid_ly.to_excel(writer,sheet_name='notpaid_ly',index=False)
    not_paid_yet_in4ly_df.to_excel(writer,sheet_name='not_paid_yet_in4ly_df',index=False)
    paid_bydec_ly.to_excel(writer,sheet_name='paid_bydec_ly',index=False)
    paid_lastquarter_ly.to_excel(writer,sheet_name='paid_lastquarter_ly',index=False)
    partially_paid_ty.to_excel(writer,sheet_name='partially_paid_ty',index=False)

    writer.save()
    writer.close()

####============================================================================================================================================================

def defaulter_analysis(inppath,outpth,property_list_df, merge_pdata_plist,property_financial_yrmth_df,
                       unique_pkey_df,zonemap, usemap, construcmap, occpmap, subusemap, gatnamemap, splownmap, splaccmap):
####============================================================================================================================================================
    property_list_df.dropna(subset=['propertycode'], how='all', inplace=True)
    property_list_df.dropna(subset=['propertykey'], how='all', inplace=True)

    ## find the GroupBy MAX PID, financial year or month using receipt Date
    property_fyrmth_r_max = property_financial_yrmth_df.groupby(['propertykey'])[
        'fin_year_r', 'fin_month_r'].max().reset_index()

    # Apply merge using unique property id
    merge_maxfyrmth_uniqpid_df_Year_r = property_fyrmth_r_max.merge(unique_pkey_df, on='propertykey', how='inner')

    ## 02/03/2023 We added the fin year & month replaced by 1999 & 0    as suggested by SK sir
    merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] =  merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'].fillna(1999)
    merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'] = merge_maxfyrmth_uniqpid_df_Year_r['fin_month_r'].fillna(0)

## drop data if financial year is less than 2022
    filterdata_lessthan_2022 = merge_maxfyrmth_uniqpid_df_Year_r[merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] != 2022]

#==========================================================================================================================================================================
    plist_arrange =  property_list_df[['propertykey', 'propertycode']]
    plist_arrange['propertykey'] = plist_arrange['propertykey'].drop_duplicates()
    plist_arrange['propertycode'] = plist_arrange['propertycode'].drop_duplicates()

    ## merge property list with max finacial yr & month
    merge_plist_merge_maxfyrmth_uniqpid_df_Year_r = plist_arrange.merge(merge_maxfyrmth_uniqpid_df_Year_r,
                                                                           on='propertykey', how='left')
    ## Read YTD data
    ytddata = pd.read_excel(inppath + "Paidamount_list_10032023.xlsx", sheet_name="Total")
    ## select only pid column from ytd data
    ytddata = ytddata[['propertycode']]

    ## Replace the property code values in ytd data
    ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
    ytddata['propertycode'] = ytddata['propertycode'].astype(float)
    ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)
    ytddata = ytddata.drop_duplicates('propertycode')
    ytddata.dropna(subset=['propertycode'], how='all', inplace=True)
    ytddata['paid_in_TY'] = 1
    # merge_maxfyrmth_uniqpid_df_Year_r.dropna(subset=['propertycode'], how='all', inplace=True)
    merge_plist_merge_maxfyrmth_uniqpid_df_Year_r.dropna(subset=['propertykey'], how='all', inplace=True)
    ## Merge ytd data
    merge_ytddata_mergepdaatplist = merge_plist_merge_maxfyrmth_uniqpid_df_Year_r.merge(ytddata, on='propertycode',
                                                                                        how='left')
    ## find PID without paid in TY
    wioutpaidinTY_data1 = merge_ytddata_mergepdaatplist[merge_ytddata_mergepdaatplist['paid_in_TY'] != 1]
    wioutpaidinTY_data1['Not_paid_in_TY'] = 1

    ### Add
    new_df = pd.DataFrame(wioutpaidinTY_data1, columns=['propertykey','fin_year_r', 'Not_paid_in_TY'])
    ## merege propertylist & property bill combination
    merge_pdata_plist1 = merge_pdata_plist[['propertykey', 'billdate', 'totalbillamount']]

    merge_new_df = merge_pdata_plist1.merge(new_df, on='propertykey', how='left')

    ## identify not paid in TY
    not_paid_InTY = merge_new_df[merge_new_df['Not_paid_in_TY'] == 1]

    ### find bill year
    not_paid_InTY['billdate'] = pd.to_datetime(not_paid_InTY['billdate'],errors='coerce',format='%Y-%m-%d')
    # not_paid_InTY['billyear'] = not_paid_InTY['billdate'].dt.year
    not_paid_InTY['fin_year_b'] = not_paid_InTY['billdate'].dt.year
    not_paid_InTY['fin_month_b'] = not_paid_InTY['billdate'].dt.month
    ## Find the fin year & fin bill year using billdate
    not_paid_InTY['billyear'] = np.where(not_paid_InTY['fin_month_b'] < 4, not_paid_InTY['fin_year_b'] - 1,
                                           not_paid_InTY['fin_year_b'])
    not_paid_InTY['fin_month_b'] = np.where(not_paid_InTY['fin_month_b'] < 4, not_paid_InTY['fin_month_b'] + 9,
                                            not_paid_InTY['fin_month_b'] - 3)
    ## identify if bill yr > fin yr recipt
    not_paid_InTY = not_paid_InTY[not_paid_InTY['billyear'] > not_paid_InTY['fin_year_r']]

    ### 11-03-2023

    ### find current year bill amount
    current_yr_billamt = not_paid_InTY[not_paid_InTY['billyear'] == 2022]
    groupby_current = current_yr_billamt.groupby(['propertykey'])['totalbillamount'].sum().reset_index()
    current_yr_billamt = groupby_current.rename(columns={'totalbillamount': 'Current_Billamount'})
    current_yr_billamt = current_yr_billamt[current_yr_billamt['Current_Billamount'] != 0]

## find arrears
    current_arrears_billamt = not_paid_InTY[not_paid_InTY['billyear'] != 2022]
    groupby_arrears = current_arrears_billamt.groupby(['propertykey'])['totalbillamount'].sum().reset_index()
    groupby_arrears = groupby_arrears.rename(columns={'totalbillamount': 'Current_Arrears'})
    groupby_arrears = groupby_arrears[groupby_arrears['Current_Arrears'] != 0]

# find since 2019 sum of totalbillamount using billyear
    yr2019_2021_billamt = not_paid_InTY[(not_paid_InTY['billyear'] >= 2019) & (not_paid_InTY['billyear'] <= 2021)]
    yr2019_2021_billamtsum =  yr2019_2021_billamt.groupby('propertykey')['totalbillamount'].sum().reset_index()
    yr2019_2021_billamtsum = yr2019_2021_billamtsum.rename(columns= {"totalbillamount":'Arrears_from2019'})
    yr2019_2021_billamtsum = yr2019_2021_billamtsum[yr2019_2021_billamtsum['Arrears_from2019'] != 0]

    # find since 2017 sum of totalbillamount using billyear
    yr2017_2021_billamt = not_paid_InTY[(not_paid_InTY['billyear'] >= 2017) & (not_paid_InTY['billyear'] <= 2021)]
    yr2017_2021_billamtsum =  yr2017_2021_billamt.groupby('propertykey')['totalbillamount'].sum().reset_index()
    yr2017_2021_billamtsum =yr2017_2021_billamtsum.rename(columns= {"totalbillamount":'Arrears_from2017'})
    yr2017_2021_billamtsum = yr2017_2021_billamtsum[yr2017_2021_billamtsum['Arrears_from2017'] != 0]

    merege_defaulters_df1 = wioutpaidinTY_data1.merge(current_yr_billamt, on='propertykey', how='left')
    merege_defaulters_df2 = merege_defaulters_df1.merge(groupby_arrears, on='propertykey', how='left')
    merege_defaulters_df3 = merege_defaulters_df2[['propertykey', 'propertycode', 'Current_Billamount',
                                                   'Current_Arrears']]

    merege_defaulters_df4 = merege_defaulters_df3.merge(yr2017_2021_billamtsum, on='propertykey', how='left')
    merege_defaulters_df5 = merege_defaulters_df4.merge(yr2019_2021_billamtsum, on='propertykey', how='left')

 ###------------------------------------------------------------------------------------------------------------------------------------------------------------
    ## Filter less than 2022
    ## find paid by march LY
    paid_bymarch_LY = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_month_r'] == 12) &
         (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    paid_bymarch_LY['paid_bymarch_LY'] = 1
    paid_bymarch_LY = paid_bymarch_LY.drop(columns=['fin_year_r', 'fin_month_r'])
    paid_bymarch_LY['propertykey'] = paid_bymarch_LY['propertykey'].drop_duplicates()

## find not paid in LY
    Not_paid_LY_df = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_year_r'] < 2021)].reset_index(drop=True)
    Not_paid_LY_df['Not_paid_LY'] = 1
    Not_paid_LY_df = Not_paid_LY_df.drop(columns=['fin_year_r', 'fin_month_r'])
    Not_paid_LY_df['propertykey'] = Not_paid_LY_df['propertykey'].drop_duplicates()

    ### find paid by dec LY
    paid_by_dec_LY = filterdata_lessthan_2022 \
        [(filterdata_lessthan_2022['fin_month_r'] < 10) &
         (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
    paid_by_dec_LY['propertykey'] = paid_by_dec_LY['propertykey'].drop_duplicates()
    paid_by_dec_LY['paid_by_dec_LY'] = 1
    paid_by_dec_LY = paid_by_dec_LY.drop(columns=['fin_year_r', 'fin_month_r'])

    ### Merege paidbymarch ymarch,notpaidLY & paid by decLY
    finL_output_df1 = merege_defaulters_df5.merge(paid_bymarch_LY,on='propertykey',how='left')
    finL_output_df2 = finL_output_df1.merge(Not_paid_LY_df, on='propertykey', how='left')
    finL_output_df3 = finL_output_df2.merge(paid_by_dec_LY, on='propertykey', how='left')

    ## Drop na on current billamount & current arrears
    finL_output_df3.dropna(subset=['Current_Billamount','Current_Arrears'], how='all', inplace=True)
    finL_output_df3 = finL_output_df3.fillna(0)
    finL_output_df3['Total_Bill'] = finL_output_df3['Current_Billamount'] + finL_output_df3['Current_Arrears']

    # pbill_data = merge_pdata_plist[['propertykey', 'totalbillamount']]
    # not_paid_yet_in4ly_since2019 = not_paid_yet_in4ly_since2019.drop(columns=['fin_month_r','fin_year_r'])
    # not_paid_yet_in6ly_since2017 = not_paid_yet_in6ly_since2017.drop(columns=['fin_month_r', 'fin_year_r'])
    # not_paid_yet_in4ly_df1 = pbill_data.merge(not_paid_yet_in4ly_since2019,on='propertykey', how='left')

    actualtotalarrears =  not_paid_InTY.groupby(['propertykey'])['totalbillamount'].sum().reset_index()
    actualtotalarrears = actualtotalarrears.rename(columns= {"totalbillamount":'ActualTotalArrears'})
    actualtotalarrears = actualtotalarrears[actualtotalarrears['ActualTotalArrears'] != 0]

    finL_output_df4 = finL_output_df3.merge(actualtotalarrears, on='propertykey', how='left')

    # finL_output_df4.to_csv(outpth + "/"+ "pbiConsolidated_CrntArrears_11032023.csv", sep="|",index=False)

    onelakh_above_arrears = finL_output_df4[finL_output_df4['ActualTotalArrears'] >= 100000]
    onelakh_above_arrears['1lakhlist'] = 1

    merge_plist_onlearrears = property_list_df.merge(onelakh_above_arrears,on='propertykey',how='left')

    plist_rearrange_data =  merge_plist_onlearrears.copy()
    plist_rearrange_data['Use_Type'] = plist_rearrange_data['usetypekey'].map(usemap)
    plist_rearrange_data['Construction_Type'] = plist_rearrange_data['constructiontypekey'].map(construcmap)
    plist_rearrange_data['Occupancy_Type'] = plist_rearrange_data['occupancykey'].map(occpmap)
    plist_rearrange_data['Subuse_Type'] = plist_rearrange_data['subusetypekey'].map(subusemap)
    plist_rearrange_data['Zone_Type'] = plist_rearrange_data['zone'].map(zonemap)
    plist_rearrange_data['gat_name'] = plist_rearrange_data['gat'].map(gatnamemap)
    plist_rearrange_data['specialowner_Type'] = plist_rearrange_data['specialownership'].map(splownmap)
    plist_rearrange_data['specialoccupant_Type'] = plist_rearrange_data['specialoccupantkey'].map(splaccmap)

    # plist_rearrange_data = plist_rearrange_data[['propertykey', 'propertycode_x', 'propertyname', 'propertyaddress','occ_mobile',
    #        'Current_Billamount', 'Current_Arrears', 'Arrears_from2017',
    #        'Arrears_from2019', 'paid_bymarch_LY', 'Not_paid_LY', 'paid_by_dec_LY',
    #        'Total_Bill', 'ActualTotalArrears', '1lakhlist', 'Use_Type',
    #        'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'Zone_Type',
    #        'gat_name', 'specialowner_Type', 'specialoccupant_Type']]

    plist_rearrange_data1111 = plist_rearrange_data[
        ['propertykey', 'propertycode_x', 'propertyname', 'propertyaddress', 'occ_mobile',
         'Current_Billamount', 'Current_Arrears',
         'Total_Bill','1lakhlist', 'Use_Type',
         'Construction_Type', 'Occupancy_Type', 'Subuse_Type', 'Zone_Type',
         'gat_name', 'specialowner_Type', 'specialoccupant_Type']]

    onelakh_plistdf = plist_rearrange_data1111[plist_rearrange_data1111['1lakhlist'] == 1]

    onelakh_plistdf.to_excel(outpth + "/" + "LatestDump1lakh&above_11032023_.xlsx", index=False)

###=======================================================================================================================================================================
