import datetime
import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')

today= datetime.datetime.now()

def read_data(inppath):
    # property_bill_df = pd.read_csv(inppath + "property_bill.csv",low_memory=False)
    # property_bill_df = pd.read_csv(inppath + "Property_Tax_Dump_2015to2022.csv",low_memory=False)

    ### 12-03-2023
    # property_bill_df = pd.read_csv(inppath + "Property_Bill_24042023.csv",low_memory=False,encoding='utf-8-sig')    ###11/03/23
    property_bill_df = pd.read_csv(inppath + "Property_Bill_Trial.csv",low_memory=False,encoding='utf-8-sig')    ###11/03/23

    property_receipt_df = pd.read_csv(inppath + "Property_Tax_Receipt_Amount_Dump.csv",low_memory=False,encoding='utf-8-sig')    ###11/03/23

    property_list_df = pd.read_csv(inppath + "Property_List_24042023.csv",low_memory=False)
    property_list_df = property_list_df[property_list_df['verified'] != 'N']

    property_list_df.dropna(subset=['propertycode'], how='all', inplace=True)
    property_list_df.dropna(subset=['propertykey'], how='all', inplace=True)

    return property_bill_df, property_list_df, property_receipt_df


def mapping_type(mappath):
    usetype = pd.read_csv(mappath + "usetype.csv")
    usemap = dict(zip(usetype['usetypekey'],usetype['eng_usename']))
    # final_output['User_Type'] = final_output['usetypekey'].map(uuu)

    consttype = pd.read_csv(mappath + "constructiontype.csv")
    construcmap = dict(zip(consttype['constructiontypekey'],consttype['eng_constructiontypename']))
    # final_output['Construction_Type'] = final_output['constructiontypekey'].map(ccc)

    occptype=  pd.read_csv(mappath + "occupancy.csv")
    occpmap = dict(zip(occptype['occupancykey'],occptype['occupancyname']))
    # final_output['Occupancy_Type'] = final_output['occupancykey'].map(ooo)

    subusetype= pd.read_csv(mappath + "subusetype.csv")
    subusemap = dict(zip(subusetype['subusetypekey'],subusetype['eng_subusename']))
    # final_output['Subset_Type'] = final_output['subusetypekey'].map(sss)

    zonetype =pd.read_csv(mappath + "zone.csv")
    zonemap = dict(zip(zonetype['zonekey'],zonetype['eng_zonename']))
    # final_output['Zone_Type'] = final_output['zonekey'].map(zzz)

    gattype = pd.read_csv(mappath + "gat.csv")
    # gattype['gatname_z'] = gattype['gatname'].astype(str) + "_" + gattype['zonetype'].astype(str)
    gattype['gatname_z'] = gattype['gatname'].astype(str)

    gatnamemap = dict(zip(gattype['gat'], gattype['gatname_z']))
    # final_output['gat_name'] = final_output['gat'].map(ggg)

    specialowner = pd.read_csv(mappath + "specialownership.csv")
    splownmap = dict(zip(specialowner['specialownership'], specialowner['eng_specialownershipname']))
    # final_output['specialowner_Type'] = final_output['specialownership'].map(splown)

    splacctype = pd.read_csv(mappath + "specialoccupant.csv")
    splaccmap = dict(zip(splacctype['specialoccupantkey'], splacctype['eng_specialoccupantname']))
    # final_output['specialoccupant_Type'] = final_output['specialoccupantkey'].map(splacc)

    return zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap


def filter_data(property_bill_df):
    ## Rearrange & filterinf the columns of property bill data
    # property_df = property_bill_df[['propertykey', 'zonekey', 'billdate',
    #                             'totalbillamount', 'totalbalanceamount',
    #                             'fromdate', 'todate', 'modeofpayment',
    #                             'paidamount', 'balanceamount', 'billamount', 'receiptdate']]
    # property_df = property_bill_df[
    #     ['propertybillkey', 'propertykey', 'financialyearkey', 'zonekey', 'billdate', 'totalbillamount',
    #      'modeofpayment','paidamount', 'balanceamount', 'billamount',
    #      'receiptdate']]
    ### 11-03-2023
    # property_df = property_bill_df[
    #     ['propertybillkey', 'propertykey', 'financialyearkey', 'fromdate','modeofpayment','paidamount', 'balanceamount', 'billamount',
    #      'receiptdate']]
    property_df = property_bill_df[
        ['propertybillkey', 'propertykey', 'financialyearkey', 'fromdate', 'balanceamount', 'billamount']]

    property_df = property_df.rename(columns={'fromdate':'billdate','balanceamount':'totalbillamount'})

    # property_df = property_bill_df[['propertykey','propertycode','zonekey','finalusetype',
    #                             'usetypekey','constructiontypekey','subusetypekey','occupancykey',
    #                             'billdate', 'totalbillamount', 'totalbalanceamount',
    #                             'fromdate', 'todate','modeofpayment','paidamount', 'balanceamount',
    #                             'billamount','receiptdate', 'permission',
    #                             'zone','gat']]

    ## find the fromdate (Billed Date) column year or month
    # property_data['fromdate'] = pd.to_datetime(property_data['fromdate'])
    # property_data['fin_year_b'] = property_data['fromdate'].dt.year
    # property_data['fin_month_b'] = property_data['fromdate'].dt.month

    property_fin_yrmth_df = property_df.copy()
    ## find the (receiptdate) column year or month
    property_fin_yrmth_df['receiptdate'] = pd.to_datetime(property_df['receiptdate'],errors = 'coerce',format='%Y-%m-%d')
    property_fin_yrmth_df['fin_year_r'] = property_fin_yrmth_df['receiptdate'].dt.year
    property_fin_yrmth_df['fin_month_r'] = property_fin_yrmth_df['receiptdate'].dt.month
    print("Identified the financial year & month\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")

    return property_fin_yrmth_df,property_df


def unique_pkey_df(property_list_df):
    ### find the Unique property code (pid) from the property bill data
    unique_values = property_list_df.propertykey.unique()
    unique_pkey_df = pd.DataFrame({'propertykey': unique_values})

    print("defined the unique property key using property bill data\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
    return unique_pkey_df
