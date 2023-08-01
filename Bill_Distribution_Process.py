import datetime
import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import os
import read_filter_data as rfd
import csv
from datetime import datetime,timedelta

#------------------------------------------------------
today = datetime.today().date()
tday  =today.strftime("%d%m%Y")


last = today - timedelta(days=0)
report_date  = last.strftime("%d%m%Y")

#-----------------------------------------------------
std_path = r"D:\PTAX Project\Bill_Distribution_Process_Tool/"
inppath = std_path + "Input/"
outpth = std_path + "output/" + tday + "/"
if os.path.exists(outpth):
       pass
else:
       os.mkdir(outpth)
mappath = std_path + "Mapping/"

##
zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap,splownmap,splaccmap = rfd.mapping_type(mappath)

## Read
bill_data = pd.read_excel(inppath + f"visitreports_{tday}.xlsx")
# bill_data = pd.read_csv(inppath + "visitreports_12052023.csv",encoding='utf-8')

## Visit Person list
wout_visitperson = pd.read_excel(inppath + "VisitPerson.xlsx", sheet_name="OG")
wout_visitperson_list = wout_visitperson['visitingPersonName'].tolist()
#-----------------------------------------------------------------------------------------------------------------------
Visit_person_data1 = bill_data[~bill_data['visitingPersonName'].isin(wout_visitperson_list)]

# Visit_person_data1['propertyCode'] =  Visit_person_data1['propertyCode'].\
#                                                         apply(lambda x: str(x).split('.')[0] if str(x).find('.') != -1 else x)

# Visit_person_data1.dropna(subset=['propertyCode'], how='all', inplace=True)
# Visit_person_data1['propertyCode'] = Visit_person_data1['propertyCode'].str.replace(',', '')\
#        .str.replace(' ', '').str.replace('-', '')\
#        .str.replace('102PTAX_2023002071', '00000000').astype(float)

Visit_person_data1['propertyCode'] = Visit_person_data1['propertyCode'].apply("{:.02f}".format)

# duplicates = Visitperson_datesorted[Visitperson_datesorted.duplicated(subset='propertyCode')]

## Convert Visit Date into date format
Visit_person_data1['visitDate'] = pd.to_datetime(Visit_person_data1['visitDate'],errors='coerce').dt.date

Visitperson_datesorted = Visit_person_data1.sort_values(['visitDate','propertyCode'])
Visitperson_dropduplicates = Visitperson_datesorted.drop_duplicates('propertyCode',keep='last',ignore_index=True)

VisitPerson_rename = Visitperson_dropduplicates.rename(columns ={'propertyCode':'propertycode'})

#=======================================================================================================================
## Read property List data
property_list_df = pd.read_csv(inppath + "Property_List_24042023.csv", low_memory=False)
property_list_df = property_list_df[property_list_df['verified'] != 'N']

property_list_df.dropna(subset=['propertycode'], how='all', inplace=True)
property_list_df.dropna(subset=['propertykey'], how='all', inplace=True)

property_list_df = property_list_df.drop_duplicates('propertycode')

property_list_df['Zone_Type'] = property_list_df['zone'].map(zonemap)
property_list_df['gat_name'] = property_list_df['gat'].map(gatnamemap)
property_list_df =  property_list_df[['Zone_Type', 'gat_name', 'propertycode']]

#=======================================================================================================================

##----------------------------------------------------------------------------------------------------------------------
## Paid-amount data of Last Year
ytddata = pd.read_excel(inppath + f"Paid_amount 2022-04-01 To 2023-03-31. (1).xlsx",sheet_name='2022-04-01 To 2023-03-31')
## select only pid column from ytd data
ytddata =  ytddata[['propertycode','receiptdate']]
ytddata = ytddata.rename(columns={"receiptdate":'Last_yr_receiptdate'})
## Replace the property code values in ytd data
ytddata["propertycode"] = ytddata["propertycode"].replace("1100900002.10.10", "1100900002.20")
ytddata['propertycode'] = ytddata['propertycode'].astype(float)
ytddata['propertycode'] = ytddata['propertycode'].apply("{:.02f}".format)
ytddata = ytddata.drop_duplicates('propertycode')
ytddata.dropna(subset=['propertycode'], how='all', inplace=True)

#=======================================================================================================================
# Paid Amount data of This year
ytddata_ty = pd.read_excel(inppath + f"Paidamount_list_{report_date}.xlsx", sheet_name="Total")
## select only pid column from ytd data
ytddata_ty =  ytddata_ty[['propertycode','receiptdate','paidamount']]
ytddata_ty = ytddata_ty.rename(columns={"receiptdate":'TY_receiptdate','paidamount':'TY_paidamount'})
## Replace the property code values in ytd data
ytddata_ty["propertycode"] = ytddata_ty["propertycode"].replace("1100900002.10.10", "1100900002.20")
ytddata_ty['propertycode'] = ytddata_ty['propertycode'].astype(float)
ytddata_ty['propertycode'] = ytddata_ty['propertycode'].apply("{:.02f}".format)
ytddata_ty = ytddata_ty.drop_duplicates('propertycode')
ytddata_ty.dropna(subset=['propertycode'], how='all', inplace=True)

#-----------------------------------------------------------------------------------------------------------------------
## Merge the data
merege_df = VisitPerson_rename.merge(property_list_df,on='propertycode',how='left')
finalddd = merege_df.merge(ytddata,on='propertycode',how='left')
finalddd_ata = finalddd.merge(ytddata_ty,on='propertycode',how='left')

##----------------------------------------------------------------------------------------------------------------------
finalddd_ata_Rename = finalddd_ata.rename(columns={"Zone_Type":'Zone','gat_name':'Gat'})
finalddd_ata_Rename['Last_yr_receiptdate'] = pd.to_datetime(finalddd_ata_Rename['Last_yr_receiptdate'],format="%Y-%m-%d")
finalddd_ata_Rename['TY_receiptdate'] = pd.to_datetime(finalddd_ata_Rename['TY_receiptdate'],format="%Y-%m-%d")

finalddd_ata_Rename['LY_receiptmonth'] = finalddd_ata_Rename['Last_yr_receiptdate'].dt.month
finalddd_ata_Rename['TY_receiptmonth'] = finalddd_ata_Rename['TY_receiptdate'].dt.month

finalddd_ata_Rename['Flag(AftrBillDist_Paid)'] = np.where(finalddd_ata_Rename['TY_receiptdate'] >= finalddd_ata_Rename['visitDate'],1,0)

# finalddd_ata_Rename['Flag(EarlyPayers)'] = np.where((finalddd_ata_Rename['LY_receiptmonth'] <= finalddd_ata_Rename['TY_receiptmonth']) |
#                                               (finalddd_ata_Rename['LY_receiptmonth']==0) | (finalddd_ata_Rename['TY_receiptmonth'] >0),
#                                               "Yes",'No')
finalddd_ata_Rename['Flag(EarlyPayers)']=  np.where(((finalddd_ata_Rename['LY_receiptmonth'] <= finalddd_ata_Rename['TY_receiptmonth']))
                                                    & (finalddd_ata_Rename['LY_receiptmonth']==0) & (finalddd_ata_Rename['TY_receiptmonth'] >0),'Yes', 'No')
#-----------------------------------------------------------------------------------------------------------------------
## rearrange the columns in standard format
# bill_distributed = finalddd_ata_Rename[['addressUpdated', 'alternateMobileUpdated', 'createdAt',
#        'isAddressUpdated', 'isAlternateMobileUpdated', 'isCodeSend',
#        'isMobileUpdated', 'isPropertyCodeRight', 'isPropertyFound',
#        'mobileUpdated', 'propertycode', 'propertyImg', 'propertyLat',
#        'propertyLong', 'upayogakartaShulkName', 'updatedAt', 'visitDate',
#        'visitingPersonContactNo', 'visitingPersonName', 'visitingPerson_id',
#        'Zone', 'Gat', 'Last_yr_receiptdate', 'TY_receiptdate',
#        'TY_paidamount', 'LY_receiptmonth', 'TY_receiptmonth', 'Flag(AftrBillDist_Paid)',
#        'Flag(EarlyPayers)']]

bill_distributed = finalddd_ata_Rename[['addressUpdated', 'alternateMobileUpdated', 'createdAt',
       'isAddressUpdated', 'isAlternateMobileUpdated', 'isCodeSend',
       'isMobileUpdated', 'isPropertyCodeRight',
       'mobileUpdated', 'propertycode', 'propertyImg', 'propertyLat',
       'propertyLong', 'upayogakartaShulkName', 'updatedAt', 'visitDate',
       'visitingPersonContactNo', 'visitingPersonName', 'visitingPerson_id',
       'Zone', 'Gat', 'Last_yr_receiptdate', 'TY_receiptdate',
       'TY_paidamount', 'LY_receiptmonth', 'TY_receiptmonth', 'Flag(AftrBillDist_Paid)',
       'Flag(EarlyPayers)']]

#-----------------------------------------------------------------------------------------------------------------------
## Final Dump
bill_distributed.to_excel(outpth + "Master_Bill_Distributed_Payments.xlsx",index=False,sheet_name='Final')

print("Bill Distribution data process is Completed")