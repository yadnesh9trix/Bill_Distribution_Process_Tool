#import required libraries
import pandas as pd
import os
import datetime
import warnings
warnings.filterwarnings('ignore')
import ptax_process as pt
import read_filter_data as rfd
import excelcsv_writer as ew
import data_process as dp

## Define the today's date
today = datetime.datetime.today().date()
tday  =today.strftime("%d_%b_%Y")

## Start
if __name__ == '__main__':
    std_path= r"D:/PTAX Project/Ptax_Project/"
    inppath = std_path + "Input/"
    outpth = std_path + "output/" + tday + "/"
    if os.path.exists(outpth):
        pass
    else:
        os.mkdir(outpth)
    mappath = std_path + "Mapping/"
    print('Your ptax model is running.\nPlease wait...\n============================='
          '===============================================================================')
    #===================================================================================================================
    # fetching class object of property tax data process
    ptprocess = pt.ptax_process()
    #-------------------------------------------------------------------------------------------------------------------------------------

    ### Read all property related master data mapping files such as zone,gat, usage type ...
    zonemap,usemap,construcmap,occpmap,subusemap,\
        gatnamemap,splownmap,splaccmap = rfd.mapping_type(mappath)

    #-------------------------------------------------------------------------------------------------------------------------------------
    ## Defined the property bill details, receipt details and property list details.
    property_bill_df, property_list_df, property_receipt_df = rfd.read_data(inppath)

    #-------------------------------------------------------------------------------------------------------------------------------------
    ## filtering the property bill data
    # filter_property_df,property_df = rfd.filter_data(property_bill_df)

    #-------------------------------------------------------------------------------------------------------------------------------------
    ## find the unique property key using property bill data
    unique_pkey_df = rfd.unique_pkey_df(property_list_df)

    #-------------------------------------------------------------------------------------------------------------------------------------
    ytddata_df = dp.collection_data(inppath)

    #-------------------------------------------------------------------------------------------------------------------------------------
    # 03-04-2023
    # defined the process of arrears bills details
    AllBillArrears = dp.find_arrears_cntbill(outpth, inppath, ytddata_df, property_bill_df, property_list_df)

    plist_arrange, merge_maxfyrmth_uniqpid_df_Year_r,\
        property_finmth_df,filterdata_lessthan_2022 = dp.property_FY_process(outpth,inppath,ytddata_df,property_receipt_df,
                                                                             property_list_df,unique_pkey_df)

    # receipt details process and property defaulters
    dp.newlist_receiptdate(inppath,outpth,plist_arrange, merge_maxfyrmth_uniqpid_df_Year_r,
        property_finmth_df,filterdata_lessthan_2022,AllBillArrears,ytddata_df,property_list_df,
                           zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap)

