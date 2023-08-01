#import required libraries
import pandas as pd
import numpy as np
import os
import datetime
import warnings
warnings.filterwarnings('ignore')
import ptax_process as pt
import read_filter_data as rfd
import excelcsv_writer as ew
import collectionVsSMSProcess as csmsp
from openpyxl import load_workbook
#-----------------------------------------------------------------------------------------------------------------------
## Define the today's date
today = datetime.datetime.today().date()
tday  =today.strftime("%d_%b_%Y")

#-----------------------------------------------------------------------------------------------------------------------
## Start
if __name__ == '__main__':
    std_path= r"C:\PTAX Project\PTAx/"
    inppath = std_path + "Input/"
    outpth = std_path + "output/" + tday
    if os.path.exists(outpth):
        pass
    else:
        os.mkdir(outpth)
    mappath = std_path + "Mapping/"
    print('Your ptax model is running.\nPlease wait...\n============================='
          '===============================================================================')

    # Read collection data of Today's
    ytddata_df =  csmsp.todaycollection(inppath)

    # csmsp.total5yearscollcetion(inppath,outpth,ytddata_df)

    ### SMS camapign which sent SMS on 14th march shasti property 9575/95Cr.
    csmsp.noarrears_onlycurrentbills51k(inppath,outpth,ytddata_df)

    # Identify PID of SMS campign which is match with mseb mobile number property 2923 out of 23K
    csmsp.noarrears_onlycurrentbills75k(inppath,outpth,ytddata_df)

    # # For daily updates graph Outstanding Vs Collection
    csmsp.ShastiVsCollection18706(inppath,outpth,ytddata_df)

    # Identify PID of SMS campign which is match with mseb mobile number property 2923 out of 23K
    csmsp.SMSCampign2923_23K(inppath,outpth,ytddata_df)

    # # Identify PID of SMS campign which is sent message of property 9500
    csmsp.Shasti_ytddata9500SMS(inppath,outpth,ytddata_df)

    # # Identify PID of SMS campign which is match with mseb mobile number property 2476 out of 23K
    csmsp.SMScampign2476(inppath,outpth,ytddata_df)

    # Identify the list who have Arrears& currenbills pending paid from SMS campign VS collection(1LAc 10K)
    csmsp.ArrearsCrntBillsPending_WoutShastiPlist_1lac10K(inppath,outpth,ytddata_df)