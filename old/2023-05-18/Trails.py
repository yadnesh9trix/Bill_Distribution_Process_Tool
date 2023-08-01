import datetime
import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import xlsxwriter
import openpyxl
from openpyxl import Workbook
from openpyxl.worksheet.datavalidation import DataValidation
from openpyxl.styles import Alignment


masterdata = "D:\PTAX Project\Bi Dashboard/Master_Plist_Data.xlsx"

df=  pd.read_excel(masterdata)
arrears_greaterthan_999 = df[df['Arrears'] > 999]
# arrears_greaterthan_999.columns
filter_df = arrears_greaterthan_999[arrears_greaterthan_999['Last Payment Date'] <= "2022-03-31"]
filter_df['Total Amount'] = filter_df['Arrears'] + filter_df['Current_Bill']

filter_df['Last Payment Date'] = np.where(filter_df['Last Paidamount'].isnull(),np.nan,filter_df['Last Payment Date'])

filter_df['Last Payment Date New'] = pd.to_datetime(filter_df['Last Payment Date'], format='%Y-%m-%d',errors='coerce').dt.date

filter_df = filter_df[['Zone_Type', 'gat_name', 'propertycode', 'propertyname',
       'own_mobile', 'assessmentdate', 'Arrears', 'Current_Bill','Total Amount',
       'Shasti_Flag', 'Use_Type','propertyaddress', 'Last Payment Date New', 'Last Paidamount']]

filter_df['gat_name'] = filter_df['gat_name'].replace(np.nan,"unknown")


filter_df_rename = filter_df.rename(columns={'Zone_Type':'झोन', 'gat_name':'गट क्र', 'propertycode':'मालमत्ता क्रमांक', 'propertyname':'मालकाचे नाव',
       'own_mobile':'मोबाईल क्र.', 'assessmentdate':'कर आकारणी दिनांक', 'Arrears':'थकबाकी', 'Current_Bill':'चालू मागणी रु.','Total Amount':'एकुण मागणी रु.',
       'Shasti_Flag':'अवैधबांधकाम शास्ती फ्लॅग', 'Use_Type':'वापर प्रकार','propertyaddress':'मालमत्तेचा पत्ता', 'Last Payment Date New':'मागील भरणा दिनांक', 'Last Paidamount':'मागील भरणा रक्कम'})

filter_df_rename['अडचण असण्याची कारणे']=""

# filter_df['Last Payment Date'] = pd.to_datetime(filter_df['Last Payment Date'], format='%Y-%m-%d',
#                                                                errors='ignore')
# filter_df['Last Payment Date'] = filter_df['Last Payment Date'].dt.date()
# sheet_name = ['1K to 50K', '50K to 1Lakh', '1Lakh to 5Lakh','5Lakh & Above']

lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
       'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi', 'Moshi']

gatt = filter_df['gat_name'].unique().tolist()
pathh = "D:\PTAX Project\Bi Dashboard/"

# [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 11.0, 10.0, 12.0, 13.0, 14.0, 16.0, 17.0, 18.0, 15.0, 0.0, 'unknown']

msterdatapath_ = "D:\PTAX Project\Bi Dashboard\Mapping/"
zonetype = pd.read_csv(msterdatapath_ + "zone.csv")
zonemap = dict(zip(zonetype['eng_zonename'], zonetype['zonename']))

for i in lst:
    writer = pd.ExcelWriter(pathh + "/" + f"DefaulterList_50K&Above_{i}.xlsx", engine="xlsxwriter")
    for j in gatt:
        zonee = filter_df_rename[(filter_df_rename['झोन'] == i) & (filter_df_rename['गट क्र'] == j)]
        df50k_above = zonee[zonee['एकुण मागणी रु.'] >= 50000]
        if len(zonee)==0:
            pass
        else:
            df50k_above['झोन'] = df50k_above['झोन'].map(zonemap)
            df50k_above.to_excel(writer, index=False, sheet_name=  f"गट क्र._({str(j)})")

            wb_length = len(df50k_above)
            worksheet = writer.sheets[f"गट क्र._({str(j)})"]
            rule = '"कोर्ट केस,कोर्ट केसस्टे,केंद्रीय सरकारमालमत्ता,राज्य सरकार मालमत्ता,महानगरपालिकेची मालमत्ता,रस्ता रुंदीकरण्यात पडलेली मालमत्ता,''दुबार मालमत्ता,मोकळी जमीन रद्द करणे,बंद कंपनी,पडीक/जीर्ण मालमत्ता,सापडत नसलेली मालमत्ता,BIFR/Liquidation,इतर,"'
            dropdown_range = f'O2:O{wb_length+1}'
            worksheet.data_validation(dropdown_range, {'validate': 'list', 'source': rule})

            worksheet.freeze_panes(1, 3)
            workbook = writer.book

            worksheet.set_column('A1:O1', 13)
            worksheet.set_column('D1:D1', 16)
            border_format = workbook.add_format({'border': 1,
                                                 'align': 'left',
                                                 'font_color': '#000000',
                                                 'font_size': 20})
            worksheet.conditional_format(f'A1:O{wb_length+1}', {'type': 'cell',
                                                    'criteria': '>=',
                                                    'value': 0,
                                                    'format': border_format})
            worksheet.set_row(wb_length+1, 22)
    writer.save()
writer.close()




# import openpyxl
# from openpyxl import Workbook
# from openpyxl.worksheet.datavalidation import DataValidation
# # # create a new workbook
# workbook = Workbook()
#
# for i in lst:
#     writer = pd.ExcelWriter(pathh + "/" + f"DefaulterList_50K&Above_{i}.xlsx", engine="xlsxwriter")
#     for j in gatt:
#         zonee = filter_df_rename[(filter_df_rename['झोन'] == i) & (filter_df_rename['गट क्र'] == j)]
#
#         worksheet = workbook.active
#         worksheet = writer.sheets[f"गट क्र._({str(j)})"]
#         rule = '"कोर्ट केस,कोर्ट केसस्टे,केंद्रीय सरकारमालमत्ता,राज्य सरकार मालमत्ता,महानगरपालिकेची मालमत्ता,रस्ता रुंदीकरण्यात पडलेली मालमत्ता,''दुबार मालमत्ता,मोकळी जमीन रद्द करणे,बंद कंपनी,पडीक/जीर्ण मालमत्ता,सापडत नसलेली मालमत्ता,BIFR,इतर,"'
#
#         # create a data validation object
#         dv = DataValidation(type="list", formula1=rule)
#         # add the data validation to a range of cells
#         worksheet.add_data_validation(dv)
#         dv.add('O1:O50')
#
#         # save the workbook
#         workbook.save(writer, index=False, sheet_name=  f"गट क्र._({str(j)})")



# for i in lst:
#     writer = pd.ExcelWriter(pathh + "/" + f"DefaulterPropertyList_{i}.xlsx", engine="xlsxwriter")
#     zonee = filter_df[filter_df['Zone_Type'] == i]
#
#     df1kto50k = zonee[(zonee['Total Amount'] >= 1000) & (zonee['Total Amount'] < 50000)]
#     df50kto1lac = zonee[(zonee['Total Amount'] >= 50000) & (zonee['Total Amount'] < 100000)]
#     df1lacto5lac = zonee[(zonee['Total Amount'] >= 100000) & (zonee['Total Amount'] < 500000)]
#     df5lac_above = zonee[(zonee['Total Amount'] >= 500000)]
#
#     # df1kto50k.to_excel(pathh + "/" + f"DefaulterPropertyList_{i}.xlsx", index=False, sheet_name= '1K to 50K')
#     # df50kto1lac.to_excel(pathh + "/" + f"DefaulterPropertyList_{i}.xlsx", index=False, sheet_name= '50K to 1Lakh')
#     # df1lacto5lac.to_excel(pathh + "/" + f"DefaulterPropertyList_{i}.xlsx", index=False, sheet_name= '1Lakh to 5Lakh')
#     # df5lac_above.to_excel(pathh + "/" + f"DefaulterPropertyList_{i}.xlsx", index=False, sheet_name= '5Lakh & Above')
#
#     df1kto50k.to_excel(writer, index=False, sheet_name= '1K to 50K')
#     df50kto1lac.to_excel(writer, index=False, sheet_name= '50K to 1Lakh')
#     df1lacto5lac.to_excel(writer, index=False, sheet_name= '1Lakh to 5Lakh')
#     df5lac_above.to_excel(writer, index=False, sheet_name= '5Lakh & Above')

#     writer.save()
#     writer.close()

