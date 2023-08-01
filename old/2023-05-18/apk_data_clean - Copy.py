import datetime
import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import os
import openpyxl


# path = r"D:\PTAX Project/Ptax_Project/Apk_output/"
# plist = pd.read_csv(path + "Property_List_24042023.csv",low_memory=False)
# dff = pd.read_excel(path+ "Visit_List-Wrong Property Code.xlsx")
# print(True)

lst = ['Akurdi', 'Bhosari', 'Dighi Bopkhel', 'MNP Bhavan', 'Nigdi Pradhikaran', 'Talvade', 'Chinchwad', 'Chikhali',
       'Pimpri Nagar', 'Thergaon', 'Wakad', 'Kivle', 'Fugewadi Dapodi', 'Pimpri Waghere', 'Sangvi', 'Charholi', 'Moshi']

path = "D:\PTAX Project\Bi Dashboard/"

# # open the Excel workbook
# for i in lst:
#     workbook = openpyxl.load_workbook(path+ f"DefaulterList_50K&Above_{i}.xlsx")
#
#     # loop through each sheet in the workbook
#     for sheet in workbook:
#         # set the wrap text property for the first row
#         for cell in sheet[1]:
#             cell.alignment = openpyxl.styles.Alignment(wrap_text=True)
#
#     # save the updated workbook
#
#     pth= path + "output/"
#     workbook.save(pth + f'DefaulterList_50K&Above_{i}.xlsx')

today = datetime.datetime.today().date()
tday  =today.strftime("%d_%b_%Y")

std_path = r"D:/PTAX Project/Ptax_Project/"
inppath = std_path + "Input/"
outpth = std_path + "output/" + tday + "/"
if os.path.exists(outpth):
       pass
else:
       os.mkdir(outpth)

# input_data = "D:\PTAX Project\Ptax_Project\Input/"

data = pd.read_excel(inppath + "Bill Distribution Dashboard.xlsx",sheet_name='Final')

# ['__v', '_id', 'addressUpdated', 'alternateMobileUpdated', 'createdAt',
#        'gat', 'isAddressUpdated', 'isAlternateMobileUpdated', 'isCodeSend',
#        'isMobileUpdated', 'isPropertyCodeRight', 'isPropertyFound',
#        'mobileUpdated', 'propertyCode', 'propertyImg', 'propertyLat',
#        'propertyLong', 'upayogakartaShulkName', 'upayogakartaShulk_id',
#        'updatedAt', 'visitDate', 'visit_id', 'visitingPersonContactNo',
#        'visitingPersonName', 'visitingPerson_id', 'zone', 'Unnamed: 26'

# data = data.drop_duplicates('propertyCode')
data1 = data.sort_values(['propertyCode']).drop_duplicates('propertyCode',keep='last')
data1 = data1[~data1['visitingPersonName'].isin(['Pravin','Ravikiran','Test Contractor 3','Test Contractor 1','Test Contractor 6','Vinit Kale','Ramdas'])]


# data1 =  data1[['Zone', 'Gat','propertyCode','mobileUpdated','visitingPersonName','visitDate', 'visit_id',
#        'visitingPersonContactNo', 'visitingPerson_id', 'propertyLat', 'propertyLong','_id', 'addressUpdated',
#        'alternateMobileUpdated', 'createdAt','gat_dump', 'isAddressUpdated', 'isAlternateMobileUpdated',
#        'isCodeSend', 'isMobileUpdated', 'isPropertyCodeRight',
#        'isPropertyFound' , 'propertyImg',
#         'upayogakartaShulkName','upayogakartaShulk_id', 'updatedAt' ]]

data1 =  data1[['Zone', 'Gat','propertyCode','mobileUpdated','visitingPersonName','visitDate', 'visit_id',
       'visitingPersonContactNo', 'visitingPerson_id', 'propertyLat', 'propertyLong', 'addressUpdated',
       'alternateMobileUpdated',  'upayogakartaShulkName','upayogakartaShulk_id' ]]

data1['visitDate'] = pd.to_datetime(data1['visitDate'], format='%Y-%m-%d',errors='coerce').dt.date


msterdatapath_ = "D:\PTAX Project\Bi Dashboard\Mapping/"
zonetype = pd.read_csv(msterdatapath_ + "zone.csv")
zonemap = dict(zip(zonetype['eng_zonename'], zonetype['zonename']))

writer = pd.ExcelWriter(outpth + "/" + f"Random_List.xlsx", engine="xlsxwriter")

for i in lst:
       zonee = data1[(data1['Zone'] == i)]
       random_rows = zonee.sample(n=100)

       random_rows['Zone'] = random_rows['Zone'].map(zonemap)
       wb_length = len(random_rows)
       random_rows.to_excel(writer, index=False, sheet_name=f"{str(i)}")

       worksheet = writer.sheets[f"{str(i)}"]
       worksheet.freeze_panes(1, 3)
       workbook = writer.book

       border_format = workbook.add_format({'border': 1,
                                            'align': 'left',
                                            'font_color': '#000000',
                                            'font_size': 20})
       worksheet.conditional_format(f'A1:O{wb_length + 1}', {'type': 'cell',
                                                             'criteria': '>=',
                                                             'value': 0,
                                                             'format': border_format})
       worksheet.set_row(wb_length + 1, 22)
       worksheet.set_column(f"A1:Z{wb_length}", 15)

writer.save()
writer.close()


# ggg = data1.groupby(['visitingPersonName'])['propertyCode'].count().reset_index()
# ggg.to_excel(input_data+ "visitreports_excel.xlsx",index=False)