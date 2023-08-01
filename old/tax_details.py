import pandas as pd
import datetime
import warnings
warnings.filterwarnings('ignore')
import os
# import read_data as rd

today = datetime.datetime.today().date()
tday  =today.strftime("%d_%b_%Y")


def taxdetails(inppath,outpth,usemap):
    property_list_df = pd.read_csv(inppath + "Property_List.csv",low_memory=False)
    # property_tax_df = pd.read_csv(inppath + "TAX_Details.csv",low_memory=False)
    # ptaxname = pd.read_excel(inppath + "propertytaxname.xlsx")

    # property_dff = property_list_df.merge(property_tax_df,on='propertykey',how ='left')
    # ssss = property_tax_df[['propertytaxkey',
    #                         'eng_propertytaxname']]
    # aaaxa = ssss.drop_duplicates()
    # ptax_sort_val = aaaxa.sort_values('propertytaxkey').reset_index(drop=True)
    #
    # property_dff_arange = property_list_df[['propertykey', 'propertycode',
    #                                     'ratablevalue', 'taxable', 'finalusetype','usetypekey', 'eng_propertytaxname',
    #                                     'propertytaxname', 'billamount',]]

    property_dff_arange = property_list_df[['propertykey', 'propertycode',
                                        'ratablevalue', 'taxable', 'usetypekey', 'eng_propertytaxname',
                                        'propertytaxname', 'billamount',
                                        'totalbillamount']]

    # property_dff_arrg_taxable = property_dff_arange[property_dff_arange['taxable'] != 'N']

    property_dff_arrg_taxable = property_dff_arange.copy()
    property_dff_arrg_taxable['Type_of_Use'] = property_dff_arrg_taxable['usetypekey'].map(usemap)
    # property_dff_arrg_taxable_df = property_dff_arrg_taxable.loc[property_dff_arrg_taxable['Type_of_Use'].isin(['Residential', 'Non-Residential'])]
    # property_dff_arrg_taxable_df = property_dff_arrg_taxable_df.reset_index(drop=True)

    property_dff_arrg_taxable_df = property_dff_arrg_taxable[['propertykey', 'eng_propertytaxname',
                                                              'Type_of_Use','ratablevalue','billamount']]
    # property_dff_arrg_taxable_df = property_dff_arrg_taxable[['propertykey', 'eng_propertytaxname',
    #                                                           'Type_of_Use','ratablevalue']]

    # noodprop = property_dff_arrg_taxable_df.groupby('eng_propertytaxname')['Type_of_Use'].sum().reset_index()

    grrp11 = property_dff_arrg_taxable_df.groupby(['Type_of_Use', 'eng_propertytaxname']).agg(
        {'ratablevalue': 'sum', 'billamount': 'sum', 'propertykey': 'count'}).reset_index()

    ### 10-02-2023

    df_1to12k = property_dff_arrg_taxable_df[property_dff_arrg_taxable_df['ratablevalue'] <= 12000]
    df_12001to30k = property_dff_arrg_taxable_df[property_dff_arrg_taxable_df['ratablevalue'] <= 30000]
    df_30001_above = property_dff_arrg_taxable_df[property_dff_arrg_taxable_df['ratablevalue'] > 30001]
    df_1to12k_gropby = df_1to12k.groupby(['Type_of_Use', 'eng_propertytaxname']).agg(
        {'ratablevalue': 'sum', 'billamount': 'sum', 'propertykey': 'count'}).reset_index()
    df_12001to30k_grpby = df_12001to30k.groupby(['Type_of_Use', 'eng_propertytaxname']).agg(
        {'ratablevalue': 'sum', 'billamount': 'sum', 'propertykey': 'count'}).reset_index()
    df_30001_above_grpby = df_30001_above.groupby(['Type_of_Use', 'eng_propertytaxname']).agg(
        {'ratablevalue': 'sum', 'billamount': 'sum', 'propertykey': 'count'}).reset_index()


    filter_1to12k_gropby = df_1to12k_gropby.loc[df_1to12k_gropby['Type_of_Use'].isin(['Residential', 'Non-Residential'])]
    filter_12001to30k_grpby = df_12001to30k_grpby.loc[df_12001to30k_grpby['Type_of_Use'].isin(['Residential', 'Non-Residential'])]
    filter_30001_above_grpby = df_30001_above_grpby.loc[df_30001_above_grpby['Type_of_Use'].isin(['Residential', 'Non-Residential'])]

    filter_1to12k_gropby.to_csv(outpth +"/"+"filter_1to12k_gropby.csv")
    filter_12001to30k_grpby.to_csv(outpth +"/"+"filter_12001to30k_grpby.csv")
    filter_30001_above_grpby.to_csv(outpth +"/"+"filter_30001_above_grpby.csv")

    print(True)
    # property_dff_arrg_taxable_df = grrp11.loc[grrp11['Type_of_Use'].isin(['Residential', 'Non-Residential'])]

    grrp1_rename = property_dff_arrg_taxable_df.rename(columns={'propertykey':"Number_Of_Properties",'eng_propertytaxname':'Name_Of_Tax',
                                                 'ratablevalue':'Total_Rateable_value','billamount':'Current_Demand'})

    soprrt = grrp1_rename.sort_values(['Name_Of_Tax', 'Type_of_Use'])

    # soprrt.to_csv(outpth +"/"+"tax_details.csv")

    soprrt['Current_Rate%']=0
    soprrt['Increase_In_Rate%']=0
    soprrt['Increase_In_Demand']=0

    soprrt_df = soprrt[['Name_Of_Tax', 'Type_of_Use','Number_Of_Properties','Total_Rateable_value','Current_Rate%',
                        'Current_Demand','Increase_In_Rate%','Increase_In_Demand']]

    soprrt_df.to_excel(outpth +"/"+"PCMC_Ptax_IncRate.xlsx",index=False)

    # ss = property_dff_arrg_taxable['eng_propertytaxname'].unique().tolist()
    # dfffd = pd.DataFrame(ss, columns=['propertytaxname'])
    # dfffd.to_excel(outpth + "/" + "propertytaxname.xlsx")
    # grrp11 = property_dff_arrg_taxable.groupby(['Type_of_Use', 'eng_propertytaxname', 'billamount']).agg(
    #     {'ratablevalue': 'sum', 'Type_of_Use': 'sum'}).reset_index()

    print(True)
    pass





if __name__ == '__main__':
    std_path= r"C:\PTAX Project\PTAx/"
    inppath = std_path + "Input/"
    outpth = std_path + "output/" + tday
    if os.path.exists(outpth):
        pass
    else:
        os.mkdir(outpth)
        outpth = outpth + "/"
    mappath = std_path + "Mapping/"

    usetype = pd.read_csv(mappath + "usetype.csv")
    usemap = dict(zip(usetype['usetypekey'],usetype['eng_usename']))
    taxdetails(inppath,outpth,usemap)