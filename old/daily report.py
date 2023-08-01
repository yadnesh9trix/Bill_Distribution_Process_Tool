import pandas as pd
import datetime
import warnings
warnings.filterwarnings('ignore')
import os
# import read_data as rd

today = datetime.datetime.today().date()
# tday  =today.strftime("%d_%m_%Y")


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
    zonemap = dict(zip(zonetype['zonename'], zonetype['eng_zonename']))
    # zonemap = dict(zip(zonetype['zonekey'],zonetype['eng_zonename']))
    # final_output['Zone_Type'] = final_output['zonekey'].map(zzz)

    gattype = pd.read_csv(mappath + "gat.csv")
    gattype['gatname_z'] = gattype['gatname'].astype(str) + "_" + gattype['zonetype'].astype(str)
    gatnamemap = dict(zip(gattype['gat'], gattype['gatname_z']))

    return zonemap,usemap,construcmap,occpmap,subusemap,gatnamemap



def dailyreport(inppath,zonemap,outpath):
    ####==========================================================================================================================
    ytddata = pd.read_excel(inppath + "Paidamount list.xlsx", sheet_name="Total")
    # zz = dict(zip(zonetype['zonename'], zonetype['eng_zonename']))
    ytddata['eng_zone'] = ytddata['ezname'].map(zonemap)
    sumgrpby = ytddata.groupby(['ezname', 'eng_zone']).agg({'magil': 'sum', 'chalu': 'sum'}).reset_index()
    sumgrpby['sum_of_magil_in_cr'] = sumgrpby['magil'] / 10000000
    sumgrpby['sum_of_chalu_in_cr'] = sumgrpby['chalu'] / 10000000
    lissy = ['Nigdi Pradhikaran','Akurdi','Chinchwad','Thergaon','Sangvi','Pimpri Waghere',
             'Pimpri Nagar','MNP Bhavan','Fugewadi Dapodi','Bhosari','Charholi',
             'Moshi','Chikhali','Talvade','Kivle','Dighi Bopkhel','Wakad']

    d = {v: k for k, v in enumerate(lissy)}
    df_YTD = sumgrpby.sort_values('eng_zone', key=lambda x: x.map(d), ignore_index=True)
    # df_YTD.to_excel(outpath + "today_TodalTax_collection.xlsx",index=False)

    ####==========================================================================================================================
    ###
    tddata = pd.read_excel(inppath + "Paidamount_list.xlsx", sheet_name="TD")
    grpbytot = tddata.groupby(['ezname', 'gatname']).agg({'paidamount': 'sum'}).reset_index()
    grpbytot['eng_zone'] = grpbytot['ezname'].map(zonemap)
    # aaaa = grpbytot.melt(id_vars=['ezname', 'gatname','eng_zone'], var_name='groubysum', value_name='paidamount')
    pvotable = grpbytot.pivot_table(index=['eng_zone', 'ezname'], columns='gatname', values='paidamount')
    pp = pvotable.reset_index()

    lissy = ['Nigdi Pradhikaran','Akurdi','Chinchwad','Thergaon','Sangvi','Pimpri Waghere',
             'Pimpri Nagar','MNP Bhavan','Fugewadi Dapodi','Bhosari','Charholi',
             'Moshi','Chikhali','Talvade','Kivle','Dighi Bopkhel','Wakad']

    d = {v: k for k, v in enumerate(lissy)}
    df_TD = pp.sort_values('eng_zone', key=lambda x: x.map(d), ignore_index=True)
    # df_TD.to_excel(outpath + "ytd_GatZoneWiseTax_Collection.xlsx",index=False)

    return df_TD,df_YTD


if __name__ == '__main__':
    std_path= r"C:\PTAX Project\PTAx\Manual Daily Report/"
    inppath = std_path + "Input/" + today + "/"
    outpth = std_path + "output/" + today + "/"
    if os.path.exists(outpth):
        pass
    else:
        os.mkdir(outpth)

    mappath = std_path + "Mapping/"

    zonemap, usemap, construcmap, occpmap, subusemap, gatnamemap =  mapping_type(mappath)

    dailyreport(inppath,zonemap,outpth)
    # writer = pd.ExcelWriter(outpth + )