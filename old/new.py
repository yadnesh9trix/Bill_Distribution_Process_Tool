import pandas as pd

std_path = r"E:\repo\PTAx/"
inppath = std_path + "Input/"
outpth = std_path + "output/"

# inpdata = inppath + "collection report.xlsx"

final_output = pd.read_csv(inppath + "Property_List_Dump.csv", low_memory=False)

usertype = pd.read_csv(inppath + "usetype.csv")
uuu = dict(zip(usertype['usetypekey'], usertype['eng_usename']))
final_output['Use_Type'] = final_output['usetypekey'].map(uuu)

consttype = pd.read_csv(inppath + "constructiontype.csv")
ccc = dict(zip(consttype['constructiontypekey'], consttype['eng_constructiontypename']))
final_output['Construction_Type'] = final_output['constructiontypekey'].map(ccc)

occptype = pd.read_csv(inppath + "occupancy.csv")
ooo = dict(zip(occptype['occupancykey'], occptype['occupancyname']))
final_output['Occupancy_Type'] = final_output['occupancykey'].map(ooo)

subusetype = pd.read_csv(inppath + "subusetype.csv")
sss = dict(zip(subusetype['subusetypekey'], subusetype['eng_subusename']))
final_output['Subuse_Type'] = final_output['subusetypekey'].map(sss)

zonetype = pd.read_csv(inppath + "zone.csv")
zzz = dict(zip(zonetype['zone'], zonetype['eng_zonename']))
final_output['Zone_Type'] = final_output['zone'].map(zzz)

gattype = pd.read_csv(inppath + "gat.csv")
gattype['gatname_z'] = gattype['gatname'].astype(str) + "_"+ gattype['zonetype'].astype(str)
ggg = dict(zip(gattype['gat'], gattype['gatname_z']))
final_output['gat_name'] = final_output['gat'].map(ggg)

specialowner = pd.read_csv(inppath + "specialownership.csv")
splown = dict(zip(specialowner['specialownership'], specialowner['eng_specialownershipname']))
final_output['specialowner_Type'] = final_output['specialownership'].map(splown)

splacctype = pd.read_csv(inppath + "specialoccupant.csv")
splacc = dict(zip(splacctype['specialoccupantkey'], splacctype['eng_specialoccupantname']))
final_output['specialoccupant_Type'] = final_output['specialoccupantkey'].map(splacc)

fnal = final_output[['propertykey','zone', 'gat', 'totalarea',
       'permission', 'usetypekey', 'finalusetype', 'subusetypekey',
       'constructiontypekey', 'occupancykey', 'specialoccupantkey',
       'specialownership', 'occupantname','Use_Type', 'Construction_Type', 'Occupancy_Type',
       'Subuse_Type', 'Zone_Type', 'gat_name', 'specialowner_Type',
       'specialoccupant_Type']]

final_output.to_csv(outpth + "Property_List_Details.csv", index=False)


NOt_paid_ly4yr = pd.read_csv(outpth + "NOt_PAid_4LYYRS.csv", low_memory=False)

merge_all = NOt_paid_ly4yr.merge(final_output, on = 'propertykey' , how ='inner')
merge_all =merge_all.drop(columns='Unnamed: 0')

zonewise = merge_all.groupby(['zone','Zone_Type','gat_name','Use_Type','Subuse_Type']).agg({'billamount':'sum','propertykey':'count'}).reset_index()
zonewise.to_csv(outpth + "PCMC_Ptax_Summary.csv", index=False)

# merge_all.to_csv(outpth + "PCMC_Ptax_4Years_Defaulters.csv", index=False)


print(True)