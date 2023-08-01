import datetime
import pandas as pd
import datetime as dt
import time
import numpy as np
import warnings
warnings.filterwarnings('ignore')

class ptax_process():

    def __int__(self):
        pass

    def ptaxbill_process(self,filter_propertybill_df,unique_pid_df):

        property_financial_yrmth_df = self.fin_yearmonth(filter_propertybill_df)

        ## find the GroupBy MAX PID, financial year or month using receipt Date
        property_fyrmth_r_max = property_financial_yrmth_df.groupby(['propertykey'])['fin_year_r','fin_month_r'].max().reset_index()

        # Apply merge using unique property id
        merge_maxfyrmth_uniqpid_df_Year_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey',how='inner')

        ## drop data if financial year is less than 2022
        filterdata_lessthan_2022 = merge_maxfyrmth_uniqpid_df_Year_r[merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'] != 2022]

        return merge_maxfyrmth_uniqpid_df_Year_r,filterdata_lessthan_2022,property_financial_yrmth_df

    def fin_yearmonth(self,property_data):
        #### find the financial year & Month
        property_data['fin_year_r'] = np.where(property_data['fin_month_r'] <4,property_data['fin_year_r']-1,property_data['fin_year_r'])
        property_data['fin_month_r'] = np.where(property_data['fin_month_r']<4,property_data['fin_month_r']+9,property_data['fin_month_r']-3)
        print("Identied the financial year & month using receipt date column\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")

        fin_yearmonth_propertydf = property_data.copy()
        return fin_yearmonth_propertydf

    def paid_last_qtr_LY(self,filterdata_lessthan_2022):

        ## find the defaulter list might, may and major
        might_default_df = filterdata_lessthan_2022[(filterdata_lessthan_2022['fin_month_r'] > 9) &
                                                    (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
        might_default_df['paid_last_qtr_LY'] = 1
        print("Calculated the property paid last quarter in LY\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return might_default_df

    def paid_by_dec_LY(self,filterdata_lessthan_2022):
        may_default_df = filterdata_lessthan_2022\
                                [(filterdata_lessthan_2022['fin_month_r'] < 10) &
                                                  (filterdata_lessthan_2022['fin_year_r'] == 2021)].reset_index(drop=True)
        may_default_df['paid_by_dec_LY'] = 1
        print("Calculated the property paid by dec LY\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return may_default_df

    def not_paid_LY(self,filterdata_lessthan_2022):
        major_default_df = filterdata_lessthan_2022\
                                [(filterdata_lessthan_2022['fin_year_r'] < 2021)].reset_index(drop=True)
        major_default_df['Not_paid_LY'] = 1
        print("Calculating the property not paid in LY \n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return major_default_df


    def not_paid_yet_in4ly(self,merge_maxfyrmth_uniqpid_df_Year_r):
        not_paid_yet_df = merge_maxfyrmth_uniqpid_df_Year_r\
                                    [(merge_maxfyrmth_uniqpid_df_Year_r['fin_year_r'].isnull().values)].reset_index(drop=True)
        not_paid_yet_df['Not_paid_in_4Yrs'] = 1

        ### drop of year & month column from not paid yet df
        not_paid_yet_pid_df  =  not_paid_yet_df.drop(columns = ['fin_year_r', 'fin_month_r'])

        print("Calculated to not paid in 4LY property\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return not_paid_yet_pid_df

    def not_paid_yet_in7ly_since2015_2020(self,unique_pid_df,merge_maxfyrmth_uniqpid_df_Year_r,property_financial_yrmth_df):
        pid_fin_yrmth_df =  property_financial_yrmth_df.copy()

        pid_fin_yrmth_df['billdate'] = pd.to_datetime(pid_fin_yrmth_df['billdate'])
        pid_fin_yrmth_df['billyear'] = pid_fin_yrmth_df['billdate'].dt.year

        def not_paid_in8ly_since2015(pid_fin_yrmth_df):
            property_financial_yrmth_df_2015 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2015]

            property_fyrmth_r_max = property_financial_yrmth_df_2015.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2015Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in8ly_since2015 = merge_maxfyrmth_uniqpid_df_2015Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2015Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in8ly_since2015['Not_paid_in_8Yrs'] = 1
            ### drop of year & month column from not paid yet df
            not_paid_yet_in8ly_since2015  =  not_paid_yet_in8ly_since2015.drop(columns = ['fin_year_r', 'fin_month_r'])

            return not_paid_yet_in8ly_since2015
        ##----------------------------------------------------------------------------------------------------------
        def not_paid_in7ly_since2016(pid_fin_yrmth_df):
            property_financial_yrmth_df_2016 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2016]

            property_fyrmth_r_max = property_financial_yrmth_df_2016.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2016Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in7ly_since2016 = merge_maxfyrmth_uniqpid_df_2016Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2016Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in7ly_since2016['Not_paid_in_7Yrs'] = 1

            return not_paid_yet_in7ly_since2016
        ##----------------------------------------------------------------------------------------------------------

        def not_paid_in6ly_since2017(pid_fin_yrmth_df):
            property_financial_yrmth_df_2017 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2017]

            property_fyrmth_r_max = property_financial_yrmth_df_2017.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2017Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in6ly_since2017 = merge_maxfyrmth_uniqpid_df_2017Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2017Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in6ly_since2017['Not_paid_in_6Yrs'] = 1

            return not_paid_yet_in6ly_since2017
        ##----------------------------------------------------------------------------------------------------------

        def not_paid_in5ly_since2018(pid_fin_yrmth_df):
            property_financial_yrmth_df_2018 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2018]
            property_fyrmth_r_max = property_financial_yrmth_df_2018.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2018Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in5ly_since2018 = merge_maxfyrmth_uniqpid_df_2018Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2018Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in5ly_since2018['Not_paid_in_5Yrs'] = 1

            return not_paid_yet_in5ly_since2018
        ##----------------------------------------------------------------------------------------------------------

        def not_paid_in4ly_since2019(pid_fin_yrmth_df):
            property_financial_yrmth_df_2019 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2019]
            property_fyrmth_r_max = property_financial_yrmth_df_2019.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2019Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in4ly_since2019 = merge_maxfyrmth_uniqpid_df_2019Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2019Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in4ly_since2019['Not_paid_in_4Yrs'] = 1

            return not_paid_yet_in4ly_since2019
        ##------------------------------------------------------------------------------------------------------------

        def not_paid_in3ly_since2020(pid_fin_yrmth_df):
            property_financial_yrmth_df_2020 = pid_fin_yrmth_df[
                pid_fin_yrmth_df['billyear'] >= 2020]
            property_fyrmth_r_max = property_financial_yrmth_df_2020.groupby(['propertykey'])[
                'fin_year_r', 'fin_month_r'].max().reset_index()

            # Apply merge using unique property id
            merge_maxfyrmth_uniqpid_df_2020Yr_r = property_fyrmth_r_max.merge(unique_pid_df, on='propertykey', how='inner')

            not_paid_yet_in3ly_since2020 = merge_maxfyrmth_uniqpid_df_2020Yr_r\
                                        [(merge_maxfyrmth_uniqpid_df_2020Yr_r['fin_year_r'].isnull().values)].reset_index(drop=True)
            not_paid_yet_in3ly_since2020['Not_paid_in_3Yrs'] = 1

            return not_paid_yet_in3ly_since2020
        ##--------------------------------------------------------------------------------------------------------------

        not_paid_yet_in8ly_since2015 = not_paid_in8ly_since2015(pid_fin_yrmth_df)
        not_paid_yet_in7ly_since2016 = not_paid_in7ly_since2016(pid_fin_yrmth_df)
        not_paid_yet_in6ly_since2017 = not_paid_in6ly_since2017(pid_fin_yrmth_df)
        not_paid_yet_in5ly_since2018 = not_paid_in5ly_since2018(pid_fin_yrmth_df)
        not_paid_yet_in4ly_since2019 = not_paid_in4ly_since2019(pid_fin_yrmth_df)
        not_paid_yet_in3ly_since2020 = not_paid_in3ly_since2020(pid_fin_yrmth_df)

        ### drop of year & month column from not paid yet df
        # not_paid_yet_in7ly_since2015_df  =  not_paid_yet_in7ly_since2015.drop(columns = ['fin_year_r', 'fin_month_r'])

        print("Calculated to not paid in 7LY Since From 2015 to 2020 defaulters properties\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return not_paid_yet_in8ly_since2015, not_paid_yet_in7ly_since2016, \
            not_paid_yet_in6ly_since2017, not_paid_yet_in5ly_since2018, not_paid_yet_in4ly_since2019,\
            not_paid_yet_in3ly_since2020


    def partially_paid_df(self,filter_propertybill_df):
        # Find the partially paid main dataframe PID
        filter_data_greaterthan_2022 = filter_propertybill_df[filter_propertybill_df['fin_year_r'] >= 2022]

        non_zero_balanceamt = filter_data_greaterthan_2022\
                                                [filter_data_greaterthan_2022['balanceamount'] != 0]

        grpby_sum_nonbillamt = non_zero_balanceamt.groupby(['propertykey', 'billdate'])\
                                                                    ['paidamount', 'totalbillamount'].sum().reset_index()
        # non_zero_balanceamt = non_zero_balanceamt.fillna(0)

        grpby_sum_nonbillamt['percentage'] = round(
            (grpby_sum_nonbillamt['paidamount'] / grpby_sum_nonbillamt['totalbillamount']) * 100)

        property_partiallypaid_ty = grpby_sum_nonbillamt.copy()
        ## 0 - Fully Paid
        ## 1 - Paritially Paid
        property_partiallypaid_ty['partially_paid'] = np.where(property_partiallypaid_ty['percentage']>90,0,1)

        # grp_by_sum_billamt.to_csv(outpath + "partially_paid_pid_TY_List.csv", index=False)
        print("Calculated to partially paid property\n---------------------------------------------------------------"
              "----------------------------------------------------------------------")
        return property_partiallypaid_ty