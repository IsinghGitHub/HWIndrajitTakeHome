"""
Provider Network Comparison - provider count and network performance script.
"""

import os
import numpy as np
import pandas as pd
import sys
import pyodbc
import warnings
import datetime
import requests
import smtplib
import logging
from requests.structures import CaseInsensitiveDict
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pandas import DataFrame
from datetime import datetime
import polars as pl
import datetime
import pyarrow
import pyarrow.parquet as pq
import pyarrow as pa
from joblib import Parallel, delayed
import pandas as pd
import json
import time

# fetching process id alloted by server/machine to thin running python process
PID = os.getpid() 

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)

# Reading Landscape Data.
file_path = "./sample_data/"
additionalFilesPath = "./sample_data/"


# Defining Extraction Process Function.

def send_email(email_recipient,
			   email_subject,
			   email_message):

	email_sender = 'sender@example.com'  # change here

	msg = MIMEMultipart()
	msg['From'] = email_sender
	msg['To'] = email_recipient
	msg['Subject'] = email_subject

	msg.attach(MIMEText(email_message, 'plain'))

	try:
		server = smtplib.SMTP('smtp.office365.com', 587)
		server.ehlo()
		server.starttls()
		server.login('sender@example.com', '<EMAIL_PASSWORD>')  # change here
		text = msg.as_string()
		server.sendmail(email_sender, email_recipient, text)
		#print('email sent')   
		server.quit()
	except:
		print("SMPT server connection error")
	return True

# function to emit progress report
def emit_progress(percent, message, extra=None):
    payload = {
        "type": "progress",
        "percent": percent,
        "message": message
    }
    if extra is not None:
        payload["data"] = extra

    print(json.dumps(payload))
    sys.stdout.flush()
    
# function to print log statements
def emit_log(message):
    print(json.dumps({
        "type": "log",
        "message": message
    }))
    sys.stdout.flush()
    
# function to send error
def emit_error(message):
    print(json.dumps({
        "type": "error",
        "message": message
    }))
    sys.stdout.flush()



# file_path=r""
# filespath=r""

# net="208546"#"201580"
# county=[42101]

# file_path=r""
# requestId= 10
# filespath=r""
# clientId= 45
# planType="HMO/HMO-POS"
# planTypeId= "2"
# baseParentOrg="Org A"
# compareParentOrg="Org B"
# baseParentOrgId="1"
# compareParentOrgId="2"
# county=[6037]
# id_parameter="Check_1"

# base= {
#         "1": {
#             "parentOrganization": "Org A",
#             "planBidIds": "H0000_001_0",
#             "networkIds": "100001"
#         }
#     }



# compare= {"2": {
#     "parentOrganization": "Org B",
#     "planBidIds": "H0000_002_0",
#     "networkIds": "100002"}
        
#     }



def read_and_drop_duplicates(file_path, filespath, net,county):
    status = 'ACTIVE'
    selected_columns = [
            'state', 'FIPS State County Code', 'New Flag',
            'npi',  'Address First Line', 'specialty_code', 'Status'
        ]

    ind_flag_list=['PCP','Physician Specialists','Other Providers']
    loc_conf=['Low', 'Medium', 'High', 'LOCATION CONFIDENCE NOT AVAILABLE']
    ind_file = f"{file_path}{filespath}/network_files/Individual/{net}"
    org_file = f"{file_path}{filespath}/network_files/Organization/{net}.csv"
    ind_columns = ['state', 'New Flag', 'npi', 'Specialty Description', 'location_confidence','FIPS State County Code']
    spec_code = [25270,25277,25271,25281,25282,25272,25273,25274,25283,25269,25275,25278,25276,25280,25279,26285,26284,26288,26292,26286,26287,26290,26293,26289,26291,26294,19066,27295,27296,27297,27298,28309,28304,28300,28308,28302,29311,29312,29313,29310,30314,30315,30317,30316,31320,31318,31327,31321,31325,31319,31326,31322,31324,18064,18065,32330,32331,32333,32332,32328,32329,32334,38382,39383,33335,33337,33341,33347,33343,33344,33345,33346,33349,33338,33351,33350,33340,33336,33339,33352,33342,33348,33353,34360,34354,34361,34355,34358,34362,34357,34359,34363,34364,34356,10006,10005,10002,10001,10011,10004,10003,12018,12020,12016,12021,12014,12017,12013,12015,12019,14029,14028,14027,14026,15031,31323,16033,16035,16034,16032,37372,37371,37373,37375,37378,37380,37381,37379,37377,37376,37374,17041,17056,17047,17062,17037,17040,17053,17058,17055,17059,17048,17063,17051,17045,17036,17061,17038,17046,17057,17060,40384,17049,17050,17054,36370,36367,36368,22249,22225,22231,22238,22226,22246,22239,22228,22241,22233,22227,22229,22230,22242,22240,22245,22243,22236,22247,22248,22237,23254,23251,24268,24255,24266,24256,24261,24263,24257,24262,24264,24258,24260,24265,24267,24259,17043,17044,17042]
    spec_code = [str(s)+".parquet" for s in spec_code]
    file_list = os.listdir(ind_file)
    file_list = [f for f in file_list if f not in spec_code]
    df2 = [pd.read_parquet(ind_file+"/"+j, columns=ind_columns, filters=[('FIPS State County Code','in',county),('location_confidence', 'in', loc_conf)]) for j in file_list]
    #df2 = [pd.read_parquet(ind_file+"/"+j, columns=ind_columns) for j in file_list]

    ind_df_ind = pd.concat(df2,ignore_index=True).drop_duplicates()
    ind_df_ind=ind_df_ind[ind_df_ind['New Flag'].isin(ind_flag_list)]
    
    ind_df_org = pl.scan_csv(org_file)
    #ind_df_org=ind_df_org.collect().to_pandas().drop_duplicates()

    if ind_df_org.collect().height==0:
        ind_df_org=pd.DataFrame(columns=selected_columns)
    else:
        ind_df_org = (
                ind_df_org
                .select(selected_columns)
                .filter((pl.col("Status").str.to_uppercase() == status.upper()) & 
                        (pl.col('FIPS State County Code').is_in(county)) & (pl.col('New Flag') != 'None'))
                .collect()
                .to_pandas()
            ).drop_duplicates()
    return ind_df_ind, ind_df_org


def load_base_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,county):
    net1_list=list(set([int(n.strip()) for n in base[baseParentOrgId]['networkIds'].split(",")]))
    #base_county=list(set([int(n.strip()) for n in base[baseParentOrgId]['commonCounty'].split(",")]))
    network_id=str(baseParentOrgId)+"_"+str(planTypeId)
    network_name=baseParentOrg+" "+planType
    # emit_log('above read drop dupli')
    net_df = [read_and_drop_duplicates(file_path, filespath, net, county) for net in net1_list]
    # emit_log('above read drop dupli')
    ind_dfs = [res[0] for res in net_df]
    org_dfs = [res[1] for res in net_df]
    base_ind_df = pd.concat(ind_dfs, ignore_index=True).drop_duplicates()
    base_org_df = pd.concat(org_dfs, ignore_index=True).drop_duplicates()
 
    base_ind_df['network_id']=network_id
    base_ind_df['network_name']=network_name   
    base_org_df['network_id']=network_id
    base_org_df['network_name']=network_name

    if base_ind_df.empty:
        base_ind_df = pd.DataFrame(columns=['state','New Flag','npi','Specialty Description',
                                   'location_confidence','FIPS State County Code','network_id','network_name'])
    if base_org_df.empty:
        base_org_df = pd.DataFrame(columns=['state','FIPS State County Code','New Flag','npi',
                                       'Address First Line','specialty_code','Status','network_id','network_name'])    


    return base_ind_df,base_org_df,net1_list,network_id,network_name



def load_compare_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,compare_net,county):
    
    compareParentOrg1=compare[compare_net]['parentOrganization']
    # compareId="152"
    net2_list=list(set([int(n) for n in compare[compare_net]['networkIds'].split(",")]))
    #compare_county=list(set([int(n) for n in compare[compareId]['commonCounty'].split(",")]))
    network_id=str(compare_net)+"_"+str(planTypeId)
    network_name=compareParentOrg1+" "+planType
    net_df = [read_and_drop_duplicates(file_path, filespath, net, county) for net in net2_list]
    ind_dfs = [res[0] for res in net_df]
    org_dfs = [res[1] for res in net_df]
    compare_ind_df = pd.concat(ind_dfs, ignore_index=True).drop_duplicates()
    compare_org_df = pd.concat(org_dfs, ignore_index=True).drop_duplicates()
 
    compare_ind_df['network_id']=network_id
    compare_ind_df['network_name']=network_name   
    compare_org_df['network_id']=network_id
    compare_org_df['network_name']=network_name

    if compare_ind_df.empty:
        base_ind_df = pd.DataFrame(columns=['state','New Flag','npi','Specialty Description',
                                   'location_confidence','FIPS State County Code','network_id','network_name'])
    if compare_org_df.empty:
        compare_org_df = pd.DataFrame(columns=['state','FIPS State County Code','New Flag','npi',
                                       'Address First Line','specialty_code','Status','network_id','network_name'])    

    return compare_ind_df,compare_org_df,net2_list,network_id,network_name



def categorize(val):
    try:
        val = float(val)
        if val >= 4:
            return 'High'
        elif 2<val<4:
            return 'Medium'
        elif val <= 2:
            return 'Low'
        else:
            return 'Not Available'
    except:
         return 'Not Available'
    
    
   
def categorize_utilization_score(val):
    try:
        val = float(val)
        if val >= 80:
            return 'High'
        elif val <=40:
            return 'Low'
        elif 40<val<80:
            return 'Medium'
        else:
            return 'Not Available'
    except:
        return 'Not Available' 



def count_data(filespath,base,compare,baseParentOrgId,compareParentOrgId,planTypeId,baseParentOrg,compareParentOrg,planType,id_parameter,county,clientId,userId):
    try:
        ##########################################################################################
        # DB stubbed out — results are written to ./sample_data/output/ instead
        output_dir = "./sample_data/output"
        os.makedirs(output_dir, exist_ok=True)
        ##########################################################################################
        t1 = time.time()
        spec_df=pd.read_csv(f"{additionalFilesPath}{filespath}/HSD_specialty.csv")

        spec_df['Specialty Description']=spec_df['Specialty Description'].str.strip()
        spec_df_list=spec_df['Specialty Description'].tolist()

        ###Creting Empty table to store results
        hosp_nan=[208546]
        compareorgid_list=[(i.strip()+"_"+planTypeId) for i in compareParentOrgId.split(",")]
        baseorg=baseParentOrg+" "+planType
        compareorg=[(i.strip()+" "+planType) for i in compareParentOrg.split("|")]
        base_compareorgid=baseParentOrgId+"_"+planTypeId
        filt_bid=pd.DataFrame({"network_id":[base_compareorgid]+compareorgid_list,
                               "network_name":[baseorg]+compareorg})
        

        # Duplicate each row and assign "common" & "unique"
        df=filt_bid.copy()
        mergeInBlank = df.loc[df.index.repeat(2)].reset_index(drop=True)
        mergeInBlank["Unique/Common"] = ["common", "unique"] * (len(df) // 1)
		
        addProviderType_Ind = pd.DataFrame({
            "network_id": filt_bid['network_id'].unique().tolist() * 3,
            "Provider Type": (["PCP"] * filt_bid['network_id'].nunique()) + (["Physician Specialist"] * filt_bid['network_id'].nunique())
             + (["Other Providers"] * filt_bid['network_id'].nunique())
        })
        
        addProviderType_Ind_spec = pd.DataFrame({
            "network_id": filt_bid['network_id'].unique().tolist() * len(spec_df_list),
            "Provider Type": sum([[ptype] * len(filt_bid['network_id'].unique().tolist()) for ptype in spec_df_list], [])
            # + (["Other Provider"] * filt_bid['network_id'].nunique())
        })
        
        addProviderType_org = pd.DataFrame({
            "network_id": filt_bid['network_id'].unique().tolist() * 1,
            "Provider Type": (["Hospital"] * filt_bid['network_id'].nunique())
        })
        renameCol = {'network_id':'Network ID'}
        
        mergeInBlank_ind = mergeInBlank.merge(addProviderType_Ind, on=['network_id'], how='left')
        mergeInBlank_ind['NPI'] = ' '
        mergeInBlank_ind.rename(columns=renameCol, inplace=True)
        
        mergeInBlank_ind_spec = mergeInBlank.merge(addProviderType_Ind_spec, on=['network_id'], how='left')
        mergeInBlank_ind_spec['NPI'] = ' '
        mergeInBlank_ind_spec.rename(columns=renameCol, inplace=True)
        
        mergeInBlank_org = mergeInBlank.merge(addProviderType_org, on=['network_id'], how='left')
        mergeInBlank_org['NPI'] = ' '
        mergeInBlank_org.rename(columns=renameCol, inplace=True)        
        del df, mergeInBlank, addProviderType_Ind, addProviderType_org, renameCol,addProviderType_Ind_spec

        # t1= datetime.datetime.now()
        #### -------- PNC code starts now ---------  ###
        finaltable=pd.DataFrame()
        finaltable_spec=pd.DataFrame()
        cached_df_ind=[]
        cached_df_org=[]
        index=1
        t2 = time.time()
        initial_process_time = t2-t1
        #emit_log(f"Initial process time: {initial_process_time:.2f} seconds")
        # emit_progress(5, "HSD table read completed")
        
        base_ind_df,base_org_df,net1_list_nan,network_id1_nan,network_name1_nan=load_base_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,county)
        #emit_log('hi below load base file')
        
        cached_df_ind.append(base_ind_df)
        cached_df_org.append(base_org_df)
        t3 = time.time()
        t_baseFileRead = t3-t2
        #emit_log(f"baseFile_read_time: {t_baseFileRead:.2f} seconds")
        #emit_progress(20, "Base files loaded")
        
        # to implement progress for compareOrgid_list
        length = len(compareorgid_list)
        progress_step = 60 // length if length > 0 else 0
        current_progress = 20
        
        for compareId in compareorgid_list:
            compare_net=compareId.split("_")[0]
            compare_ind_df,compare_org_df,net2_list_nan,network_id2_nan,network_name2_nan=load_compare_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,compare_net,county)
            cached_df_ind.append(compare_ind_df)
            cached_df_org.append(compare_org_df)
            t4 = time.time()
            t_compare_read = t4-t3
            #emit_log(f"compare_file_read_time: {t_compare_read:.2f} seconds")
            final_data_ind=pd.concat([base_ind_df,compare_ind_df],ignore_index=True)
            final_data_org=pd.concat([base_org_df,compare_org_df],ignore_index=True)
            spec_df_list_upper=[spec.upper() for spec in spec_df_list]

            final_data_ind_spec=final_data_ind[final_data_ind['Specialty Description'].str.upper().isin(spec_df_list_upper)]
            del compare_ind_df,compare_org_df
            
            final_data_org['Address First Line'] = final_data_org['Address First Line'].str.title()
            final_data_org['npi'] = final_data_org['npi'].astype('Int64')
            
          
            final_data_org['npiXaddress'] = (
                    final_data_org['npi'].astype(str) +
                    final_data_org['Address First Line'] +
                    final_data_org['specialty_code'].astype(str)
                  )
            
            
            final_data_ind['Unique/Common'] = np.where(
      			final_data_ind.groupby(['npi', 'state', 'FIPS State County Code', 'New Flag'])['network_id'].transform('nunique').eq(1),
      			"unique", "common"
      		)
      
            final_data_org['Unique/Common'] = np.where(
      			final_data_org.groupby(['npi', 'state', 'FIPS State County Code', 'Address First Line', 'New Flag', 'specialty_code'])['network_id'].transform('nunique').eq(1),
      			"unique", "common"
      		)
            
            final_data_ind_spec['Unique/Common'] = np.where(
      			final_data_ind_spec.groupby(['npi', 'state', 'FIPS State County Code', 'New Flag'])['network_id'].transform('nunique').eq(1),
      			"unique", "common"
      		)
            
            if len(final_data_ind) > 0:
                group_df_0_ind_count =(
      			final_data_ind
      			.groupby(['network_id','network_name', 'New Flag', 'Unique/Common', 'state', 'FIPS State County Code'], as_index=False)
      			.agg({'npi': 'nunique'})
      			.rename(columns={'New Flag': 'Provider Type', 'npi': 'NPI', 'network_id': 'Network ID'})
      		)
            else:
                group_df_0_ind_count = pd.DataFrame(data={'Network ID':[base_compareorgid,compareId]})
                group_df_0_ind_count = group_df_0_ind_count.merge(mergeInBlank_ind, on=['Network ID'], how='left')
            
            if len(final_data_ind_spec)> 0:
                group_specDF = (
 				final_data_ind_spec
 				.groupby(['network_id','network_name','Specialty Description','Unique/Common', 'state', 'FIPS State County Code'], as_index=False)
 				.agg({'npi': 'nunique'})
 				.rename(columns={'npi': 'NPI', 'Specialty Description': 'Provider Type','network_id': 'Network ID'})
 			)
            else:
                group_specDF= pd.DataFrame(data={'Network ID':[base_compareorgid,compareId]})
                group_specDF=group_specDF.merge(mergeInBlank_ind_spec, on=['Network ID'], how='left')
                
            if len(final_data_org) > 0:        
                group_df_0_org = (
						final_data_org
						.groupby(['network_id','network_name','New Flag', 'Unique/Common', 'state', 'FIPS State County Code'], as_index=False)
						.agg({'npiXaddress': 'nunique'})
						.rename(columns={'npiXaddress': 'NPI', 'New Flag': 'Provider Type', 'network_id': 'Network ID'})
					)
            else:
                group_df_0_org = pd.DataFrame(data={'Network ID':[base_compareorgid,compareId]})
                group_df_0_org = group_df_0_org.merge(mergeInBlank_org, on=['Network ID'], how='left')

    
            group_df = pd.concat([group_df_0_ind_count, group_df_0_org])
            group_df = (
					group_df
					.groupby(['Network ID','network_name', 'Provider Type', 'Unique/Common'], as_index=False)
					.agg({'NPI': 'sum'})
					.rename(columns={'NPI': 'npi_count', 'Provider Type': 'flag', 'Network ID': 'network_id', 'Unique/Common': 'common_unique'})
				)
            
            if net1_list_nan==hosp_nan:
                data = {'network_name': [network_name1_nan], 'network_id': [network_id1_nan], 'flag' : ["Hospital"], 'npi_count': [' ']}
                df = pd.DataFrame(data)
                group_df=pd.concat([group_df,df],ignore_index=True)
            
            if net2_list_nan==hosp_nan:
                data = {'network_name': [network_name2_nan], 'network_id': [network_id2_nan], 'flag' : ["Hospital"], 'npi_count': [' ']}
                df = pd.DataFrame(data)
                group_df=pd.concat([group_df,df],ignore_index=True)
                
        
        
            group_specDF = (
					group_specDF
					.groupby(['Network ID','network_name', 'Provider Type', 'Unique/Common'], as_index=False)
					.agg({'NPI': 'sum'})
					.rename(columns={'NPI': 'npi_count', 'Provider Type': 'flag', 'Network ID': 'network_id', 'Unique/Common': 'common_unique'})
				)
            
            
        
            group_df['base_compare'] = 'Compare_' + str(index)
            group_specDF['base_compare'] = 'Compare_' + str(index)

            finaltable=pd.concat([finaltable,group_df])
            finaltable_spec=pd.concat([finaltable_spec,group_specDF])

            del final_data_org,final_data_ind,group_df_0_ind_count,group_df_0_org,final_data_ind_spec
            # t2=datetime.datetime.now()
            index+=1
            t5 = time.time()
            t_compare_processing_time = t5-t4
            #emit_log(f"compare pocessing time: {t_compare_processing_time:.2f} seconds")
            current_progress += progress_step
            #emit_progress(current_progress, "n compare file loded")
            
        #emit_progress(82, "Compare files loaded completely")

        #########-------Formatting count data--------############
        t6 = time.time()
        #emit_log(f"read and process count excluding base time: {t6-t3:.2f} seconds")
        #########--------Creating table for group_df-------------###########
        finaltable['network_id'] = finaltable['network_id'].astype(str)
        finaltable['base_compare'] = np.where(finaltable['network_id'] == base_compareorgid,"base_" + finaltable['base_compare'],finaltable['base_compare'])
        finaltable['req_id'] = 	id_parameter
        finaltable_spec['network_id'] = finaltable_spec['network_id'].astype(str)
        finaltable_spec['base_compare'] = np.where(finaltable_spec['network_id'] == base_compareorgid,"base_" + finaltable_spec['base_compare'],finaltable_spec['base_compare'])
        finaltable_spec['req_id'] = id_parameter
        t7 = time.time()
        #emit_log(f"table_creation_time: {t7-t6:.2f} seconds")
        
        ###########-------Network performance Indicator---------######
        
        
        final_data_ind=pd.concat(cached_df_ind,ignore_index=True)
        final_data_org=pd.concat(cached_df_org,ignore_index=True)
        

        
        npiScore_ind = (pl.scan_csv(f"{additionalFilesPath}{filespath}/Score for Individual - PNC Medicare.csv",infer_schema_length=10_000,ignore_errors=True)

        .select([
        pl.col("NPI").alias("npi"),
        "New Flag",
        pl.col("County FIPS").alias("FIPS State County Code"),
        "Cost Score",
        "Quality Score",
        pl.col("Specialty").alias("Specialty Description"),
        "MA Utilization Score",
        "FFS Utilization Score",'Quality color indicator'
    ])
    .filter(pl.col("FIPS State County Code").is_in(county))
    .unique()
    .collect()
    .to_pandas()
)
        #emit_progress(85, "Individual Score file loaded")
        
        npiScore_Org = (pl.scan_csv(f"{additionalFilesPath}{filespath}/Scores for Organization - PNC Medicare.csv",infer_schema_length=10_000,ignore_errors=True)

                  .select([
        "npi",
        "Address First Line",
        "state",
        "FIPS State County Code",
        "Quality Score",
        "Cost Score",
        "FFS Utilization Score",
        "MA Utilization Score"
    ])
    .filter(pl.col("FIPS State County Code").is_in(county))
    .unique()
    .collect()
    .to_pandas()
)
        #emit_progress(87, "Org score file loaded")
        
        
        final_NetPerIndi_Indi = pd.DataFrame()
        final_NetPerIndi_Org = pd.DataFrame()
        
        final_data_ind_Hosp = final_data_org[final_data_org['New Flag'] == 'Hospital']
        	
        columns_to_convert = ['npi', 'FIPS State County Code']
        t8 = time.time()
        #emit_log(f"scores file readTime: {t8-t7:.2f} seconds")
        
        final_data_ind[columns_to_convert] = final_data_ind[columns_to_convert].astype('int64')
        #final_data_ind_Spe[columns_to_convert] = final_data_ind_Spe[columns_to_convert].astype('int64')
        final_data_ind_Hosp[columns_to_convert] = final_data_ind_Hosp[columns_to_convert].astype('int64')
        	
        npiScore_ind[columns_to_convert] = npiScore_ind[columns_to_convert].astype('int64')
        #npiScore_ind_Spe[columns_to_convert] = npiScore_ind_Spe[columns_to_convert].astype('int64')
        npiScore_Org[columns_to_convert] = npiScore_Org[columns_to_convert].astype('int64')
        npiScore_Org['Address First Line']=npiScore_Org['Address First Line'].str.strip().str.upper()

		# Merge dataframes - PCP
        npiScore_ind["merge"] = np.where(npiScore_ind['New Flag'].isin(["PCP"]),"PCP",npiScore_ind['Specialty Description'])
        npiScore_ind.drop(columns=['Specialty Description'],inplace=True)
        final_data_ind['merge']=np.where(final_data_ind['New Flag'].isin(["PCP"]),"PCP",final_data_ind['Specialty Description'])
        final_data_ind_Score = final_data_ind.merge(npiScore_ind, on=['npi','New Flag','FIPS State County Code','merge'], how='left')
        final_data_ind_Score[['Quality Score', 'Cost Score',  'FFS Utilization Score','MA Utilization Score','Quality color indicator']] = final_data_ind_Score[['Quality Score', 'Cost Score',  'FFS Utilization Score','MA Utilization Score','Quality color indicator']].fillna(value='Not Available')
        final_data_ind_Score.drop(columns=['merge'],inplace=True)
        final_data_ind_Score.rename(columns={"MA Utilization Score":"Utilization Score"},inplace=True)
        
        # ##--Spe
        # final_data_ind_addedSpe_NPIScore = final_data_ind_Spe.merge(npiScore_ind_Spe, on=['npi', 'state', 'Specialty Description', 'FIPS State County Code', 'New Flag'], how='left')
        # final_data_ind_addedSpe_NPIScore[['Quality Score', 'Cost Score', 'Utilization Score']] = final_data_ind_addedSpe_NPIScore[['Quality Score', 'Cost Score', 'Utilization Score']].fillna(value='Not Available')
        	
        ##--Hosp
        final_data_ind_Hosp['Address First Line']=final_data_ind_Hosp['Address First Line'].str.strip().str.upper()
        final_data_org_addedHosp_NPIScore = final_data_ind_Hosp.merge(npiScore_Org, on=['npi','Address First Line', 'state', 'FIPS State County Code'], how='left')
        final_data_org_addedHosp_NPIScore[['Quality Score', 'Cost Score', 'FFS Utilization Score','MA Utilization Score']] = final_data_org_addedHosp_NPIScore[['Quality Score', 'Cost Score', 'FFS Utilization Score','MA Utilization Score']].fillna(value='Not Available')
        final_data_org_addedHosp_NPIScore.rename(columns={"MA Utilization Score":"Utilization Score"},inplace=True)
            
		# Define your group columns (adjust as needed)
		# Categorize each score column independently
        def get_score_category_counts(df, group_cols):
            result_rows = []
            for cat in ['High', 'Medium', 'Low']:
                temp = df.groupby(group_cols).apply(
					lambda x: pd.Series({
						    'Quality Score': (x['Quality Score Category'] == cat).sum(),
						    'Cost Score': (x['Cost Score Category'] == cat).sum(),
						    'Utilization Score': (x['Utilization Score Category'] == cat).sum(),
						})
			        ).reset_index()
                temp['Scoring Category'] = cat
                result_rows.append(temp)
            final_output = pd.concat(result_rows, ignore_index=True)
            return final_output[group_cols + ['Quality Score', 'Cost Score', 'Utilization Score', 'Scoring Category']]
        
        
        for col in ['Quality Score', 'Cost Score']:
            final_data_ind_Score[f'{col} Category'] = final_data_ind_Score[col].apply(categorize)
            #final_data_ind_addedSpe_NPIScore[f'{col} Category'] = final_data_ind_addedSpe_NPIScore[col].apply(categorize)
            final_data_org_addedHosp_NPIScore[f'{col} Category'] = final_data_org_addedHosp_NPIScore[col].apply(categorize)

        for col in ['Utilization Score']:
            final_data_ind_Score[f'{col} Category'] = final_data_ind_Score[col].apply(categorize_utilization_score)
            #final_data_ind_addedSpe_NPIScore[f'{col} Category'] = final_data_ind_addedSpe_NPIScore[col].apply(categorize)
            final_data_org_addedHosp_NPIScore[f'{col} Category'] = final_data_org_addedHosp_NPIScore[col].apply(categorize_utilization_score)
    

        group_cols = ['New Flag', 'network_name', 'network_id']
        empty_net_per_col=['New Flag', 'network_name', 'network_id', 'Quality Score', 'Cost Score','Utilization Score', 'Scoring Category']
        
        if not final_data_ind_Score.empty:
            final_data_ind_Score_table=get_score_category_counts(final_data_ind_Score, group_cols)
        else:
            final_data_ind_Score_table=pd.DataFrame(columns=empty_net_per_col)
            
        if not final_data_org_addedHosp_NPIScore.empty:
            final_data_org_addedHosp_NPIScore_table=get_score_category_counts(final_data_org_addedHosp_NPIScore, group_cols)
        else:
            final_data_org_addedHosp_NPIScore_table=pd.DataFrame(columns=empty_net_per_col)
        	
        final_NetPerIndi_Indi = pd.concat([final_NetPerIndi_Indi, final_data_ind_Score_table])
        final_NetPerIndi_Org = pd.concat([final_NetPerIndi_Org, final_data_org_addedHosp_NPIScore_table])

        
        del  final_data_ind_Score_table,final_data_org_addedHosp_NPIScore_table,final_data_ind,final_data_org

        #networkPerformanceIndi['network_id'] = networkPerformanceIndi['network_id'].astype('Int64')
        t9 = time.time()
        #emit_log(f"network performance excluding read: {t9-t8:.2f} seconds")
        
        ####------Network Performnce Indicator Market----------##########
        ind_market_df=pd.read_csv(f"{additionalFilesPath}{filespath}/network_performance_countywise.csv")
        org_market_df=pd.read_csv(f"{additionalFilesPath}{filespath}/network_performance_countywise_hospital.csv")
        ind_market_df=ind_market_df[ind_market_df['FIPS State County Code'].isin(county)]
        org_market_df.rename(columns={"FIPS County Code":"FIPS State County Code"},inplace=True)
        
        org_market_df=org_market_df[org_market_df['FIPS State County Code'].isin(county)]
        ind_market_df=ind_market_df[ind_market_df["Scoring Category"]!="Not Available"]
        ind_market_df.rename(columns={"MA Utilization Score":"Utilization Score"},inplace=True)
        org_market_df=org_market_df[org_market_df["Scoring Category"]!="Not Available"]
        ind_market_df1=pd.concat([ind_market_df,org_market_df],ignore_index=True)
        ind_market_df1=ind_market_df1.drop(columns=['state', 'county_name',
        	   'FIPS State County Code','FFS Utilization Score'])
        net_per_mar_ind = ind_market_df1.groupby(["Scoring Category", "New Flag"], as_index=False).agg({
    "Utilization Score": "sum",
    "Cost Score": "sum",
    "Quality Score": "sum"})
        net_per_mar_ind['network_name']='Common Counties'
        net_per_mar_ind['network_id']=""

        networkPerformanceIndi = pd.concat([final_NetPerIndi_Indi, final_NetPerIndi_Org,net_per_mar_ind])
        networkPerformanceIndi['req_id'] = 	id_parameter
        # emit_log('Vibhore code starts')
        networkPerformanceIndi = networkPerformanceIndi[['req_id','network_id','network_name','Quality Score','Cost Score','Utilization Score','New Flag','Scoring Category']]
        
        t10 = time.time()
        #emit_progress(94, "Network Performance Indicator processng completed")
        
        
        
        ###-----------Save results to CSV (DB stubbed out)-----------###
        counts_path = f"{output_dir}/NI+_Results_Table.csv"
        bargraph_path = f"{output_dir}/NI+_Results_BarGraph.csv"
        perf_path = f"{output_dir}/NI+_Results_PerformIndicators.csv"

        finaltable[['req_id','network_id','network_name','common_unique','base_compare','flag','npi_count']].to_csv(counts_path, index=False)
        finaltable_spec[['req_id','network_id','network_name','common_unique','base_compare','flag','npi_count']].to_csv(bargraph_path, index=False)
        networkPerformanceIndi[['req_id','network_id','network_name','Quality Score','Cost Score','Utilization Score','New Flag','Scoring Category']].to_csv(perf_path, index=False)

        print(f"Results written to {output_dir}/")
        print(f"  NI+_Results_Table.csv          — {len(finaltable)} rows")
        print(f"  NI+_Results_BarGraph.csv        — {len(finaltable_spec)} rows")
        print(f"  NI+_Results_PerformIndicators.csv — {len(networkPerformanceIndi)} rows")
        return 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        error_message = f"Error: {str(e)}"
        print(error_message)
        
def main():
    if len(sys.argv) < 14:
        print(
            "Usage: python provider_count_assignment.py "
            "<countyId> <id_param> <filespath> <clientID> <userID> "
            "<base_json> <compare_json> <baseParentOrg> <compareParentOrg> "
            "<baseParentOrgId> <compareParentOrgId> <planType> <planTypeId>\n"
            f"Got {len(sys.argv) - 1} argument(s), expected 13."
        )
        sys.exit(1)
    countyId = sys.argv[1]  # '4005,4007,4025,12011,17031,17043' | common counties for all base and compare parent orgs together NOTs parentOrg wise
    id_param = sys.argv[2] # '1'
    filespath = sys.argv[3] # 'UAT'
    clientID = sys.argv[4] # '45'
    userID = sys.argv[5] # '6a68-8324-a0b4-96d27efe8708'
    
    # base as dictionary --> {"1": {"parentOrganization": "Org A","planBidIds": "H0000_001_0","networkIds": "100001"}}
    base = json.loads(sys.argv[6]) 
    # compare as dictionary
    compare = json.loads(sys.argv[7])
    baseParentOrg = sys.argv[8] # 'Org A'
    compareParentOrg = sys.argv[9] # 'Org B|Org C'
    baseParentOrgId = sys.argv[10] # '1'
    compareParentOrgId = sys.argv[11] # '2,3'
    planType = sys.argv[12]  # 'HMO/HMO-POS'
    planTypeId=sys.argv[13]  # '3'
     
    c1 = countyId.split(",")
    c2 = [i.strip(" ") for i in c1]
    c2 = [int(i) for i in c2]
    
    CountStatus = count_data(filespath=filespath,base=base,compare=compare,baseParentOrgId=baseParentOrgId,
        	        	     compareParentOrgId=compareParentOrgId,planTypeId=planTypeId,baseParentOrg=baseParentOrg,
        	        	     compareParentOrg=compareParentOrg,planType=planType,id_parameter=id_param,county=c2,clientId=clientID,userId=userID )
    
    if(CountStatus==1):
        # emit_progress(100, "Success")
        print("Success")
    else:
        # emit_progress(-1, "Failed")
        print("Failed")
        
        

if __name__ == '__main__':
		main()
