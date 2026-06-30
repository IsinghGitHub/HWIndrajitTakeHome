"""
Provider Network Comparison - downloadable provider file generation script.
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
outputfile_path = "./sample_data/output/Individual/"
outputfile_path_hosp = "./sample_data/output/Organization/"


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
# county=[5001,5003,5005,5007,5009,5015,5019,5023,5027,5029,5031,5033,5039,5045,5047,5049,5051,5053,5055,5057,5059,5061,5063,5065,5067,5069,5071,5075,5081,5085,5087,5089,5091,5093,5099,5103,5105,5109,5115,5119,5121,5125,5127,5129,5131,5133,5135,5123,5137,5139,5141,5143,5145,4005,4013,4015,4019,4021,4025,4027,6001,6017,6019,6029,6031,6037,6039,6041,6055,6057,6059,6061,6065,6067,6071,6073,6075,6077,6079,6083,6085,6095,6099,6107,6111,6113,8001,8005,8013,8014,8019,8031,8035,8041,8039,8043,8059,8069,8083,8101,8119,8123,15003,15007,15009,19009,19011,19013,19015,19017,19019,19021,19023,19029,19031,19033,19035,19043,19045,19047,19049,19055,19057,19061,19065,19067,19069,19071,19075,19079,19083,19085,19087,19093,19095,19097,19099,19101,19103,19105,19107,19111,19113,19115,19121,19123,19125,19127,19129,19133,19137,19139,19141,19145,19149,19153,19155,19157,19163,19167,19169,19179,19181,19183,19187,19193,16001,16003,16015,16027,16039,16045,16049,16059,16073,16075,16083,16085,16087,17005,17007,17013,17019,17021,17027,17031,17037,17043,17063,17073,17083,17089,17091,17093,17095,17099,17097,17105,17117,17119,17123,17111,17113,17141,17143,17161,17167,17163,17177,17179,17197,17201,17203,20011,20015,20021,20037,20041,20045,20059,20079,20087,20091,20103,20115,20113,20121,20155,20169,20173,20177,20191,20209,27003,27019,27037,27053,27109,27123,27131,27139,27163,29001,29007,29009,29011,29019,29027,29029,29031,29037,29039,29043,29047,29055,29057,29059,29067,29071,29077,29083,29085,29091,29093,29095,29097,29099,29101,29105,29107,29109,29113,29123,29127,29119,29145,29149,29153,29155,29157,29163,29165,29167,29169,29177,29201,29183,29185,29187,29189,29510,29207,29209,29213,29217,29219,29221,29225,29229,30001,30003,30009,30013,30015,30017,30023,30025,30027,30029,30031,30041,30043,30047,30049,30053,30057,30061,30063,30067,30073,30081,30087,30089,30093,30095,30097,30101,30107,30111,38017,31021,31023,31025,31037,31039,31043,31051,31053,31055,31059,31067,31095,31109,31119,31125,31127,31131,31141,31143,31151,31153,31155,31169,31177,31179,31185,35001,35028,35047,35043,35049,35057,35061,32510,32001,32003,32005,32019,32021,32023,32031,40015,40017,40021,40027,40037,40041,40047,40051,40071,40073,40079,40081,40083,40097,40087,40101,40109,40111,40113,40115,40117,40119,40123,40125,40131,40133,40135,40143,40145,40147,41005,41009,41011,41017,41027,41029,41045,41051,41067,46083,46099,46127,48001,48005,48007,48013,48019,48021,48025,48029,48037,48039,48041,48051,48053,48061,48063,48067,48071,48085,48091,48097,48113,48121,48135,48141,48139,48147,48157,48161,48167,48181,48183,48185,48187,48189,48201,48203,48209,48213,48215,48219,48245,48249,48251,48257,48259,48273,48277,48289,48291,48299,48303,48309,48325,48337,48339,48347,48349,48355,48361,48367,48387,48397,48401,48407,48409,48423,48439,48449,48453,48459,48467,48477,48489,48491,48493,48497,49003,49005,49011,49029,49035,49043,49045,49049,49053,49057,53001,53005,53007,53011,53013,53015,53017,53021,53023,53025,53029,53031,53033,53035,53037,53039,53041,53043,53045,53047,53049,53051,53053,53057,53059,53061,53063,53065,53067,53071,53073,53077,55001,55009,55015,55021,55025,55027,55029,55039,55041,55047,55049,55055,55059,55061,55067,55069,55071,55073,55075,55077,55079,55083,55085,55087,55089,55097,55101,55105,55111,55115,55117,55125,55127,55131,55133,55135,55137,55139,55141,56003,56029,56033]
# id_parameter="Check_1"

# base= {
# 		"1": {
#  			"parentOrganization": "Org A",
#  			"planBidIds": "H0000_001_0",
#  			"networkIds": "100001"
# 		}
#     }

# compare= {
# 		"2": {
#  			"parentOrganization": "Org B",
#  			"planBidIds": "H0000_002_0",
#  			"networkIds": "100002"
# 		}
#     }







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


def read_and_drop_duplicates(file_path, filespath, net,county):
    status = 'ACTIVE'
    selected_columns = [
            'npi', 'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'carrier_name', 
           			 'specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code', 'New Flag', 'city', 'state', 
           			 'zip_code', 'county_name', 'FIPS State County Code', 'Address First Line','Status'
		]
    ind_flag_list=['PCP','Physician Specialists','Other Providers']
    loc_conf=['Low', 'Medium', 'High', 'LOCATION CONFIDENCE NOT AVAILABLE']
    ind_file = f"{file_path}{filespath}/network_files/Individual/{net}"
    org_file = f"{file_path}{filespath}/network_files/Organization/{net}.csv"
    ind_columns = ['npi',  'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'Address First Line',
				   'Parent_Organization', 'location_confidence','location_confidence_address_level','specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code','Specialty Flag', 'New Flag', 'city', 'state', 'zip_code', 'county_name', 'FIPS State County Code', 
				   'Specialty Description','Is_Dual_Provider','Affiliation ID','Affiliation Presentation Name']
    spec_code = [25270,25277,25271,25281,25282,25272,25273,25274,25283,25269,25275,25278,25276,25280,25279,26285,26284,26288,26292,26286,26287,26290,26293,26289,26291,26294,19066,27295,27296,27297,27298,28309,28304,28300,28308,28302,29311,29312,29313,29310,30314,30315,30317,30316,31320,31318,31327,31321,31325,31319,31326,31322,31324,18064,18065,32330,32331,32333,32332,32328,32329,32334,38382,39383,33335,33337,33341,33347,33343,33344,33345,33346,33349,33338,33351,33350,33340,33336,33339,33352,33342,33348,33353,34360,34354,34361,34355,34358,34362,34357,34359,34363,34364,34356,10006,10005,10002,10001,10011,10004,10003,12018,12020,12016,12021,12014,12017,12013,12015,12019,14029,14028,14027,14026,15031,31323,16033,16035,16034,16032,37372,37371,37373,37375,37378,37380,37381,37379,37377,37376,37374,17041,17056,17047,17062,17037,17040,17053,17058,17055,17059,17048,17063,17051,17045,17036,17061,17038,17046,17057,17060,40384,17049,17050,17054,36370,36367,36368,22249,22225,22231,22238,22226,22246,22239,22228,22241,22233,22227,22229,22230,22242,22240,22245,22243,22236,22247,22248,22237,23254,23251,24268,24255,24266,24256,24261,24263,24257,24262,24264,24258,24260,24265,24267,24259,17043,17044,17042]
    spec_code = [str(s)+".parquet" for s in spec_code]
    file_list = os.listdir(ind_file)
    file_list = [f for f in file_list if f not in spec_code]
    df2 = [pd.read_parquet(ind_file+"/"+j, columns=ind_columns, filters=[('FIPS State County Code','in',county),('location_confidence', 'in', loc_conf)]) for j in file_list]
    #df2=[pd.read_parquet(ind_file+"/"+j) for j in file_list]
    #ind_df_ind = pd.concat(df2,ignore_index=True).drop_duplicates()
    ind_df_ind = pd.concat(df2,ignore_index=True).drop_duplicates()
    ind_df_ind.rename(columns={'Affiliation ID':'group_tin','Affiliation Presentation Name':'group_name'}, inplace=True)
    ind_df_ind=ind_df_ind[ind_df_ind['New Flag'].isin(ind_flag_list)]
    
    ind_df_org = pl.scan_csv(org_file)
    
    #ind_df_org=ind_df_org.select(selected_columns).collect().to_pandas()
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
        ind_df_org['Specialty Description'] = ind_df_org['specialty_name']
        ind_df_org['Parent_Organization'] = ind_df_org['carrier_name']
    return ind_df_ind, ind_df_org




def load_base_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,county):
    net1_list=list(set([int(n.strip()) for n in base[baseParentOrgId]['networkIds'].split(",")]))
    #base_county=list(set([int(n.strip()) for n in base[baseParentOrgId]['commonCounty'].split(",")]))
    network_id=str(baseParentOrgId)+"_"+str(planTypeId)
    network_name=baseParentOrg+" "+planType
    net_df = [read_and_drop_duplicates(file_path, filespath, net, county) for net in net1_list]
    ind_dfs = [res[0] for res in net_df]
    org_dfs = [res[1] for res in net_df]
    base_ind_df = pd.concat(ind_dfs, ignore_index=True).drop_duplicates()
    base_org_df = pd.concat(org_dfs, ignore_index=True).drop_duplicates()
 
    base_ind_df['network_id']=network_id
    base_ind_df['network_name']=network_name   
    base_org_df['network_id']=network_id
    base_org_df['network_name']=network_name

    if base_ind_df.empty:
        base_ind_df = pd.DataFrame(columns=['npi', 'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'Address First Line',
		   'Parent_Organization', 'location_confidence','location_confidence_address_level','specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code','Specialty Flag', 'New Flag', 'city', 'state', 'zip_code', 'county_name', 'FIPS State County Code', 
		   'Specialty Description','Is_Dual_Provider','group_tin','group_name'])
    if base_org_df.empty:
        base_org_df = pd.DataFrame(columns=['npi', 'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'carrier_name', 
       			 'specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code', 'New Flag', 'city', 'state', 
       			 'zip_code', 'county_name', 'FIPS State County Code', 'Address First Line','Status'])  


    return base_ind_df,base_org_df,network_id


def load_compare_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,compare_net,county):
    
    compareParentOrg=compare[compare_net]['parentOrganization']
    net2_list=list(set([int(n) for n in compare[compare_net]['networkIds'].split(",")]))
    #compare_county=list(set([int(n) for n in compare[compare_net]['commonCounty'].split(",")]))
    network_id=str(compare_net)+"_"+str(planTypeId)
    network_name=compareParentOrg+" "+planType
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
        base_ind_df = pd.DataFrame(columns=['npi', 'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'Address First Line',
		   'Parent_Organization', 'location_confidence','location_confidence_address_level','specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code', 'Specialty Flag','New Flag', 'city', 'state', 'zip_code', 'county_name', 'FIPS State County Code', 
		   'Specialty Description','Is_Dual_Provider','group_tin','group_name'])
    if compare_org_df.empty:
        base_org_df = pd.DataFrame(columns=['npi',  'type', 'presentation_name', 'first_name', 'last_name', 'network_id', 'network_name', 'carrier_name', 
       			 'specialty_category', 'specialty_name', 'specialty_subspecialty', 'specialty_code', 'New Flag', 'city', 'state', 
       			 'zip_code', 'county_name', 'FIPS State County Code', 'Address First Line','Status'])  


    return compare_ind_df,compare_org_df,network_id


# def process_npis_data(df_npis):
# 	df_npis = (
# 		df_npis
# 		.drop_duplicates()
# 		.astype({'npis': 'Int64'})
# 		.astype({'npis': str})
# 		.groupby(['npi'], as_index=False)
# 		.agg({'npis': ' '.join})
# 	)
# 	return df_npis

# def merge_and_clean(bid1_df, bid2_df, merge_cols, b1_col_name, b2_col_name, secondIter=False):
#     final_df = bid1_df.merge(bid2_df, on=merge_cols, how="outer")
# 	# Fill missing values for bid2 columns
#     final_df[b2_col_name].fillna(value='N', inplace=True)

# 	# Separate records unique to bid2 (where Bid_ID_x is missing)
#     uniqueToB2 = final_df[final_df['network_id_x'].isna()]
#     final_df = final_df[final_df['network_id_x'].notna()]

# 	# Drop '_x' columns from uniqueToB2 and '_y' columns from final_df
#     uniqueToB2 = uniqueToB2.drop(columns=[col for col in uniqueToB2.columns if col.endswith('_x')])
#     final_df = final_df.drop(columns=[col for col in final_df.columns if col.endswith('_y')])
# 	
# 	# Rename '_y' columns in uniqueToB2 and '_x' columns in final_df
#     uniqueToB2.columns = [col.replace('_y', '') for col in uniqueToB2.columns]
#     final_df.columns = [col.replace('_x', '') for col in final_df.columns]

# 	# Concatenate back the uniqueToB2 records
#     final_df = pd.concat([final_df, uniqueToB2])

# 	# Fill missing values for bid1 columns
#     final_df[b1_col_name].fillna(value='N', inplace=True)
#     final_df.drop_duplicates(inplace=True)

#     return final_df



def extract_data(filespath,base,compare,baseParentOrgId,compareParentOrgId,planTypeId,baseParentOrg,compareParentOrg,planType,id_parameter,county,clientId,userId):
	try:
		#emit_log('inside try block')
		t11= datetime.datetime.now()
		compareorgid_list=[(i.strip()+"_"+planTypeId) for i in compareParentOrgId.split(",")]
		baseorg=baseParentOrg+" "+planType
		compareorg=[(i.strip()+" "+planType) for i in compareParentOrg.split("|")]
		base_compareorgid=baseParentOrgId+"_"+planTypeId
		filt_bid=pd.DataFrame({"network_id":[base_compareorgid]+compareorgid_list,
				               "network_name":[baseorg]+compareorg})
		
		final_data_combined = pd.DataFrame()
# 		indFinal_combined = pd.DataFrame()
# 		orgFinal_combined = pd.DataFrame()

		base_ind_df,base_org_df,base_id=load_base_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,county)
		base_org_df['Address First Line'] = base_org_df['Address First Line'].str.title()

		#emit_progress(10, "Base file read completed")
        
		npiScore_ind = (pl.scan_csv(f"{additionalFilesPath}{filespath}/Score for Individual - PNC Medicare.csv",infer_schema_length=10_000,ignore_errors=True)

        .select([
        pl.col("NPI").alias("npi"),
        "New Flag",
        pl.col("County FIPS").alias("FIPS State County Code"),
        "Cost Score",
        "Quality Score",
        pl.col("Specialty").alias("Specialty Description"),'NPI Score',
        "MA Utilization Score",
        "FFS Utilization Score",'Quality color indicator'
    ])
    .filter(pl.col("FIPS State County Code").is_in(county))
    .unique()
    .collect()
    .to_pandas()
)
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
		npiScore_Org['Quality Score'] = npiScore_Org['Quality Score'].fillna("Not available")

		npiScore_Org['Quality color indicator']='Not available'
		#emit_progress(20, "Npi score file for Individual and organisation")
  
        # to implement progress for compareOrgid_list
		length = len(compareorgid_list)
		progress_step = 40 // length if length > 0 else 0
		current_progress = 20
		index=1
				
		for compareId,compareid_name in zip(compareorgid_list,compareorg):
			compare_net=compareId.split("_")[0]
            #compareId="152"
			compare_ind_df,compare_org_df,compare_id=load_compare_files(file_path, filespath,base,compare,baseParentOrgId,planTypeId,baseParentOrg,planType,compare_net,county)
			compare_org_df['Address First Line'] = compare_org_df['Address First Line'].str.title()
            
            
			final_data_ind=pd.concat([base_ind_df,compare_ind_df])
			final_data_org=pd.concat([base_org_df,compare_org_df])
			col_net1 = baseorg
			col_net2 = compareid_name
			grp_cols_ind = ['npi', 'state', 'county_name', 'New Flag']
			print('line 299...')
			final_data_ind['net_count'] = (final_data_ind.groupby(grp_cols_ind)['network_id'].transform('nunique'))
			final_data_ind[col_net1] = np.where(final_data_ind['net_count'] > 1, "Y",np.where(final_data_ind['network_id'] == base_id, "Y", "N"))
            
			final_data_ind[col_net2] = np.where(final_data_ind['net_count'] > 1, "Y",np.where(final_data_ind['network_id'] == compare_id, "Y", "N"))
            
			final_data_ind.drop(columns=['net_count','network_id','network_name','Parent_Organization'], inplace=True)
			final_data_ind=final_data_ind.drop_duplicates()
            
			grp_cols_org = ['npi', 'state', 'county_name','Address First Line', 'New Flag', 'specialty_code']
			print('line 309...')
			final_data_org['net_count'] = (
                final_data_org
                .groupby(grp_cols_org)['network_id']
                .transform('nunique')
            )
            
			final_data_org[col_net1] = np.where(
                final_data_org['net_count'] > 1, "Y",
                np.where(final_data_org['network_id'] == base_id, "Y", "N")
            )
            
			final_data_org[col_net2] = np.where(
                final_data_org['net_count'] > 1, "Y",
                np.where(final_data_org['network_id'] == compare_id, "Y", "N")
            )
            
			final_data_org.drop(columns=['net_count','network_id','network_name','Parent_Organization'], inplace=True)

				#emit_log('line 451...')
			current_progress += progress_step
			#emit_progress(current_progress, "{index} compare file loded")

			index+=1

		columns_to_convert = ['npi', 'FIPS State County Code']
		#emit_progress(82, "Compare files loaded completely")
	
		final_data_ind[columns_to_convert] = final_data_ind[columns_to_convert].astype('int64')
		final_data_org[columns_to_convert] = final_data_org[columns_to_convert].astype('int64')
	
		npiScore_ind[columns_to_convert] = npiScore_ind[columns_to_convert].astype('int64')
		npiScore_Org[columns_to_convert] = npiScore_Org[columns_to_convert].astype('int64')
		npiScore_Org['Address First Line']=npiScore_Org['Address First Line'].str.strip().str.upper()

		npiScore_ind["merge"] = np.where(npiScore_ind['New Flag'].isin(["PCP"]),"PCP",npiScore_ind['Specialty Description'])
		npiScore_ind.drop(columns=['Specialty Description'],inplace=True)
		final_data_ind['merge']=np.where(final_data_ind['New Flag'].isin(["PCP"]),"PCP",final_data_ind['Specialty Description'])
		final_data_ind = final_data_ind.merge(npiScore_ind, on=['npi','New Flag','FIPS State County Code','merge'], how='left')
		final_data_ind[['NPI Score','Quality Score', 'Cost Score',  'FFS Utilization Score','MA Utilization Score','Quality color indicator']] = final_data_ind[['NPI Score','Quality Score', 'Cost Score',  'FFS Utilization Score','MA Utilization Score','Quality color indicator']].fillna(value='Not Available')
		final_data_ind.drop(columns=['merge'],inplace=True)
		##--Spe
		# emit_log(final_data_org.columns)
		final_data_org['Address First Line']=final_data_org['Address First Line'].str.strip().str.upper()
		final_data_org = final_data_org.merge(npiScore_Org, on=['npi','Address First Line', 'state', 'FIPS State County Code'], how='left')
		final_data_org[['Quality Score', 'Cost Score', 'FFS Utilization Score','MA Utilization Score','Quality color indicator']] = final_data_org[['Quality Score', 'Cost Score', 'FFS Utilization Score','MA Utilization Score','Quality color indicator']].fillna(value='Not Available')
		
# 		final_data_org['location_confidence_address_level']='High'
# 		final_data_org['location_confidence']='High'
# 		
		final_data_ind['Quality Score'] = pd.to_numeric(final_data_ind['Quality Score'],errors = "coerce")
		final_data_ind['NPI Score'] = pd.to_numeric(final_data_ind['NPI Score'],errors = "coerce")

		final_data_ind['Cost Score'] = pd.to_numeric(final_data_ind['Cost Score'],errors = "coerce")
		final_data_ind['FFS Utilization Score'] = pd.to_numeric(final_data_ind['FFS Utilization Score'],errors = "coerce")
		final_data_org['Quality Score'] = pd.to_numeric(final_data_org['Quality Score'],errors = "coerce")
		final_data_org['Cost Score'] = pd.to_numeric(final_data_org['Cost Score'],errors = "coerce")
		final_data_org['FFS Utilization Score'] = pd.to_numeric(final_data_org['FFS Utilization Score'],errors = "coerce")
		final_data_org['MA Utilization Score'] = pd.to_numeric(final_data_org['MA Utilization Score'],errors = "coerce")
		final_data_ind['MA Utilization Score'] = pd.to_numeric(final_data_ind['MA Utilization Score'],errors = "coerce")
		
		final_data_combined = pd.concat([final_data_ind, final_data_org])

		final_data_combined.drop_duplicates(inplace=True)
		#final_data_combined.drop(columns=['network_id', 'Parent_Organization','network_name'], inplace=True, axis=1)
		restCol = ['type','npi', 'first_name', 'last_name', 'presentation_name', 'Address First Line','zip_code','city','state','county_name' ,'location_confidence','location_confidence_address_level', 'New Flag','specialty_category', 'specialty_name', 'specialty_subspecialty',
		'specialty_code','Specialty Description','Specialty Flag', 'Status', 'FIPS State County Code','Is_Dual_Provider','group_tin','group_name','carrier_name','Quality Score','NPI Score', 'Cost Score', 'FFS Utilization Score','MA Utilization Score','Quality color indicator']
		scoreCol = ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality color indicator']
		payerCols = list(set(final_data_combined.columns) - set(restCol))
		restCol = ['type','npi', 'first_name', 'last_name', 'presentation_name', 'Address First Line','zip_code','city','state','county_name' ,'location_confidence','location_confidence_address_level','New Flag','Specialty Description','Specialty Flag','group_tin','group_name', 'Status', 'FIPS State County Code']
		col_seq=  restCol + payerCols + scoreCol
		#col_seq = [i for i in col_seq if i not in ['Parent_Organization',  'Bid_ID', 'FIPS State County Code']]
		final_data_combined[payerCols] = final_data_combined[payerCols].fillna(value='N')
		final_data_combined.isnull().sum()
		columns_dict = {
			'New Flag': 'Provider Type',
			'county_name': 'County',
			'npi': 'NPI',
			'type': 'Type',
			'presentation_name': 'Presentation Name',
			'city': 'City',
			'state': 'State',
			'zip_code': 'Zip Code',
			'Specialty Description': 'Specialty Name',
			'first_name': 'First Name',
            'location_confidence':"Location Confidence County Level",
            'location_confidence_address_level':"Location Confidence Address level",            
			'last_name': 'Last Name', 'ma_uti_count':'MA Utilization Count',
            'ffs_uti_count':'FFS Utilization Count',
            "group_name":"Group Name","group_tin":"Group TIN"
		}

		# Rename columns for final_data_ind
		# # Map description
		desc_map = {
			 '1': "Performing Greatly Below Benchmark",
			 '2': "Performing Below Benchmark",
			 '3': "Performing at Benchmark",
			 '4': "Performing Above Benchmark",
			 '5': "Performing Greatly Above Benchmark", "Not Available":'Not Available'
		 }
		final_data_combined=final_data_combined[col_seq].drop_duplicates()
		final_data_combined.rename(columns=columns_dict, inplace=True)
		final_data_combined['City']=final_data_combined['City'].str.title()
		final_data_combined = final_data_combined.sort_values(by="NPI",ascending=True)

		cols=['NPI Score','Quality Score', 'Cost Score','FFS Utilization Score','MA Utilization Score']

		final_data_combined[cols] = final_data_combined[cols].replace('Not Available', np.nan)
  
		final_data_combined_na =   final_data_combined[(final_data_combined['Quality Score'].isna())&(final_data_combined['Cost Score'].isna())]
  
		final_data_combined_not_na =   final_data_combined[~((final_data_combined['Quality Score'].isna())&(final_data_combined['Cost Score'].isna()))]
		final_data_combined = pd.concat([final_data_combined_not_na,final_data_combined_na])

		cols= ['NPI Score','Quality Score','Cost Score','FFS Utilization Score','MA Utilization Score','Quality color indicator']
		final_data_combined[cols]= final_data_combined[cols].replace("", pd.NA).astype("string")
		final_data_combined[cols]= final_data_combined[cols].fillna('NA')
		final_data_combined.rename(columns = {'Quality color indicator' : 'Quality Score Confidence'}, inplace=True)
  
		#emit_progress(87, "Scores column added in dataframe")
        
		# UNCOMMENT TO RUN IN PNC
		############################
		Ahd_hosp_map=pd.read_csv(f"{additionalFilesPath}{filespath}/Hospital_name_mapping.csv")
		Ahd_hosp_map['Presentation Name']=Ahd_hosp_map['Presentation Name'].str.strip().str.upper()
		Ahd_hosp_map['AHD_Facility Name']=Ahd_hosp_map['AHD_Facility Name'].str.strip()
		# print(Ahd_hosp_map.head())
		final_data_combined_hosp =final_data_combined[final_data_combined['Provider Type']=='Hospital']
		final_data_combined_hosp['Presentation Name']=final_data_combined_hosp['Presentation Name'].str.strip().str.upper()
		final_data_combined_hosp=final_data_combined_hosp.merge(Ahd_hosp_map,on="Presentation Name",how='left')
 
		final_data_combined_hosp['Presentation Name'] = (
            final_data_combined_hosp['AHD_Facility Name']
            .fillna(final_data_combined_hosp['Presentation Name'].str.title())
        )
        # Optional: drop helper column
		final_data_combined_hosp.drop(columns=['AHD_Facility Name'], inplace=True)
		del Ahd_hosp_map
		# print(final_data_combined_hosp.head())
		# final_data_combined_hosp =final_data_combined[final_data_combined['Provider Type']=='Hospital']

		if clientId in [45]:
			hosp_scoreCol = ['Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']
			ind_scoreCol = ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']

			restCol_hosp = ['Type','NPI', 'Presentation Name', 'Address First Line','Zip Code','City', 'State', 'County','FIPS State County Code','Provider Type', 'Specialty Name','Status']

			restCol = ['Type','NPI', 'First Name', 'Last Name', 'Presentation Name', 'Zip Code','City', 'State', 'County','FIPS State County Code',"Location Confidence County Level",'Provider Type', 'Specialty Name','Specialty Flag', 'Status']
			col_seq_hosp=restCol_hosp + payerCols + hosp_scoreCol
			col_seq_ind=restCol + payerCols + ind_scoreCol
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].replace("Not available", pd.NA).astype("string")  
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].fillna('NA')
		elif clientId==8: #demo
			hosp_scoreCol = ['Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']
			ind_scoreCol = ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']

			restCol_hosp = ['Type','NPI', 'Presentation Name', 'Address First Line','Zip Code','City', 'State', 'County','FIPS State County Code','Provider Type', 'Specialty Name','Status']

			restCol = ['Type','NPI', 'First Name', 'Last Name', 'Presentation Name','Address First Line', 'Zip Code','City', 'State', 'County','FIPS State County Code',"Location Confidence Address level","Location Confidence County Level",'Provider Type', 'Specialty Name','Specialty Flag','Group TIN','Group Name', 'Status']
			col_seq_hosp=restCol_hosp + payerCols + hosp_scoreCol
			col_seq_ind=restCol + payerCols + ind_scoreCol
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].replace("Not available", pd.NA).astype("string")  
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].fillna('NA')
            
		elif clientId==5:
			hosp_scoreCol = ['Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']
			ind_scoreCol = ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','Quality Score Confidence']

			restCol_hosp = ['Type','NPI', 'Presentation Name', 'Address First Line','Zip Code','City', 'State', 'County','FIPS State County Code','Provider Type', 'Specialty Name','Status']

			restCol = ['Type','NPI', 'First Name', 'Last Name', 'Presentation Name', 'Zip Code','City', 'State', 'County','FIPS State County Code',"Location Confidence County Level",'Provider Type', 'Specialty Name','Specialty Flag','Group TIN','Group Name', 'Status']
			col_seq_hosp=restCol_hosp + payerCols + hosp_scoreCol
			col_seq_ind=restCol + payerCols + ind_scoreCol
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].replace("Not available", pd.NA).astype("string")  
			final_data_combined_hosp['Quality Score Confidence']=final_data_combined_hosp['Quality Score Confidence'].fillna('NA')

            
             
		else:
			hosp_scoreCol = ['Cost Score', 'Quality Score','Quality Score Confidence']
			ind_scoreCol = ['NPI Score','Quality Score Confidence']
			restCol_hosp = ['Type','NPI', 'Presentation Name', 'Address First Line','Zip Code','City', 'State', 'County','FIPS State County Code','Provider Type', 'Specialty Name','Status']

			restCol = ['Type','NPI', 'First Name', 'Last Name', 'Presentation Name', 'Zip Code','City', 'State', 'County','FIPS State County Code','Provider Type', 'Specialty Name', 'Status']
			col_seq_hosp=restCol_hosp + payerCols + hosp_scoreCol
			col_seq_ind=restCol + payerCols + ind_scoreCol


            
		final_data_combined_hosp = final_data_combined_hosp[col_seq_hosp].drop_duplicates()
		final_data_combined_hosp['Address First Line'] = final_data_combined_hosp['Address First Line'].str.title()
		final_data_combined_hosp['County'] = final_data_combined_hosp['County'].str.title()
		final_data_combined_hosp['Status'] = final_data_combined_hosp['Status'].str.title()
		if 'Status' in final_data_combined_hosp.columns:
			final_data_combined_hosp.drop(columns = 'Status', inplace=True)
        # Saving Hospital to Hospital downloadable file
		final_data_combined_hosp.rename(columns={"Quality Score Confidence":"NPI Score Confidence"},inplace=True)

		for col in ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','NPI Score Confidence']:
			if col in final_data_combined_hosp.columns:
				final_data_combined_hosp[col]=final_data_combined_hosp[col].fillna("NA").astype("string")
				final_data_combined_hosp[col]=final_data_combined_hosp[col].replace("Not available", pd.NA).astype("string")
        

		# print('line 529..',final_data_combined_hosp[['Presentation Name']].head())
		df_final_data_hosp = pl.from_pandas(final_data_combined_hosp)
  
		# Saving Hospital to Hospital downloadable file csv and parquet
		df_final_data_hosp.write_csv(outputfile_path_hosp + str(id_parameter) + '.csv')
		df_final_data_hosp.write_parquet(outputfile_path_hosp + str(id_parameter) + ".parquet")

		npi_to_npi_df_ind = final_data_combined[final_data_combined['Provider Type']!='Hospital'][col_seq_ind].drop_duplicates()
  
		for column_case in ['First Name','Last Name','Presentation Name','County','Address First Line','Location Confidence Address level', 'Status']:
			if column_case in npi_to_npi_df_ind.columns:
				npi_to_npi_df_ind[column_case] = (npi_to_npi_df_ind[column_case].astype(str).str.title())

		if clientId in [45,5]:
			# final_data_combined_hosp["Location Confidence Address level"]="High"
			final_data_combined_hosp["Location Confidence County Level"]="High"
			final_data_combined_hosp.drop(columns = ['Address First Line'], inplace=True)
			npi_to_npi_df_ind['Quality Score Confidence']= npi_to_npi_df_ind['Quality Score Confidence'].replace("Not available", pd.NA).astype("string")
			npi_to_npi_df_ind['Quality Score Confidence']=npi_to_npi_df_ind['Quality Score Confidence'].fillna("NA")

		elif clientId==8: #demo
			final_data_combined_hosp["Location Confidence Address level"]="High"
			final_data_combined_hosp["Location Confidence County Level"]="High"
			npi_to_npi_df_ind['Quality Score Confidence']= npi_to_npi_df_ind['Quality Score Confidence'].replace("Not available", pd.NA).astype("string")
			npi_to_npi_df_ind['Quality Score Confidence']=npi_to_npi_df_ind['Quality Score Confidence'].fillna("NA")
		else:
			final_data_combined_hosp.drop(columns = ['Address First Line'], inplace=True)

		#npi_to_npi_df_ind = final_data_combined[final_data_combined['Provider Type']!='Hospital'][col_seq_ind].drop_duplicates()

		npi_to_npi_df_ind.rename(columns={"Quality Score Confidence":"NPI Score Confidence"},inplace=True)
		npi_to_npi_df=pd.concat([npi_to_npi_df_ind,final_data_combined_hosp],ignore_index=True)
        
		if "Group TIN" in npi_to_npi_df.columns:
			npi_to_npi_df['Group TIN']=pd.to_numeric(npi_to_npi_df['Group TIN'], errors='coerce').astype('Int64')
			npi_to_npi_df['Group TIN']=(npi_to_npi_df['Group TIN'].astype("Int64").astype("string").fillna("NA"))
            
		if "Group Name" in npi_to_npi_df.columns:
			npi_to_npi_df['Group Name'] = npi_to_npi_df['Group Name'].astype(str).str.strip().replace(['', 'nan', 'None', 'NaN'], 'NA')


		if "NPI Score" in npi_to_npi_df.columns:
			npi_to_npi_df['NPI Score']=npi_to_npi_df['NPI Score'].fillna("NA").astype("string")
		npi_to_npi_df['Location Confidence County Level'] = npi_to_npi_df['Location Confidence County Level'].str.title()
		npi_to_npi_df=npi_to_npi_df.drop_duplicates()

		if 'Status' in npi_to_npi_df.columns:
			npi_to_npi_df.drop(columns = 'Status', inplace=True)
		if 'Specialty Flag' in npi_to_npi_df.columns:
			npi_to_npi_df['Specialty Flag'] = npi_to_npi_df['Specialty Flag'].fillna('Not Applicable')
		if 'NPI Score Confidence' in npi_to_npi_df.columns:
			npi_to_npi_df['NPI Score Confidence']=npi_to_npi_df['NPI Score Confidence'].replace("Not available", pd.NA).astype("string")
            
		for col in ['NPI Score','Cost Score', 'Quality Score','FFS Utilization Score','MA Utilization Score','NPI Score Confidence']:
			if col in npi_to_npi_df.columns:
				npi_to_npi_df[col]=npi_to_npi_df[col].fillna("NA").astype("string")
				npi_to_npi_df[col]=npi_to_npi_df[col].replace("Not available", pd.NA).astype("string")
    
		if clientId not in [8, 45, 5]:
			npi_to_npi_df.drop(columns=['Cost Score', 'Quality Score'], inplace=True, errors='ignore')

		# Normalize mixed-type object columns before polars conversion.
		# Concat of individual (parquet) + hospital (CSV) rows produces mixed
		# str/int (Zip Code) and str/NaN (First Name, Last Name, etc.) columns
		# that pyarrow rejects when building utf8 arrays.
		str_cols = ['Zip Code', 'First Name', 'Last Name', 'Specialty Flag', 'Group Name', 'Status']
		for col in str_cols:
			if col in npi_to_npi_df.columns:
				npi_to_npi_df[col] = npi_to_npi_df[col].astype(str).replace({'nan': 'NA', 'None': 'NA', '': 'NA'})

		df_final_data_ind = pl.from_pandas(npi_to_npi_df)
  
		# Saving NPI to NPI downloadable file csv and parquet
		df_final_data_ind.write_csv(outputfile_path + str(id_parameter) + '.csv')
		df_final_data_ind.write_parquet(outputfile_path + str(id_parameter) + ".parquet")
		#emit_progress(99, "Individual file created on server (CSV and parquet)")
		return 1
	except Exception as e:
		error_message = f"Error: {str(e)}"


		
def main():
    countyId = sys.argv[1]  # '4005,4007,4025,12011,17031,17043' | common counties for all base and compare parent orgs together NOTs parentOrg wise
    id_param = sys.argv[2] # '1'
    filespath = sys.argv[3] # 'UAT'
    clientID = int(sys.argv[4]) # '45'
    userID = sys.argv[5]
    
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
    
    CountStatus = extract_data(filespath=filespath,base=base,compare=compare,baseParentOrgId=baseParentOrgId,compareParentOrgId=compareParentOrgId,planTypeId=planTypeId,baseParentOrg=baseParentOrg,compareParentOrg=compareParentOrg,planType=planType,id_parameter=id_param,county=c2,clientId=clientID,userId=userID)
    
    if(CountStatus==1):
         # emit_progress(100, "Success")
         print("Success")
    else:
         # emit_progress(-1, "Failed")
         print("Failed")


if __name__ == '__main__':
		main()
