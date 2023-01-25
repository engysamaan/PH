"""
This module is to create the PFG Baldwin xml format
"""
import pandas as pd
from AMS_Trigger.config import *
from dbConnect import ProfileID
from AMS_Trigger.XMLbuilder_Utilities import convert2XML, append_xmltree, node_wrapper, insert_xmltree
pd.set_option('display.max_columns', None)



### Delete all all XML after runing
# CURR_DIR = os.path.dirname(os.path.realpath(__file__))
# all_files = os.listdir(CURR_DIR)
# xml_files = list(filter(lambda f: f.endswith('.xml'), all_files))
# for f in xml_files:
# 	print(f' Deleting:{f}')
# 	os.remove(f)



print('Extracting  <table_Baldwin >  from DB ...')
Category = 'Baldwin'
q_baldwin_cs_auth = """
select AuthCodeIds, string_agg(ShipDistNumber,',') as {1}_CustomerNumber
from (Select ProfileLocation.ShipDistNumber,string_agg(LocationProducts.AuthCodeID,',') as AuthCodeIDs from locationproducts
        join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
        join AuthCodeMaster on LocationProducts.AuthCodeID = AuthCodeMaster.AuthCodeId
        where locationproducts.profileid={0} and ProfileLocation.HideLocFromContract=0 and AuthCodeMaster.Category
        like '{1}%'
        group by ProfileLocation.ShipDistNumber,LocationProducts.LocationID) as tt
    group by  AuthCodeIds
""".format(ProfileID, Category)

# Baldwin_df = pd.read_sql_query(q_baldwin_cs_auth, con)
# print(Baldwin_df)

q_baldwin_prod_details = """
select Distinct ProdDivPrint as Division,
AuthCodeMaster.Category as ProductAuthorizations,
'' as Products, -- this col will the Child to hold product subChild xml block in insert_xmltree func
Pricing as Baldwin_Pricing,
PromotionalAccess as Baldwin_PromotionalAccess,
GroupRebate as Baldwin_GroupRebate,
MonthlyRebate as Baldwin_MonthlyRebate
from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--   ANY| Top LocationID and for EACH AuthCodeID
where ProfileID ={} and LocationID ={} and AuthCodeMaster.AuthCodeId = {}
"""

q_location_id = """
select top(1) LocationProducts.LocationID  from LocationProducts
join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
where locationproducts.profileid={} and ShipDistNumber='{}' and ProfileLocation.HideLocFromContract=0
"""
# baldwin_product = pd.read_sql_query(
# 		q_baldwin_prod_details.format(74, 143, 105), con)
# print(baldwin_product.T)
q_products = """
select  ProdCode,ProdCodeMaster.ProdName  from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--    TOP LOCATION ID  & For EACH AUthCodeID with
where ProfileID ={} and LocationID ={}and AuthCodeMaster.AuthCodeId ={}
"""


############## Helper Function #############
def create_inner_table(auth_id, location_id):
	# for auth_id in auth_ids:
	# print(f"Get Products for each authID: {auth_id} and LocationID: {location_id}")
	products_df = pd.read_sql(q_products.format(ProfileID, location_id, auth_id), con)
	# print(products_df)
	if len(products_df) != 0:
		## if df is empty then skip
		prodcode_list = products_df["ProdCode"]
		description_list = products_df["ProdName"]
		Products_Dict = [
				{f"{Category}_ProductCode": p, f"{Category}_ProductCodeDescription": t}
				for p, t in zip(prodcode_list, description_list)
		]
		# print("############################################")
	# print(Products_Dict)
	else:
		print("Df is empty for Location")
		
		Products_Dict = [
				{
						f"{Category}_ProductCode": "",
						f"{Category}_ProductCodeDescription": "",
				}
		]
		# print("############################################")
	# print(Products_Dict)
	return Products_Dict


##########*********************************************#####################
baldwin_product = pd.DataFrame()  ##data is not from DB, based productdetails values
table = pd.read_sql_query(q_baldwin_cs_auth, con)
for row in range(len(table)):
	### Extract only CustomerNumber_df
	CustomerNumber = table.Baldwin_CustomerNumber
	cs_df = pd.DataFrame(CustomerNumber.loc[[row]])
	convert2XML(cs_df, filename='transition_II.xml', mode='w')
	
	### Extract all AuthCodeIds and its CustomerNumer and split on ','
	top_CSnum = table.Baldwin_CustomerNumber.str.split(',').tolist()[row][
		0]  ## [0] top CSnum 4each row | no loop needed
	# print(f'*** CSNum: {top_CSnum} ***')
	AuthCode_list = (table.AuthCodeIds.str.split(',').tolist())[row]
	## Get Location ID by passing CS num which is the ShipDistNumber
	id = pd.read_sql_query(q_location_id.format(ProfileID, top_CSnum), con)  # 1404
	location_id = (id.LocationID[0])  # type:int  ## add '0' to  get the num out from series and pass it
	## Loop in AuthCode_list and get Details (table 2) 4each location
	for auth_id in range(len(AuthCode_list)):
		# print(auth_id, AuthCode_list[auth_id])
		baldwin_prodDetails = pd.read_sql_query(
				q_baldwin_prod_details.format(ProfileID, location_id, AuthCode_list[auth_id]), con)
		
		# print(baldwin_prodDetails["Baldwin_GroupRebate"])
		## To create the top part of Product details, check each column, if value Not none then add column to the df with true,
		# else false
		
		## If value in the database = “true”, then this should read “Yes, If value = “false”, then this should read “-“
		f = lambda x: 'Yes - See Group Program Detail' if x == 1 else '-'
		baldwin_prodDetails["Baldwin_GroupRebate"] = baldwin_prodDetails["Baldwin_GroupRebate"].map(f)
		
		## ShowTag
		## Todo: try to make it a func or class
		if baldwin_prodDetails.Baldwin_Pricing[0] is None:
			baldwin_product['Show_Pricing_Column'] = ['false']
		else:
			baldwin_product['Show_Pricing_Column'] = ['true']
		
		if baldwin_prodDetails.Baldwin_PromotionalAccess[0] is None:
			baldwin_product['Show_PromotionalAccess_Column'] = ['false']
		else:
			baldwin_product['Show_PromotionalAccess_Column'] = ['true']
		
		if baldwin_prodDetails.Baldwin_GroupRebate[0] is None:
			baldwin_product['Show_GroupRebate_Column'] = ['false']
		else:
			baldwin_product['Show_GroupRebate_Column'] = ['true']
			
		## Convert baldwin product to xml
		convert2XML(baldwin_product, filename="transition_II.xml", mode="a")

		
		baldwin_prodDetails.index = ['Baldwin-ProductDetails']
		convert2XML(baldwin_prodDetails, filename="transition_I.xml", mode="w")
		json_data = create_inner_table(AuthCode_list[auth_id], location_id)
		str_xml = convert2XML(
				dict_or_df=json_data,
				custome_root="item",
				item_func="Product",
				# mode="a",  ## uncomment to see the data for  testing only
				# filename="test_xml.xml",  ## uncomment to see the data for testing only
		)
		parent_tag = '{}-Product'.format(Category)
		end_tag = parent_tag
		insert_xmltree(xml=str_xml, myChild='Products', filename="transition_I.xml",
		               subChild_tag="Product")
		append_xmltree("transition_II.xml", "transition_I.xml", mode="a")
	node_wrapper("transition_II.xml", f"{Category}.xml", parent_tag, end_tag, mode="a")
try:
	with open(f"{Category}.xml", "r") as f, open("xml_1.xml", "a") as g:
		g.write("<table_{0}>{1}</table_{0}>".format(Category, f.read()))
except:
	print(f'No {Category} data found for this ProfileID: {ProfileID} ')
	pass
