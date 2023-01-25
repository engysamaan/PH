""" This Module is for building the Stanadyne table XML"""

import pandas as pd
from AMS_Trigger.config import *
from dbConnect import ProfileID
from AMS_Trigger.XMLbuilder_Utilities import convert2XML, append_xmltree, node_wrapper, insert_xmltree


print('Extracting  <table_Stanadyne>  from DB ...')
Category = 'Stanadyne'
q_stanadyne_cs = """
select AuthCodeIds, string_agg(ShipDistNumber,',') as {1}_CustomerNumber
from (Select ProfileLocation.ShipDistNumber,string_agg(LocationProducts.AuthCodeID,',') as AuthCodeIDs from locationproducts
        join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
        join AuthCodeMaster on LocationProducts.AuthCodeID = AuthCodeMaster.AuthCodeId
        where locationproducts.profileid={0} and ProfileLocation.HideLocFromContract=0 and AuthCodeMaster.Category
        like '{1}%'
        group by ProfileLocation.ShipDistNumber,LocationProducts.LocationID) as tt
    group by  AuthCodeIds
""".format(ProfileID, Category)
q_location_id = """
select top(1) LocationProducts.LocationID  from LocationProducts
join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
-- 166917  | 166500
where locationproducts.profileid={} and ShipDistNumber='{}' and ProfileLocation.HideLocFromContract=0
"""
q_stanadyn_pord_details_I = """
select Distinct ProdDivPrint as Division,
AuthCodeMaster.Category as ProductAuthorizations,
'' as Products, -- this col will the Child to hold product subChild xml block in insert_xmltree func
Pricing as Stanadyne_Pricing,
PromotionalAccess as Stanadyne_PromotionalAccess
from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--   ANY| Top LocationID and for EACH AuthCodeID
where ProfileID = {} and LocationID ={} and AuthCodeMaster.AuthCodeId ={}"""

q_products = """
select  ProdCode,ProdCodeMaster.ProdName  from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--    TOP LOCATION ID  & For EACH AUthCodeID with
where ProfileID ={} and LocationID ={}and AuthCodeMaster.AuthCodeId ={}
"""
q_stanadyn_pord_details_II = """
select Distinct Pricing as Stanadyne_Pricing,
PromotionalAccess as Stanadyne_PromotionalAccess
from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--   ANY| Top LocationID and for EACH AuthCodeID
where ProfileID = {} and LocationID ={} and AuthCodeMaster.AuthCodeId ={}
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
				{f"{Category}_ProductCode": "",
				 f"{Category}_ProductCodeDescription": "",
				 }]
	# print("############################################")
	# print(Products_Dict)
	return Products_Dict


########################## MANIN ####################################################
print('Building "<table_Stanadyne>" !!!\n')
stanadyne_cs_df = pd.read_sql_query(q_stanadyne_cs, con)
# print(stanadyne_cs_df)
table = stanadyne_cs_df

for row in range(len(table)):  ## len(2)
	### Extract only CustomerNumber_df
	CustomerNumber = table.Stanadyne_CustomerNumber
	# CustomerNumber.index = ['Stanadyne_CustomerNumber']
	cs_df = pd.DataFrame(CustomerNumber.loc[[row]])
	
	convert2XML(cs_df, filename='transition_II.xml', mode='w')
	
	### Extract all AuthCodeIds and its CustomerNumer and split on ','
	top_CSnum = table.Stanadyne_CustomerNumber.str.split(',').tolist()[row][
		0]  ## [0] top CSnum 4each row | no loop needed
	# print(f'*** CSNum: {top_CSnum} ***')
	AuthCode_list = (table.AuthCodeIds.str.split(',').tolist())[row]
	## Get Location ID by passing CS num which is the ShipDistNumber
	id = pd.read_sql_query(q_location_id.format(ProfileID, top_CSnum), con)  # 1404
	location_id = (id.LocationID[0])  # type:int  ## add '0' to  get the num out from series and pass it
	## Loop in AuthCode_list and get AddendumDetails (table 2) 4each location
	for auth_id in range(len(AuthCode_list)):
		# print(auth_id, AuthCode_list[auth_id])
		stanadyne_prodDetails = pd.read_sql_query(
				q_stanadyn_pord_details_I.format(ProfileID, location_id, AuthCode_list[auth_id]), con)
		stanadyne_prodDetails = stanadyne_prodDetails.replace(regex=r'Tier.\d\D',
		                                                      value='').replace(regex='No', value='-')  # remove Tier.n
		
		# middle part should print – Premier, Partner,etc.
		stanadyne_prodDetails.fillna("-", inplace=True)  # replace any none value with (-)
		stanadyne_prodDetails.index = ['Stanadyne-ProductDetails']
		## convert the AddendumProductDetails to XML
		convert2XML(stanadyne_prodDetails, filename="transition_I.xml", mode="w")
		### Crete the json data from the 3rd XML level|table
		json_data = create_inner_table(AuthCode_list[auth_id], location_id)
		str_xml = convert2XML(
				dict_or_df=json_data,
				custome_root="Products",
				# item_func="Product",
				# mode="a",  ## uncomment to see the data for  testing only
				# filename="transition_I.xml",  ## uncomment to see the data for  testing only
				)
		# source_file = 'transition_II.xml'
		# dist_file = "xml_4.xml"
		parent_tag = '{}-Product'.format(Category)
		end_tag = parent_tag
		
		insert_xmltree(xml=str_xml, myChild='Products', filename="transition_I.xml", subChild_tag="Product")
		
		append_xmltree("transition_II.xml", "transition_I.xml", mode="a")
	node_wrapper("transition_II.xml", f"{Category}.xml", parent_tag, end_tag, mode="a")
try:
	with open(f"{Category}.xml", "r") as f, open("xml_1.xml", "a") as g:
		g.write("<table_{0}>{1}</table_{0}>".format(Category, f.read()))
except Exception as e:
	print(f'No {Category} data found for this ProfileID: {ProfileID}\n'
	      f'Error: {e}')
	pass
