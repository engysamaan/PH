""" AddendumII Products"""
import re

## TODO: Remove all Stanadyne and Baldin tables if Division = Engine Mobile Aftermarket
import pandas as pd
from AMS_Trigger.config import *
from dbConnect import ProfileID
from AMS_Trigger.XMLbuilder_Utilities import append_xmltree, convert2XML, insert_xmltree, node_wrapper

print('Extracting table_AddendumII-Products table from DB ...')
index = "CustomerNumber_df"

q_addendum_cs = """
select AuthCodeIds, string_agg(ShipDistNumber,',') as CustomerNumber
from (Select ProfileLocation.ShipDistNumber,string_agg(LocationProducts.AuthCodeID,',') as AuthCodeIDs from
locationproducts
        join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
        join AuthCodeMaster on LocationProducts.AuthCodeID = AuthCodeMaster.AuthCodeId
        where locationproducts.profileid={0} and ProfileLocation.HideLocFromContract=0
        and
        (AuthCodeMaster.Category Not like 'Stanadyne%' and AuthCodeMaster.Category Not like 'Baldwin%')
        group by ProfileLocation.ShipDistNumber,LocationProducts.LocationID) as tt
    group by  AuthCodeIds""".format(ProfileID)

q_location_id = """
select top(1) LocationProducts.LocationID  from LocationProducts
join ProfileLocation on ProfileLocation.LocationID = locationproducts.LocationID
-- 166917  | 166500
where locationproducts.profileid={} and ShipDistNumber='{}' and ProfileLocation.HideLocFromContract=0
"""
q_productdetails = """
select Distinct ProdDivPrint as Division,
AuthCodeMaster.Category as ProductAuthorizations,
LocationProducts.Classfication as 'AddendumII-Product_Classification',
LocationProducts.DiscountLevel as 'AddendumII-Product_Pricing'
from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--   ANY| Top LocationID and for EACH AuthCodeID
where ProdDivPrint !='Engine Mobile Aftermarket' and (ProfileID = {} and LocationID ={} and AuthCodeMaster.AuthCodeId ={})"""

q_products = """
select  ProdCode,ProdCodeMaster.ProdName  from LocationProducts
join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
join ProdCodeMaster on ProdCodeMaster.ProdAuthID = AuthCodeMaster.AuthCodeId
--    TOP LOCATION ID  & For EACH AUthCodeID with
where ProfileID ={} and LocationID ={}and AuthCodeMaster.AuthCodeId ={}
"""


## Helper Function:
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
				{"ProductCode": p, "ProductCodeDescription": t}
				for p, t in zip(prodcode_list, description_list)
				]
		# print("############################################")
	# print(Products_Dict)
	else:
		print("Df is empty for Location")

		Products_Dict = [
				{
						"ProductAuthorizationName": "",
						"TradeAreas": "",
						}
				]
		# print("############################################")
	# print(Products_Dict)
	return Products_Dict


########################## MANIN ####################################################
print('Building "<table_AddendumII-Products>" !!!\n')
Addendum_cs_df = pd.read_sql(q_addendum_cs, con)

table = Addendum_cs_df

for row in range(len(table)):  ## len(2)
	### Extract only CustomerNumber_df
	CustomerNumber = pd.DataFrame(table.CustomerNumber)
	df = pd.DataFrame(CustomerNumber.loc[[row]])
	convert2XML(df, filename='transition_II.xml', mode='w')

	### Extract all AuthCodeIds and its CustomerNumer and split on ','
	top_CSnum = table.CustomerNumber.str.split(',').tolist()[row][0]  ## [0] top CSnum 4each row | no loop needed
	# print(f'*** CSNum: {top_CSnum} ***')
	AuthCode_list = (table.AuthCodeIds.str.split(',').tolist())[row]
	## Get Location ID by passing CS num which is the ShipDistNumber
	id = pd.read_sql_query(q_location_id.format(ProfileID, top_CSnum), con)  # 1404
	location_id = (id.LocationID[0])  # type:int  ## add '0' to  get the num out from series and pass it
	## Loop in AuthCode_list and get AddendumDetails (table 2) 4each location
	for auth_id in range(len(AuthCode_list)):
		# print(auth_id, AuthCode_list[auth_id])
		addendum_prodDetails = pd.read_sql_query(
				q_productdetails.format(ProfileID, location_id, AuthCode_list[auth_id]), con)
		if not addendum_prodDetails.empty:
			# extract the middle portion only Ex: Premier, Partner,etc.
			pattern = re.compile(r"^(?:\\.|[^/\\])*/((?:\\.|[^/\\])*)")
			try:
				addendum_prodDetails['AddendumII-Product_Classification'] = pattern.match(
					addendum_prodDetails['AddendumII-Product_Classification'][0]).group(1)
			except Exception as err:
				print(err)

			addendum_prodDetails.fillna("-", inplace=True)  # replace any none value with (-)
			addendum_prodDetails.index = ['AddendumII-ProductDetails']
			## convert the AddendumProductDetails to XML
			convert2XML(addendum_prodDetails, filename="transition_I.xml", mode="w")
			### Crete the json data for the 3rd XML level|table
			json_data = create_inner_table(AuthCode_list[auth_id], location_id)
			str_xml = convert2XML(
					dict_or_df=json_data,
					custome_root="Products",
					item_func="Product",
					# mode="w",  ## uncomment to see the data for  testing only
					# filename="str.xml",  ## uncomment to see the data for  testing only
					)

			insert_xmltree(xml=str_xml, filename="transition_I.xml", subChild_tag="Products") #** indentation will be
			# at the root level
			append_xmltree("transition_II.xml", "transition_I.xml", mode="a")
		parent_tag = 'AddendumII-Product'
		end_tag = parent_tag
	node_wrapper("transition_II.xml", "xml_4.xml", parent_tag, end_tag, mode="a")
try:
	with open("xml_4.xml", "r") as f, open("xml_1.xml", "a") as g:
		g.write("<table_AddendumII-Products>{}</table_AddendumII-Products>".format(f.read()))
except FileNotFoundError:
	print(f'No Addendum II data found for this ProfileID: {ProfileID} ')
	pass
