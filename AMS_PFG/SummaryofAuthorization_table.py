""" table_SummaryofAuthorizationsandCoverage   """
import io
import pandas as pd
from AMS_Trigger.config import *
from dbConnect import ProfileID
from AMS_Trigger.XMLbuilder_Utilities import convert2XML, append_xmltree, node_wrapper, insert_xmltree, SummarizeRange

""""""
print('Extracting Summury table from DB ...')

q_SummaryofAuthorizationsandCoverage = """
SELECT LocationID ,ShipDistNumber  as DistributorLocationNumber,
ShipName as DistributorLocationName,
ShipStreet1 as DistributorLocationStreetAddress,
ShipStreet2 as DistributorLocationStreetAddress2,
ShipCity as DistributorLocationCity,
ShipStateName as DistributorLocationState,
ShipPostalCode as DistributorLocationZipCode,
ShipCountry as DistributorLocationCountry
from ProfileLocation
-- if 1 = Hide it else Show it
where ProfileLocation.ProfileID ={} and HideLocFromContract = 0;
""".format(ProfileID)

q_productAuth = """
SELECT LocationProducts.LocationID,
AuthCodeMaster.Category as ProductAuthorizationName,
CONCAT_WS(' ,', [TradeAreaCodes], [TAEXCStateIDS], [TANonEXCStateIDS]) AS 'TradeAreas',
TradeAreaCodes as GeneralTradeAreas,
-- if Ex | NON EX =NULL => NO then tags[EX, NonEX]
TAEXCStateIDS as ExclusiveTradeAreas,
TANonEXCStateIDS as NonExclusiveTradeAreas
From LocationProducts
Full Outer JOIN AuthCodeMaster
on LocationProducts.AuthCodeID = AuthCodeMaster.AuthCodeId
Where LocationProducts.LocationID = {};
"""

SummaryofAuthorizationsandCoverage = pd.read_sql_query(
        q_SummaryofAuthorizationsandCoverage, con, index_col=None
)

LocationIDs = SummaryofAuthorizationsandCoverage["LocationID"]

def create_inner_table(locID):
    for locID in LocationIDs:
        # print(f" - Product for LocationID {locID}")
        product_authorizations = pd.read_sql(q_productAuth.format(locID), con)
        if len(product_authorizations) != 0:
            prod_list = product_authorizations["ProductAuthorizationName"]
            trade_list = product_authorizations["TradeAreas"]
            generalTrade_list = product_authorizations["GeneralTradeAreas"]
            exclusive_list = product_authorizations["ExclusiveTradeAreas"]
            NonExclusive_list = product_authorizations["NonExclusiveTradeAreas"]
            ProductAuthorizations_Dict = [
                    {"ProductAuthorizationName": p, "TradeAreas": t,
                     "GeneralTradeAreas": g, "ExclusiveTradeAreas": e,
                     "NonExclusiveTradeAreas": n
                     }
                    for p, t, g, e, n in zip(prod_list, trade_list, generalTrade_list, exclusive_list,
                                             NonExclusive_list)
            ]
        else:
            ## if there is no data
            print("Df is empty for Location")
            ProductAuthorizations_Dict = [
                    {
                            "ProductAuthorizationName": "",
                            "TradeAreas": "",
                            "GeneralTradeAreas":"",
                            "ExclusiveTradeAreas":"",
                            "NonExclusiveTradeAreas":""
                    }
            ]
        # print(ProductAuthorizations_Dict)
        return ProductAuthorizations_Dict


print('Building "<SummaryofAuthorizationsandCoverage>" !!!\n')
table = SummaryofAuthorizationsandCoverage
index = "SummaryofAuthorizationsandCoverage"

# subChild_tag="ProductAuthorizations"
source_file = "xml_2.xml"
dist_file = "xml_1.xml"
parent_tag = "table_SummaryofAuthorizationsandCoverages"
end_tag = "table_SummaryofAuthorizationsandCoverages"

balston_df = pd.DataFrame()

""" Creating BalstoneSelected Node"""

def balstonSelected():
    """
        to generate BalstonSelected Tag
    """
    for locID in LocationIDs:
        prodAuth = pd.read_sql(q_productAuth.format(locID), con)
        # print(prodAuth['ProductAuthorizationName'])
        if 'IGFG Balston Filtration' in prodAuth.values or 'IGFG Balston N2' in prodAuth.values:
            balston_df['BalstonSelected'] = ['true']
        else:
            balston_df['BalstonSelected'] = ['false']
    convert2XML(balston_df, filename="xml_2.xml", mode="w")
balstonSelected()



for row in range(len(table)):
    ## extract each row as df from the table
    df = pd.DataFrame(table.iloc[[row]])

    df.index = [index]  ## df looks like:  SummaryofAuthorizationsandCoverage    1410
    LocationIDs = df["LocationID"]
    del df['LocationID']  ## drop locationID col(No need for it)
    convert2XML(df, filename="transition_I.xml", mode="w")
    ### Crete the json data from the 3rd XML level|table
    json_data = create_inner_table(LocationIDs[0])
    json_data_l =[]
    for i in range(len(json_data)):
        d =json_data[i]
        d['TradeAreas'] = SummarizeRange(d['TradeAreas']).apply()

        # if d.get('GeneralTradeAreas') is None:
        #     d.pop('GeneralTradeAreas')
        # else:
        #     d['GeneralTradeAreas'] = SummarizeRange(d['GeneralTradeAreas']).apply()

        # d['GeneralTradeAreas'] = SummarizeRange(d['GeneralTradeAreas']).apply()
        d['GeneralTradeAreas'] = d.pop('GeneralTradeAreas') if d.get('GeneralTradeAreas') is None else SummarizeRange(d['GeneralTradeAreas']).apply()

        d.pop('ExclusiveTradeAreas') if d.get('ExclusiveTradeAreas') is None else SummarizeRange(d['ExclusiveTradeAreas']).apply()

        d.pop('NonExclusiveTradeAreas') if d.get('NonExclusiveTradeAreas') is None else SummarizeRange(d['NonExclusiveTradeAreas']).apply()

        json_data_l.append(d) ## put data back in a list so item_fun can be used

    str_xml = convert2XML(
            dict_or_df=json_data_l,
            custome_root="ProductAuthorizations", ## Parent Tag
            item_func="ProductAuthorization", ## Child Tag
            # mode="w",
            # filename="str.xml", ## uncomment to see the data for  testing only
    )
    insert_xmltree(xml=str_xml, filename="transition_I.xml", subChild_tag="ProductAuthorizations")
    append_xmltree("xml_2.xml", "transition_I.xml", mode="a")
node_wrapper("xml_2.xml", dist_file, "table_SummaryofAuthorizationsandCoverages", "table_SummaryofAuthorizationsandCoverages", mode="a")
