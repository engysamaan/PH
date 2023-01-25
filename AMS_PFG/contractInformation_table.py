"""PFG ContractInformation"""
import io
import pandas as pd
from functools import reduce
from AMS_Trigger.config import *
from dbConnect import ProfileID
from AMS_Trigger.XMLbuilder_Utilities import convert2XML


print("############################### ")
# ProfileID = 805  # 805| L10191 - 806|166500
# q_DistNumber = """select DistParentNumber from AgreementProfile
# where ProfileID = {}""".format(ProfileID)

DistParentNumber = pd.read_sql_query(
		"""select DistParentNumber from AgreementProfile
	where ProfileID = {};""".format(ProfileID), con)

print('Extracting Data from ProfileID: {} and DisParentNum: {}'.format(ProfileID,
                                                                       DistParentNumber["DistParentNumber"][0]))

q_GroupNameAbbreviated = """select
       AgreementProfile.ProfileID
       ,AMSGroupMaster.FullName as GroupName
       ,AMSGroupMaster.GroupName as GroupNameAbbreviated
from AMSGroupMaster
join AgreementProfile on AgreementProfile.AMSGroupID = AMSGroupMaster.AMSGroupId
where AgreementProfile.ProfileID = {};""".format(
		ProfileID
		)
## -- Rest of ConfratInformation & DistributorShareholders

q_DistributorShareholders = """
select AgreementProfile.ProfileID,
DistParentName as ParentDistributorName,
DistParentNumber as ParentDistributorNumber,
ProfileLocation.ShipStreet1 as ParentDistributorStreetAddress,
ProfileLocation.ShipStreet2 as ParentDistributorStreetAddress2,
ProfileLocation.ShipCity as ParentDistributorCity,
ProfileLocation.ShipStateName as ParentDistributorState,
ProfileLocation.ShipPostalCode as ParentDistributorZipCode,
ProfileLocation.ShipCountry as ParentDistributorCountry,
DistOwnerInfo1 , DistOwnerInfo2 , DistOwnerInfo3,
GlobalRegion as ParentDistributorGeographicRegion,
'Distribution Agreement' as ContractType,
'3 years' as ContractTerm,
'Indirect Customer' as CustomerType,
ReportingType
from AgreementProfile
full outer join ProfileLocation on AgreementProfile.ProfileID =ProfileLocation.ProfileID
where AgreementProfile.ProfileID = {} and ProfileLocation.IsHeadQuarterLocation =1;""".format(ProfileID)

## Agreement Renewal dates (EffectiveDate and ExpiryDate)
q_AgreementRenewal = """
select top 1 RequestType from WFInstance
where ProfileID ={}
ORDER by CreatedOn DESC
""".format(ProfileID)



## <ParentDistributorSalesRegion>
q_ParentDistributorSalesRegion = """
select INDRegion,EMRegion from PFGTerritorymaster
where ZipCode = (
				select ProfileLocation.ShipPostalCode as ParentDistributorZipCode from ProfileLocation
				where ProfileLocation.IsHeadQuarterLocation =1 and ProfileID ={})""".format(ProfileID)

## ContractSubType (Omni main or Omni Simplified)
q_ContractSubType = """
select AgreementProfile.ProfileID,
case when total_0_AuthSimple > 0 or total_n_AuthSimple > 0  then 'Omni Main'
        else 'Omni Simplified' end as ContractSubType
from AgreementProfile
    left outer join (select AgreementProfile.ProfileID as ProfileID
                          , DistParentNumber as DistParentNumber
                          , sum(case when AuthCodeMaster.AuthSimple = 0 then 1 END) as "total_0_AuthSimple"
                          , sum(case when AuthCodeMaster.AuthSimple = 1 then 1 END) as "total_1_AuthSimple"
                          , sum(case when AuthCodeMaster.AuthSimple is Null then 1 END) as "total_n_AuthSimple"
                        from AgreementProfile
                        full outer JOIN ProfileLocation on ProfileLocation.ProfileID = AgreementProfile.ProfileID
                        full outer join LocationProducts on LocationProducts.ProfileID = ProfileLocation.ProfileID
                        full outer join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
                        group by AgreementProfile.ProfileID,AgreementProfile.DistParentNumber)
                        as AP1 on AP1.ProfileID = AgreementProfile.ProfileID
                        and ap1.DistParentNumber = AgreementProfile.DistParentNumber
where AgreementProfile.DistParentNumber =
						(select DistParentNumber from AgreementProfile where ProfileID = {0})
and AgreementProfile.ProfileID={0}""".format(ProfileID)

##### Platform (EnginMobil or Industrial)
q_Platform = """
select AgreementProfile.ProfileID,
case when PlatEng >= 1 and PlatInd = 0 then 'Engine Mobile'
        when platInd >= 1 and PlatEng = 0 then 'Industrial'
        when PlatEng >=1 and PlatInd >=1 then 'Engine Mobile, Industrial'
        else null end as Platform

from AgreementProfile
    left outer join (select AgreementProfile.ProfileID as ProfileID
                        ,sum(case when (ProfileLocation.HideLocFromContract != 1 or
                        ProfileLocation.HideLocFromContract is Null) and AuthCodeMaster.AuthPlatform = 'Engine Mobile' then 1 else 0 end) as "PlatEng"
                        ,sum(case when (ProfileLocation.HideLocFromContract != 1 or ProfileLocation.HideLocFromContract is Null) and AuthCodeMaster.AuthPlatform = 'Industrial' then 1 else 0 end)  as "PlatInd"
                        from AgreementProfile
                        full outer JOIN ProfileLocation on ProfileLocation.ProfileID = AgreementProfile.ProfileID
                        full outer join LocationProducts on LocationProducts.ProfileID = ProfileLocation.ProfileID
                        full outer join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
                        group by AgreementProfile.ProfileID,AgreementProfile.DistParentNumber) as AP1
                        on AP1.ProfileID = AgreementProfile.ProfileID
where AgreementProfile.DistParentNumber =
						(select DistParentNumber from AgreementProfile where ProfileID = {0})
and AgreementProfile.ProfileID={0}""".format(ProfileID)

### AgreementRequestSummary
q_AgreementRequestSummary = """
select top 1 AgreementReqSummary as AgreementRequestSummary from WFInstance
where ProfileID = {}
ORDER by CreatedOn DESC
""".format(ProfileID)

## Technology
q_Technology = """
select STRING_AGG(Tech,'; ') as Technology
from AgreementProfile
    left outer join (select AgreementProfile.ProfileID as ProfileID,
                    AuthCodeMaster.TechnologyTeam  as Tech
                        from AgreementProfile
                        full outer JOIN ProfileLocation on ProfileLocation.ProfileID = AgreementProfile.ProfileID
                        full outer join LocationProducts on LocationProducts.ProfileID = ProfileLocation.ProfileID
                        full outer join AuthCodeMaster on AuthCodeMaster.AuthCodeId = LocationProducts.AuthCodeID
                        group by AgreementProfile.ProfileID,AgreementProfile.DistParentNumber, TechnologyTeam) as AP1
                        on AP1.ProfileID = AgreementProfile.ProfileID

where AgreementProfile.DistParentNumber =
						(select DistParentNumber from AgreementProfile where ProfileID = {0})
and AgreementProfile.ProfileID={0}""".format(ProfileID)

## AllTradeAreas
q_allTradeAreas = """
select string_agg(CONCAT_WS(',', [TradeAreaCodes], [TAEXCTradeAreaCodes], [TANonEXCTradeAreaCodes]),',') as 'AllTradeAreas'
from LocationProducts
where ProfileID = {};
""".format(ProfileID)

#########################################################################################################
######################################## Calling SQL Queries#############################################

def gen_ranges(lst):
	"""
	Func that create join all nums in range
	:param lst: list of int
	:type lst:
	"""
	s = e = None
	for i in sorted(lst):
		if s is None:
			s = e = i
		elif i == e or i == e + 1:
			e = i
		else:
			yield s, e
			s = e = i
	if s is not None:
		yield s, e


## ***** GroupNameAbbreviated : Is the top part of the ContractInformation Table
GroupNameAbbreviated = pd.read_sql_query(q_GroupNameAbbreviated, con)

## ***** DistributorShareholders:
DistributorShareholders = pd.read_sql_query(q_DistributorShareholders, con)
### Str all DistOwnerInfo in 'DistributorShareholders'
DistributorShareholders['DistributorShareholders'] = DistributorShareholders[
	['DistOwnerInfo1', 'DistOwnerInfo2', 'DistOwnerInfo3']].astype(str).agg('; '.join, axis=1)
## Cleaning from (Null, NA,None ans extra ( ; ) )
DistributorShareholders['DistributorShareholders'] = DistributorShareholders['DistributorShareholders'].replace(
		regex=r'(; ;|; Null|; NA|; None|; $)', value='')

## Agreement renewal dates
def agreementRenewal():
	"""
	If RequestType column in WFInstance = “Agreement Renewal”, then blank out the EffectiveDate and ExpiryDate in the XML going to DocuSign
	else get EffectiveDate and ExpiryDate from AgreementProfile table
	"""
	AgreementRenewal = pd.read_sql_query(q_AgreementRenewal, con)
	if not AgreementRenewal.empty and 'Agreement Renewal' not in AgreementRenewal['RequestType'][0]:
		Dates = pd.read_sql_query('''
		select AgreementProfile.ProfileID, convert(varchar, EffectiveDate, 101) as EffectiveDate,
		convert(varchar, AgreementExpDate, 101) as ExpiryDate -- mm/dd/yyyy
		from AgreementProfile
		full outer join ProfileLocation on AgreementProfile.ProfileID =ProfileLocation.ProfileID
		where AgreementProfile.ProfileID = {} and ProfileLocation.IsHeadQuarterLocation =1;
	'''.format(ProfileID), con)
	else:
		Dates = pd.read_sql_query('''
		select AgreementProfile.ProfileID,'' as 'EffectiveDate', '' as 'ExpiryDate' from AgreementProfile
		where AgreementProfile.ProfileID = {}
		'''.format(ProfileID), con)
	return Dates
AgreementRenewalDates = agreementRenewal()

## ***** ParentDistributorSalesRegion:
ParentDistributorSalesRegion = pd.read_sql_query(q_ParentDistributorSalesRegion, con)
ParentDistributorSalesRegion.insert(0, 'ProfileID', ProfileID)
## combine Industrial & EnginMobile Region under "ParentDistributorSalesRegion" name and adding (Ind: & EM:) prefix
ParentDistributorSalesRegion['ParentDistributorSalesRegion'] = ParentDistributorSalesRegion[['INDRegion', 'EMRegion']] \
	.apply(lambda x: 'Ind: ' + ', EM: '.join(x.dropna().astype(str)), axis=1)

## ***** ContractSubType:
ContractSubType = pd.read_sql_query(q_ContractSubType, con)

## ***** Platform:
Platform = pd.read_sql_query(q_Platform, con)

## ***** AgreementRequestSummary:
AgreementRequestSummary = pd.read_sql_query(q_AgreementRequestSummary, con)
AgreementRequestSummary.insert(0, 'ProfileID', ProfileID)  # Adding ProfileID, so it can be merged with the rest of the DFs


## ***** Technology:
Technology = pd.read_sql_query(q_Technology, con)
Technology.insert(0, 'ProfileID', ProfileID)  ##Adding ProfileID so it can be mergred with the rest of the DFs



## ***** AllTradeAreas:
AllTradeAreas_df = pd.read_sql_query(q_allTradeAreas, con)
if AllTradeAreas_df['AllTradeAreas'][0] is not None:  ## Check if no TradeAreas
	print('No TradeAreas')
	alltrades_lst = AllTradeAreas_df['AllTradeAreas'][0].split(',')
	alltrades_lst = list(filter(None, alltrades_lst))  ## remove the empty strings from the list
	alltrades_lst = list(map(int, alltrades_lst))  ## convert the list to ints
	AllTradeAreas = (','.join(['%d' % s if s == e else '%d-%d' % (s, e) for (s, e) in gen_ranges(alltrades_lst)]))
	data = io.StringIO(AllTradeAreas)
	AllTradeAreas_df = (pd.DataFrame(data, columns=['AllTradeAreas']))

AllTradeAreas_df.insert(0, 'ProfileID', ProfileID)  # Adding ProfileID, so it can be merged with the rest of the DFs

## define list of DataFrames
dfs = [GroupNameAbbreviated, DistributorShareholders,AgreementRenewalDates, ParentDistributorSalesRegion,
       ContractSubType, Platform,
       AgreementRequestSummary, Technology, AllTradeAreas_df]

# merge all DataFrames into one on ProfileID


final_df = reduce(
		lambda left, right: pd.merge(left, right, on=["ProfileID"], how="outer"), dfs
		)

ContractInformation = final_df[
	['GroupName', 'ProfileID', 'GroupNameAbbreviated', 'ParentDistributorName', 'ParentDistributorNumber',
	 'ParentDistributorStreetAddress', 'ParentDistributorStreetAddress2',
	 'ParentDistributorCity', 'ParentDistributorState', 'DistributorShareholders',
	 'ParentDistributorZipCode', 'ParentDistributorCountry',
	 'ParentDistributorGeographicRegion', 'ParentDistributorSalesRegion', 'ContractType',
	 'ContractSubType',
	 'EffectiveDate',
	 'ContractTerm', 'ExpiryDate', 'Platform', 'Technology', 'ReportingType',
	 'CustomerType', 'AllTradeAreas',
	 'AgreementRequestSummary']]

# adding the index that will be the XML node:
ContractInformation.index = ["ContractInformation"]

print('Building 1st table A: "<ContractInformation>" !!!\n')
convert2XML(ContractInformation, filename='xml_1.xml')
