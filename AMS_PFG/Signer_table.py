""" Get SignerInformation from DB   """

import pandas as pd
# from dbConnect import ProfileID
from AMS_Trigger.config import con, ProfileID
from AMS_Trigger.XMLbuilder_Utilities import convert2XML

print('Extracting SignerInformation from DB ...')

q_SignerInformation = """
SELECT SignerName as DistributorContractSignerName
,SignerTitle DistributorContractSignerTitle
,SignerEmail as DistributorContractSignerEmail

,'{}' as ParkerContractSignerName
,'{}' as ParkerContractSignerTitle
,'{}' as ParkerContractSignerEmail
from AgreementProfile
where AgreementProfile.ProfileID = {}
"""
SignerInformation = pd.read_sql_query(q_SignerInformation.format('Matt P',
                                                                 'Vice President Filtration Group Sales',
                                                                 'xxx@parker.com',
                                                                 ProfileID), con)
SignerInformation.index = ["SignerInformation"]

print('Building "<SignerInformation>" !!!\n')
convert2XML(SignerInformation,
            filename="xml_1.xml", mode="a")

