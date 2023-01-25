"""
Generate XML format to PFG agreements. This module get triggered by the AMS_Trigger.
It takes is ProfileId as arguments
1. It deletes any XML files before running
2. It imports all the modules' that are used in building the XML format
3. Wrapp XML file with <root> tag and save it in output folder with the ASEAN_{profileID}
"""

import os
import re
import sys
import traceback

'''import the AMS_Triger packge that CMD can read'''
sys.path.extend(['C:\\Users\\Public\\Public Scripts\\AMS_CLM_{}', 'C:/Users/Public/Public Scripts/AMS_CLM_{}'.format(sys.argv[2])])
from AMS_Trigger.config import ProfileID
### Delete all
CURR_DIR = os.path.dirname(os.path.realpath(__file__))
all_files = os.listdir(CURR_DIR)
xml_files = list(filter(lambda f: f.endswith('.xml'), all_files))
for f in xml_files:
	print(f'PFG-Del: {f}')
	os.remove(f)


print(f'Executing "PFG" <=> ProfileID: ' + str(ProfileID))
try:
	import contractInformation_table
	import Signer_table
	import SummaryofAuthorization_table
	import Addendum_table
	import Stanadyne_table
	import Baldwin_table

	BusinessGroup = 'PFG'
	with open("xml_1.xml", "r") as f, open(f"output/{BusinessGroup}_{ProfileID}.xml", "w") as g:
		g.write("<root>{}</root>".format(f.read()))
except Exception as e:
	print(f'ERROR HIT {e}')
	print(traceback.format_exc())

### Delete all
CURR_DIR = os.path.dirname(os.path.realpath(__file__))
all_files = os.listdir(CURR_DIR)
xml_files = list(filter(lambda f: f.endswith('.xml'), all_files))
for f in xml_files:
	os.remove(f)
