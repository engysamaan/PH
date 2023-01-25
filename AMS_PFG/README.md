## AMS_PFG
Generate XML format to PFG agreements. This module get triggered by the AMS_Trigger. Also it can be executed from 
AMS_PFG/main.py.
It takes is ProfileId as 1st arg and env. as 2nd arg

### main.py
1. It deletes any XML files before running
2. It imports all the modules' that are used in building the XML format
3. It saves the final xml in `AMS_Trigger/output` dir or `AMS_PFG/output`(if AMS_PFG was executed from its 
   AMS_PFG/main.py 
   and not from AMS_Trigger/main.py)

### AMS_PFG Structure
* main.py
* contractInformation_table.py
* Signer_table.py
* SummaryofAuthorization_table.py
* Addendum_table.py
* Stanadyne_table.py
* Baldwin_table.py
* XMLbuilder_Utilities.py

### PFG XML Structure
* contractInformation_table
* Signer_table
* SummaryofAuthorization_table
* Addendum_table
* Stanadyne_table
* Baldwin_table

Each module is for building a specific peace in the xml structure
Each module is named after the Parent tag followed by `_table`.
ex: Signer_table is for building the Signer Tag
