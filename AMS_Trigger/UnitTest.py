"""Just testing the Task SScheduler"""

import os
import shutil
import sys
import requests
import pandas as pd
from pathlib import Path
from mysql.connector import Error

from config import con, output, python_exe, ScriptDir
from DocuSignAPI import get_access_token, post_xml

######## DELETE ALL XML FIRST from output dir ##############
for file in os.scandir(output):
	print(file)
	os.remove(file.path)


def removeXMlFiles():
	"""
	Delete all XML files between running agreements
	"""
	###
	try:
		CURR_DIR = os.path.dirname(os.path.realpath(__file__))
		all_files = os.listdir(CURR_DIR)
		xml_files = list(filter(lambda f: f.endswith('.xml'), all_files))
		for f in xml_files:
			print(f' Deleting:{f}')
			os.remove(f)
	except FileNotFoundError as f:
		# print(f)
		pass


def SendEmail(filename: str, subject: str, ToEmail: str, body: str):
	"""

	Args:
		filename (str): xml filename, for attachments
		subject (str): email subject
		ToEmail (str): receiver email address
		body (str): email body
	"""
	# try:
	## if No New Agreemts send empty email

	url = "https://sendsmtpmail.azurewebsites.net/api/Email/SendMailToMultipleRecipients"
	payload = {'FromEmail': 'coramssupport@parker.com',
	           'ToEmail': f'{ToEmail}',
	           'Subject': f'{subject}',
	           'Body': f'{body}'}
	headers = {
			'Ocp-Apim-Subscription-Key': 'xxxxx',
			}

	all_attachments = os.listdir(output)
	files = list()  ## ,1,2,3 xml
	if filename is None:
		## Send all the attachment in one email
		for Attachment in all_attachments:
			print(Attachment)
			files.append(('Attachments', (
					Attachment, open(f'{output}\{Attachment}', 'rb'),
					'text/xml')))

		if len(all_attachments) != 0:
			##** Send Email only when there are XML files
			response = requests.request("POST", url, headers=headers, data=payload, files=files)
			print(response.text)

	elif filename is not None and filename !='codeError':
		if filename in all_attachments:
			files = [
					('Attachments', (
							f'{filename}', open(f'{output}\{filename}', 'rb'), 'text/xml'))]

			response = requests.request("POST", url, headers=headers, data=payload, files=files)
			print(response.text)

	elif filename == 'codeError':
		### send email if error while generating xml format
		for Attachment in all_attachments:
			print(Attachment)
			files.append(('Attachments', (
				Attachment, open(f'{output}\{Attachment}', 'rb'),
				'text/xml')))

		if len(all_attachments) >= 0:
			response = requests.request("POST", url, headers=headers, data=payload, files=files)
			print(response.text)


def updateProcessTable(profile_id, current_status: str, new_status: str):
	"""
	this query is to update the WFDocuSign table with the
	XML agreements that have been generated and sent to DocuSign
	"""
	q_update = f"""
    UPDATE WFDocuSign
    SET Status = '{new_status}'
    WHERE ProfileID = {profile_id}
    And Status = '{current_status}'
    """
	con.execute(q_update)


def get_agreements(query_condition, environment: str):
	"""
	This function fitch for new agreements is the Process Table where Status sets to "New"
	Based on the BusinessGroup it searches for its right group module in the other 6 modules in same project directory
	then it passes ProfileID as sys.argv[1] and Environment, database environment,as sys.argv[2]

	Args:
		query_condition (): SQL Query that extract only the (ProfileID and BusinessGroup) of the New agreements'
		environment (str): tst (in Test) or prd (in Production). it is sys.argv[2]

	Returns:
			Generate XML for each agreement and store it in AMS_Trigger/output.
	"""
	try:

		# print(pd.read_sql_query("""select @@version;""", con))
		print("Connected to DB!!!")
		q_processTable = """
		select ProfileID, BusinessGroup, [Status] from WFDocuSign
		Where {};
		""".format(query_condition)
		processTable = pd.read_sql_query(q_processTable, con)
		print('Process table:')
		print(processTable)
		for row in range(len(processTable)):
			removeXMlFiles()

			## extract each row as df from the table
			table = pd.DataFrame(processTable.iloc[[row]])

			# print(f"{table['BusinessGroup'][row]}, {table['ProfileID'][row]}")
			# print('cmd /c "{} {}{}\main.py {} {}"'
			#       .format(python_exe, ScriptDir, table.BusinessGroup[row], processTable.ProfileID[row],
			#               environment))
			print('\n<><><><><><><><><><><><><><><><><><><><><><><><>\n')
			# os.system(f'cmd /c "{ScriptDir}{table.BusinessGroup[row]}\main.py'
			# f' {processTable.ProfileID[row]}"')
			"""Run AlL SCRIPTS"""
			os.system('cmd /c "{} {}{}\main.py {} {}"'
			          .format(python_exe, ScriptDir, table.BusinessGroup[row], processTable.ProfileID[row],
			                  environment))
			print(f'\n**********************************\n '
			      f'    XML Completed {table.BusinessGroup[row]} {processTable.ProfileID[row]} '
			      f'\n**********************************\n')
		return processTable
	except Error as e:
		print("Error: ", e)


def runDocuSignApi(environment: str):
	"""

	IF flier in the API access token:
		1- update Process table to 'Error'
		2- Send Email with Error

	Args:
		environment (str): tst (in Test) or prd (in Production). it is sys.argv[2]
	"""
	access_token = get_access_token(environment)
	for file in os.scandir(output):
		profile_id = Path(file).stem.split('_')
		response = post_xml(filename=file.name, environment=environment, access_token=access_token)

		if response.status in [200, 201]:
			'''
			if status is not [200 or 201]:
				1- update Propcess table to 'Error'
				2- Send Email with Error
			'''
			updateProcessTable(profile_id[1], current_status='XML Format Completed', new_status='Sent to DocuSign')
		else:

			file = open(f"C:/Users/Public/Public Scripts/AMS_CLM_TST/AMS_Trigger/error_output/{file.name}", "w")
			file.write(file)
			file.close()

			updateProcessTable(profile_id[1], current_status='XML Format Completed', new_status='Error')

			SendEmail(filename=file.name, subject='DocuSign - Send XML Error',ToEmail='coramssupport@xxx.com',
			          body=f'<html><h3>The xml file for ProfileID: {file.name} was not sent to DocuSign successfully.'
			               f'<br>Status: {response.status} {response.reason}\n Body:'
			               f' {response.data.decode("utf-8")}'
			               f'<br><br> Environment: {environment}'
			               f'</h3></html>')
		updateProcessTable(profile_id[1], current_status='XML Format Completed', new_status='Error')

		SendEmail(filename=file.name, subject='DocuSign - Send XML Error(2)',ToEmail='coramssupport@xx.com',
		          body=f'<html><h3>'
		               f'The xml file for ProfileID: {file.name} was not sent to DocuSign successfully.'
		               f'<br>Reason: {response.reason}'
		               f'<br><br> Environment: {environment}'
		               f'</h3></html>')


if __name__ == '__main__':
	''' for testing
	## IN TEST:  PDN: 5189 , PFG: 74, eBus: 5847, FCG: 4242, ING: 5324 , MSG:5329, ASEAN: 5582
				5189 ,74,5847,4242,5324 ,5329, 5582
	# "[Status] = 'ForTesting'"
	ING AutoClave Table: 5426 , 5403
	'''
	environment = sys.argv[2]
	# get_agreements(query_condition='ProfileID in (5189,5847,4242,5329)', environment=environment)
	run_get_agreements = get_agreements(query_condition="ProfileID in (5929)", environment=environment)

	# run_get_agreements = get_agreements(query_condition="[Status] = 'New' ", environment=environment)


	### 2 - Copy all the XML from Output to Output_backup dir for backup
	# shutil.copytree(output, f'{output}_backup', dirs_exist_ok=True)

	### 4 - Send all XML to CorAms Support main


	# def check_failed_agreements():
	# 	"""
	# 	check if new agreement did not make it
	# 	"""
	# 	all_files = os.listdir(output)
	# 	run_get_agreements['NewAgreements'] = run_get_agreements["BusinessGroup"] + '_' + run_get_agreements["ProfileID"].astype(str) + '.xml'
	# 	xmls = []
	# 	for xml in run_get_agreements['NewAgreements']:
	# 		if xml not in all_files:
	# 			xmls.append(xml)
	# 	return xmls
	#
	# failed_agreements = check_failed_agreements()
	#
	# if len(failed_agreements) == 0 :
	# 	SendEmail(filename=None, subject='XML Completed',ToEmail='coramssupport@xxx.com',
	# 			  body=f'<html><h3>'
	# 				   f'Attached all the XML agreements that have been successfully generate.'
	# 				   f'<br><br> Environment: dev '
	# 				   f'</h3></html>')
	# else:
	# 	SendEmail(filename='codeError', subject='XML Builder Failed',ToEmail='coramssupport@xx.com',
	# 			  body=f'<html><h3>'
	# 				   f'XML Builder failed to generate these agreements: {failed_agreements} and only the attached agreements was succeeded'
	# 				   f'<br><br> Environment: dev '
	# 				   f'</h3></html>')
	# 	print('Error Email Sent\n')


	### 4 - Send all XML to CorAms Support main
	# SendEmail(filename=None, subject='XML Completed',
	# 		  body=f'<html><h3>'
	# 			   f'Attached all the XML agreements that have been successfully generate.'
	# 			   f'<br><br> Environment: {environment}'
	# 			   f'</h3></html>')


	# runDocuSignApi(environment)

	# print('DocuSign API Is Fired')

	print('####################################')
	print('            * SUCCESSES *           ')
	print('####################################')

