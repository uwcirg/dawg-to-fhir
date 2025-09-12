# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 10:25:13 2023

@author: jsibley
"""

from dotenv import dotenv_values
import logging, pyodbc, requests, simplejson as json

def handle_bit_type(bit_value):
    if bit_value == b'\x00':
        return 0
    if bit_value == b'\x01':
        return 1

def add_update_reference_resource(input_params):
    resource_reference = ""
    return_code = 0
    return_result = {}

    id_system = str(input_params['identifier_system'])
    id_code = str(input_params['identifier_code'])
    resource_type = str(input_params['resource_type'])
    resource_type_lc = resource_type.lower()

    # If resource already added this run, return HAPI ID
    if id_code in reference_resources[resource_type_lc].keys():
        logger.info(resource_type + " ID (" + id_code + ") resource already added, returning HAPI ID (" + str(reference_resources[resource_type_lc][id_code]) + ")...")
        return_result['return_code'] = return_code
        return_result['resource_reference'] = reference_resources[resource_type_lc][id_code]
        return return_result

    # Check if the resource already exists in FHIR store, update if found, insert if not
    fhir_query_response = None
    fhir_query_headers = {'Authorization': fhir_auth_token}
    fhir_query_params = {'identifier': id_system + '|' + id_code}
    fhir_query_response = session.get(fhir_endpoint + '/' + resource_type, headers = fhir_query_headers, params = fhir_query_params)

    logger.debug("FHIR " + resource_type_lc + " query URL: " + fhir_query_response.url)

    if fhir_query_response is not None:
        if fhir_query_response.status_code != 200:
            logger.warning("FHIR " + resource_type_lc + " query failed, status code: " + str(fhir_query_response.status_code))
        else:
            fhir_query_reply = fhir_query_response.json()

            logger.debug("FHIR " + resource_type_lc + " query response: " + json.dumps(fhir_query_reply))

            if fhir_query_reply["total"] > 1:
                logger.critical("Multiple existing resources found with same ID (" + id_code + "), this should never happen... exiting.")
                return_code = 1
            else:
                if fhir_query_reply["total"] == 1:
                    if "entry" in fhir_query_reply:                                     # Existing resource found, update
                        logger.info(resource_type + " ID (" + id_code + ") found in FHIR store, updating...")
                        request_method = "PUT"
                        hapi_id = fhir_query_reply["entry"][0]["resource"]["id"]

                        logger.debug("Existing " + resource_type_lc + " resource found, HAPI ID (" + str(hapi_id) + ")")
                else:                                                                   # No existing resource fouund, insert as new
                    logger.info(resource_type + " ID (" + id_code + ") not found in FHIR store, adding...")
                    request_method = "POST"
                    hapi_id = None

                # Generate FHIR resource from source data using local FUME instance
                fume_post_data = json.dumps({'input': input_params['fume_input_data'],
                                             'fume': input_params['fume_map']})
                
                logger.debug("Calling FUME with: " + fume_post_data)
                
                fume_response = None
                fume_headers = {'Content-type': 'application/json'}
                fume_response = session.post(fume_endpoint, data = fume_post_data, headers = fume_headers)

                logger.debug("FUME " + resource_type_lc + " POST URL: " + fume_response.url)

                if fume_response is not None:

                    logger.debug("FUME " + resource_type_lc + " POST response: " + json.dumps(fume_response.json()))

                    logger.info("Got FHIR data from FUME, sending to FHIR server...")
                    bundle = {
                              "resourceType": "Bundle",
                              "type": "transaction",
                              "entry": []
                             }
                    bundle["entry"].append({"resource": fume_response.json(),
                                            "request": {
                                                        "url": resource_type,
                                                        "method": request_method
                                                       }
                                           })

                    # Send resource to FHIR server
                    fhir_response = None
                    fhir_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                                    'Authorization': fhir_auth_token}
                    if request_method == "POST":
                        fhir_response = session.post(fhir_endpoint, json = bundle, headers = fhir_headers)
                    else:
                        fume_response_json = fume_response.json()
                        fume_response_json["id"] = hapi_id
                        fhir_response = session.put(fhir_endpoint + "/" + resource_type + "/" + hapi_id, json = fume_response_json, headers = fhir_headers)
                    if fhir_response is not None:

                        logger.debug("FHIR " + resource_type_lc + " " + request_method + " URL: " + fhir_response.url)

                        fhir_reply = fhir_response.json()

                        logger.debug("FHIR " + resource_type_lc + " " + request_method + " response: " + json.dumps(fhir_reply))

                        if "entry" in fhir_reply:
                            hapi_id = fhir_reply["entry"][0]["response"]["location"].split("/")[1]
                        if request_method == "POST":
                            action = "added"
                        else:
                            action = "updated"
                        logger.info(resource_type + " ID (" + id_code + ") resource " + action + ", HAPI ID (" + str(hapi_id) + ")...")

                        resource_reference = str(hapi_id)
                        reference_resources[resource_type_lc][id_code] = str(hapi_id)
                    else:
                        logger.critical("Unable to add " + resource_type_lc + " resource with ID (" + id_code + "), exiting.")
                        return_code = 1
                else:
                    logger.critical("No data returned from FUME... exiting.")
                    return_code = 1
    else:
        logger.critical("Unable to query FHIR store for " + resource_type_lc + "s... exiting.")
        return_code = 1
    return_result['return_code'] = return_code
    return_result['resource_reference'] = resource_reference
    return return_result

config_main = dotenv_values("config_main.env")
config_secrets = dotenv_values("config_secrets.env")
config_dawg = dotenv_values("config_dawg.env")
config_fume = dotenv_values("config_fume.env")

logging.basicConfig(
    filename=config_main['LOG_FILE_PATH'],
    filemode='a',
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')

logger = logging.getLogger(__name__)

# Set debug level, anything less than 9 is "info", 9 or greater is "debug"
debug_level = int(config_main['DEBUG_LEVEL'])
if debug_level > 8:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

dawg_server = 'AM-DAWG-SQL-PRT'
dawg_database = 'uwReports'
dawg_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + dawg_server + ';DATABASE=' + dawg_database + ';Trusted_Connection=yes;')
dawg_cnxn.add_output_converter(pyodbc.SQL_BIT, handle_bit_type)

pat_cursor = dawg_cnxn.cursor()
proc_cursor = dawg_cnxn.cursor()
meds_cursor = dawg_cnxn.cursor()
labs_cursor = dawg_cnxn.cursor()
pat_sql = config_dawg['PAT_SQL']

proc_sql = config_dawg['PROC_SQL']

meds_sql = config_dawg['MEDS_SQL']

labs_sql = config_dawg['LABS_SQL']

pat_map = config_fume['FUME_PAT_MAP']

proc_base_map = config_fume['FUME_PROC_BASE_MAP']
proc_enc_map = config_fume['FUME_PROC_ENC_MAP']

meds_base_map = config_fume['FUME_MEDS_BASE_MAP']
meds_dispense_map = config_fume['FUME_MEDS_DISPENSE_MAP']
meds_requester_map = config_fume['FUME_MEDS_REQUESTER_MAP']
meds_enc_map = config_fume['FUME_MEDS_ENC_MAP']

labs_base_map = config_fume['FUME_LABS_BASE_MAP']
labs_requester_map = config_fume['FUME_LABS_REQUESTER_MAP']
labs_enc_map = config_fume['FUME_LABS_ENC_MAP']

practitioner_base_map = config_fume['FUME_PRACTITIONER_BASE_MAP']
practitioner_npi_map = config_fume['FUME_PRACTITIONER_NPI_MAP']

location_map = config_fume['FUME_LOCATION_MAP']

encounter_base_map = config_fume['FUME_ENCOUNTER_BASE_MAP']
encounter_location_map = config_fume['FUME_ENCOUNTER_LOCATION_MAP']

fhir_endpoint = config_main['FHIR_ENDPOINT']
fhir_auth_token = config_secrets['FHIR_AUTH_TOKEN']
fume_endpoint = config_fume['FUME_ENDPOINT']

# Get patient data, process 
pat_cursor.execute(pat_sql)
pat_vals = pat_cursor.fetchall()
pat_cols = [column[0] for column in pat_cursor.description]

# Get procedure data, store for later use (this speeds things up considerably over querying individually per patient)
proc_data = {}
if config_main["INCLUDE_PROCEDURES"] == '1':
    proc_cursor.execute(proc_sql)
    proc_vals = proc_cursor.fetchall()
    proc_cols = [column[0] for column in proc_cursor.description]
    # TODO: Fix this, there must be a way to do this in a single loop, but not finding it now
    for proc_row in proc_vals:
        proc_data[proc_row[0]] = []
    for proc_row in proc_vals:
        proc_data[proc_row[0]].append(dict(zip(proc_cols, proc_row)))

# Get medication data, store for later use (this speeds things up considerably over querying individually per patient)
meds_data = {}
if config_main["INCLUDE_MEDICATIONS"] == '1':
    meds_cursor.execute(meds_sql)
    meds_vals = meds_cursor.fetchall()
    meds_cols = [column[0] for column in meds_cursor.description]
    # TODO: Fix this, there must be a way to do this in a single loop, but not finding it now
    for meds_row in meds_vals:
        meds_data[meds_row[0]] = []
    for meds_row in meds_vals:
        meds_data[meds_row[0]].append(dict(zip(meds_cols, meds_row)))

labs_data = {}
if config_main["INCLUDE_LAB_ORDERS"] == '1':
    labs_cursor.execute(labs_sql)
    labs_vals = labs_cursor.fetchall()
    labs_cols = [column[0] for column in labs_cursor.description]
    # TODO: Fix this, there must be a way to do this in a single loop, but not finding it now
    for labs_row in labs_vals:
        labs_data[labs_row[0]] = []
    for labs_row in labs_vals:
        labs_data[labs_row[0]].append(dict(zip(labs_cols, labs_row)))

logger.info("=========================== STARTING DAILY RUN =============================")

pat_cnt = 0
proc_cnt = 0
proc_del_cnt = 0
meds_cnt = 0
meds_del_cnt = 0
labs_cnt = 0
labs_del_cnt = 0

reference_resources = {'location': {},
                       'encounter': {},
                       'practitioner': {}
                      }

# Open a session to the FHIR endpoint instead of making individual calls as this speeds things up significantly
session = requests.Session()

for pat_row in pat_vals:
    patient_request_method = ""
    updated_pat_map = None
    continue_flag = True

    pat_data = dict(zip(pat_cols, pat_row))

    # Check if patient already exists in FHIR store, update if found, insert if not
    fhir_query_response = None
    fhir_query_headers = {'Authorization': fhir_auth_token}
    fhir_query_params = {'identifier': 'uwDAL_Clarity|' + str(pat_data['pat_id']) + ',http://www.uwmedicine.org/epic_patient_id|' + str(pat_data['pat_id']), 'active': 'true'}
    fhir_query_response = session.get(fhir_endpoint + '/Patient', headers = fhir_query_headers, params = fhir_query_params)

    logger.debug("FHIR patient query URL: " + fhir_query_response.url)

    if fhir_query_response is not None:
        if fhir_query_response.status_code != 200:
            logger.critical("FHIR patient query failed, status code: " + str(fhir_query_response.status_code) + ", exiting...")
            continue_flag = False
        else:
            fhir_query_reply = fhir_query_response.json()
    
            logger.debug("FHIR patient query response: " + json.dumps(fhir_query_reply))
        
            if fhir_query_reply["total"] > 1:
                logger.critical("Multiple existing patients found with same ID (" + str(pat_data['pat_id']) + "), this should never happen... exiting.")
                continue_flag = False
            else:
                if fhir_query_reply["total"] == 1:
                    if "entry" in fhir_query_reply:                                     # Existing patient found, update
                        logger.info("Patient ID (" + str(pat_data['pat_id']) + ") found in FHIR store, updating...")
                        patient_request_method = "PUT"
                        patient_hapi_id = fhir_query_reply["entry"][0]["resource"]["id"]
    
                        logger.debug("Existing patient resource found, HAPI ID (" + str(patient_hapi_id) + ")")
                    
                        # Need to pull any existing identifiers (except 'epic_patient_id' and 'mrn') out of the existing patient resource to add them to the update bundle
                        addl_identifiers = {}
                        for identifier in fhir_query_reply["entry"][0]["resource"]["identifier"]:
                            if identifier["system"] in ["http://www.uwmedicine.org/mrn", "http://www.uwmedicine.org/epic_patient_id"]:
                                continue
    
                            logger.debug("Adding existing identifier to updated patient resource bundle: " + identifier["system"] + "|" + identifier["value"])
                        
                            updated_pat_map = pat_map + """
  * identifier
    * system = '""" + identifier["system"] + """'
    * value = \"""" + identifier["value"] + """\"
    """
                else:                                                                   # Patient not fouund, insert as new
                    logger.info("Patient ID (" + str(pat_data['pat_id']) + ") not found in FHIR store, adding...")
                    patient_request_method = "POST"           
                    patient_hapi_id = None
                
                # Generate FHIR patient resource from source data using local FUME instance
                if updated_pat_map is not None:
                    fume_post_data = json.dumps({'input': pat_data,
                                                 'fume': updated_pat_map})
                else:
                    fume_post_data = json.dumps({'input': pat_data,
                                                 'fume': pat_map})
                fume_patient_response = None
                fume_patient_headers = {'Content-type': 'application/json'}
                fume_patient_response = session.post(fume_endpoint, data = fume_post_data, headers = fume_patient_headers)
    
                logger.debug("FUME patient POST URL: " + fume_patient_response.url)
            
                if fume_patient_response is not None:
    
                    logger.debug("FUME patient POST response: " + json.dumps(fume_patient_response.json()))
                
                    logger.info("Got FHIR data from FUME, sending to FHIR server...")
                    pat_bundle = {
                                  "resourceType": "Bundle",
                                  "type": "transaction",
                                  "entry": []
                                 }
                    pat_bundle["entry"].append({"resource": fume_patient_response.json(),
                                                "request": {
                                                    "url": "Patient",
                                                    "method": patient_request_method
                                                   }
                                              })
                    
                    # Send patient resource to FHIR server
                    fhir_patient_response = None
                    fhir_patient_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                            'Authorization': fhir_auth_token}
                    if patient_request_method == "POST":
                        fhir_patient_response = session.post(fhir_endpoint, json = pat_bundle, headers = fhir_patient_headers)
                    else:
                        fume_patient_response_json = fume_patient_response.json()
                        fume_patient_response_json["id"] = patient_hapi_id
                        fhir_patient_response = session.put(fhir_endpoint + "/Patient/" + patient_hapi_id, json = fume_patient_response_json, headers = fhir_patient_headers)
                    if fhir_patient_response is not None:
    
                        logger.debug("FHIR patient " + patient_request_method + " URL: " + fhir_patient_response.url)
                    
                        fhir_patient_reply = fhir_patient_response.json()
    
                        logger.debug("FHIR patient " + patient_request_method + " response: " + json.dumps(fhir_patient_reply))
                    
                        if "entry" in fhir_patient_reply:
                            patient_hapi_id = fhir_patient_reply["entry"][0]["response"]["location"].split("/")[1]
                        if patient_request_method == "POST":
                            patient_action = "added"
                        else:
                            patient_action = "updated"
                        logger.info("Patient ID (" + str(pat_data['pat_id']) + ") resource " + patient_action + ", HAPI ID (" + str(patient_hapi_id) + ")...")
                        pat_cnt = pat_cnt + 1
                        
                        # If configured, process procedures for patient and insert, update, delete as needed
                        if continue_flag and config_main["INCLUDE_PROCEDURES"] == '1':
                            # Pull all existing procedures for patient from the FHIR store
                            fhir_proc_query_response = None
                            fhir_proc_query_headers = {'Authorization': fhir_auth_token}
                            fhir_proc_query_params = {'subject': 'Patient/' + str(patient_hapi_id)}
                            fhir_proc_query_response = session.get(fhir_endpoint + '/Procedure', headers = fhir_proc_query_headers, params = fhir_proc_query_params)
    
                            logger.debug("FHIR procedure query URL: " + fhir_proc_query_response.url)
                        
                            if fhir_proc_query_response is not None:
                                if fhir_proc_query_response.status_code != 200:
                                    logger.critical("FHIR procedure query failed, status code: " + str(fhir_proc_query_response.status_code) + ", exiting...")
                                    continue_flag = False
                                    
                                    logger.debug("FHIR procedure query response: " + json.dumps(fhir_proc_query_response.json()))
    
                                    break
                                else:
                                    fhir_proc_query_reply = fhir_proc_query_response.json()
                                    
                                    logger.debug("FHIR procedure query response: " + json.dumps(fhir_proc_query_reply))
    
                                    existing_fhir_proc_ids = {}
                                    if "entry" in fhir_proc_query_reply:
                                        for l in range(0, len(fhir_proc_query_reply["entry"])):
                                            proc = fhir_proc_query_reply["entry"][l]
                                            existing_fhir_proc_ids[str(proc["resource"]["identifier"][0]["value"])] = str(proc["resource"]["id"])
    
                                    dawg_proc_ids = []
                                    if str(pat_data['pat_id']) in proc_data.keys():
                                        logger.info("Processing procedure data for patient...")
                                        for proc_row in proc_data[str(pat_data['pat_id'])]:
                                            proc_request_method = ""
                                            proc_hapi_id = None
        
                                            # insert/update location resource to reference from procedure resource
                                            location_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_department_id',
                                                                                             'identifier_code': proc_row['visit_dept_id'],
                                                                                             'resource_type': "Location",
                                                                                             'fume_input_data': proc_row,
                                                                                             'fume_map': location_map
                                                                                            })

                                            # insert/update encounter resource to reference from procedure resource
                                            encounter_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_encounter_id',
                                                                                              'identifier_code': proc_row['enc_id'],
                                                                                              'resource_type': "Encounter",
                                                                                              'fume_input_data': proc_row,
                                                                                              'fume_map': encounter_base_map + "'Patient/" + str(patient_hapi_id) + "'" + encounter_location_map + "'Location/" + str(location_result['resource_reference']) + "'"
                                                                                             })

                                            if location_result['return_code'] == 0 and encounter_result['return_code'] == 0:
                                                dawg_proc_ids.append(str(proc_row['uniq_id']))
                                                post_data = json.dumps({'input': proc_row,
                                                                        'fume': proc_base_map + "'Patient/" + str(patient_hapi_id) + "'" + proc_enc_map + "'Encounter/" + encounter_result['resource_reference'] + "'"})

                                                if str(proc_row['uniq_id']) in existing_fhir_proc_ids.keys():
                                                    logger.info("Existing procedure found, upadating...")
                                                    proc_request_method = "PUT"
                                                    proc_hapi_id = existing_fhir_proc_ids[str(proc_row['uniq_id'])]

                                                    logger.debug("Existing procedure resource found, HAPI ID (" + str(proc_hapi_id) + ")")

                                                else:
                                                    proc_request_method = 'POST'
                                                    proc_hapi_id = None
            
                                                fume_proc_response = None
                                                fume_proc_headers = {'Content-type': 'application/json'}
                                                fume_proc_response = session.post(fume_endpoint, data = post_data, headers = fume_proc_headers)

                                                logger.debug("FUME procedure POST URL: " + fume_proc_response.url)
                                        
                                                if fume_proc_response is not None:
                                                    logger.info("Got FHIR data from FUME, sending to FHIR server...")

                                                    logger.debug("FUME procedure POST response: " + json.dumps(fume_proc_response.json()))


                                                    proc_bundle = {
                                                                  "resourceType": "Bundle",
                                                                  "type": "transaction",
                                                                  "entry": []
                                                                }
                                                    proc_bundle["entry"].append({"resource": fume_proc_response.json(),
                                                                                "request": {
                                                                                    "url": "Procedure",
                                                                                    "method": proc_request_method
                                                                                   }
                                                                              })

                                                    fhir_proc_response = None
                                                    fhir_proc_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                                                         'Authorization': fhir_auth_token}
                                                    if proc_request_method == "POST":
                                                        fhir_proc_response = session.post(fhir_endpoint, json = proc_bundle, headers = fhir_proc_headers)
                                                    else:
                                                        fume_proc_response_json = fume_proc_response.json()
                                                        fume_proc_response_json["id"] = proc_hapi_id
                                                        fhir_proc_response = session.put(fhir_endpoint + "/Procedure/" + proc_hapi_id, json = fume_proc_response_json, headers = fhir_proc_headers)

                                                    logger.debug("FHIR procedure " + proc_request_method + " URL: " + fhir_proc_response.url)

                                                    if fhir_proc_response is not None:
                                                        fhir_proc_reply = fhir_proc_response.json()

                                                        logger.debug("FHIR procedure " + proc_request_method + " response: " + json.dumps(fhir_proc_reply))

                                                        if "entry" in fhir_proc_reply:
                                                            proc_hapi_id = fhir_proc_reply["entry"][0]["response"]["location"].split("/")[1]
                                                        if proc_request_method == "POST":
                                                            proc_action = "added"
                                                        else:
                                                            proc_action = "updated"
                                                        logger.info("Procedure ID (" + str(proc_row['uniq_id']) + ") resource " + proc_action + ", HAPI ID (" + str(proc_hapi_id) + ")...")
                                                        proc_cnt = proc_cnt + 1
                                                    else:
                                                        logger.warning("Unable to add procedure resource with ID (" + str(proc_row['uniq_id']) + "), skipping...")
                                                else:
                                                    logger.critical("No data returned from FUME... exiting.")
                                                    continue_flag = False
                                            else:
                                                logger.critical("Error code returned from called subroutine... exiting.")
                                                continue_flag = False
                                    else:
                                        logger.info("No procedures found for patient, skipping...")

                                    # Delete any existing FHIR procedure resources not found in the current list of patient procedures from the DAWG
                                    if continue_flag:
                                        for proc_id in list(set(existing_fhir_proc_ids.keys()).difference(dawg_proc_ids)):
                                            fhir_proc_del_response = None
                                            fhir_proc_del_response = session.delete(fhir_endpoint + "/Procedure/" + existing_fhir_proc_ids[proc_id])

                                            logger.debug("FHIR procedure DELETE URL: " + fhir_proc_del_response.url)
                                    
                                            if fhir_proc_del_response is not None:

                                                if fhir_proc_del_response.headers["content-type"].strip().startswith("application/json"):
                                                    logger.debug("FHIR procedure DELETE response: " + json.dumps(fhir_proc_del_response.json()))
                                                else:
                                                    logger.debug("FHIR procedure DELETE response: " + fhir_proc_del_response.text)

                                                logger.info("Procedure ID (" + str(proc_id) + ") resource deleted, HAPI ID (" + str(existing_fhir_proc_ids[proc_id]) + ")...")
                                                proc_del_cnt = proc_del_cnt + 1
                                    else:
                                        logger.critical("Prior error condition met... exiting.")
                                        continue_flag = False
                            else:
                                logger.critical("Unable to query FHIR store for procedures... exiting.")
                                continue_flag = False
                        else:
                            if continue_flag:
                                logger.info("Not processing procedures due to coniguration setting...")
                            else:
                                logger.critical("Prior error condition met... exiting.")
                                continue_flag = False

                        # If configured, process medications for patient and insert, update, delete as needed
                        if continue_flag and config_main["INCLUDE_MEDICATIONS"] == '1':
                            # Pull all existing medications for patient from the FHIR store
                            fhir_meds_query_response = None
                            fhir_meds_query_headers = {'Authorization': fhir_auth_token}
                            fhir_meds_query_params = {'subject': 'Patient/' + str(patient_hapi_id)}
                            fhir_meds_query_response = session.get(fhir_endpoint + '/MedicationRequest', headers = fhir_meds_query_headers, params = fhir_meds_query_params)

                            logger.debug("FHIR medications query URL: " + fhir_meds_query_response.url)

                            if fhir_meds_query_response is not None:
                                if fhir_meds_query_response.status_code != 200:
                                    logger.critical("FHIR medications query failed, status code: " + str(fhir_meds_query_response.status_code) + ", exiting...")
                                    continue_flag = False

                                    logger.debug("FHIR medications query response: " + json.dumps(fhir_meds_query_response.json()))

                                else:
                                    fhir_meds_query_reply = fhir_meds_query_response.json()
                                    logger.debug("FHIR medications query response: " + json.dumps(fhir_meds_query_reply))

                                    existing_fhir_meds_ids = {}
                                    if "entry" in fhir_meds_query_reply:
                                        for l in range(0, len(fhir_meds_query_reply["entry"])):
                                            med = fhir_meds_query_reply["entry"][l]
                                            existing_fhir_meds_ids[str(med["resource"]["identifier"][0]["value"])] = str(med["resource"]["id"])

                                    dawg_meds_ids = []
                                    if str(pat_data['pat_id']) in meds_data.keys():
                                        logger.info("Processing medication data for patient...")
                                        for meds_row in meds_data[str(pat_data['pat_id'])]:
                                            meds_request_method = ""
                                            meds_hapi_id = None

                                            # insert/update location resource to reference from medication resource
                                            location_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_department_id',
                                                                                             'identifier_code': meds_row['visit_dept_id'],
                                                                                             'resource_type': "Location",
                                                                                             'fume_input_data': meds_row,
                                                                                             'fume_map': location_map
                                                                                            })

                                            # insert/update encounter resource to reference from medication resource
                                            encounter_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_encounter_id',
                                                                                              'identifier_code': meds_row['enc_id'],
                                                                                              'resource_type': "Encounter",
                                                                                              'fume_input_data': meds_row,
                                                                                              'fume_map': encounter_base_map + "'Patient/" + str(patient_hapi_id) + "'" + encounter_location_map + "'Location/" + str(location_result['resource_reference']) + "'"
                                                                                             })

                                            # insert/update practitioner resource to reference from medication resource
                                            if meds_row['provider_id'] != "-1":
                                                adjusted_practitioner_map = practitioner_base_map
                                                if meds_row['npi'] is not None:
                                                    adjusted_practitioner_map = practitioner_base_map + practitioner_npi_map

                                                practitioner_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_provider_id',
                                                                                                     'identifier_code': meds_row['provider_id'],
                                                                                                     'resource_type': "Practitioner",
                                                                                                     'fume_input_data': meds_row,
                                                                                                     'fume_map': adjusted_practitioner_map
                                                                                                    })
                                            else:
                                                practitioner_result = {'return_code': 0}

                                            if location_result['return_code'] == 0 and encounter_result['return_code'] == 0 and practitioner_result['return_code'] == 0:
                                                dawg_meds_ids.append(str(meds_row['uniq_id']))

                                                adjusted_meds_map = meds_base_map + "'Patient/" + str(patient_hapi_id) + "'" + meds_enc_map + "'Encounter/" + str(encounter_result['resource_reference']) + "'"

                                                if meds_row['provider_id'] != "-1":
                                                    adjusted_meds_map +=  meds_requester_map + "'Practitioner/" + str(practitioner_result['resource_reference']) + "'"

                                                if meds_row['quantity'] > 0:
                                                    adjusted_meds_map += meds_dispense_map

                                                post_data = json.dumps({'input': meds_row,
                                                                        'fume': adjusted_meds_map})

                                                if str(meds_row['uniq_id']) in existing_fhir_meds_ids.keys():
                                                    logger.info("Existing medication found, upadating...")
                                                    meds_request_method = "PUT"
                                                    meds_hapi_id = existing_fhir_meds_ids[str(meds_row['uniq_id'])]

                                                    logger.debug("Existing medication resource found, HAPI ID (" + str(meds_hapi_id) + ")")

                                                else:
                                                    meds_request_method = 'POST'
                                                    meds_hapi_id = None

                                                fume_meds_response = None
                                                fume_meds_headers = {'Content-type': 'application/json'}
                                                fume_meds_response = session.post(fume_endpoint, data = post_data, headers = fume_meds_headers)

                                                logger.debug("FUME medication POST URL: " + fume_meds_response.url)

                                                if fume_meds_response is not None:
                                                    logger.info("Got FHIR data from FUME, sending to FHIR server...")

                                                    logger.debug("FUME medication POST response: " + json.dumps(fume_meds_response.json()))


                                                    meds_bundle = {
                                                                  "resourceType": "Bundle",
                                                                  "type": "transaction",
                                                                  "entry": []
                                                                 }
                                                    meds_bundle["entry"].append({"resource": fume_meds_response.json(),
                                                                                "request": {
                                                                                    "url": "MedicationRequest",
                                                                                    "method": meds_request_method
                                                                                   }
                                                                              })
                                                    fhir_meds_response = None
                                                    fhir_meds_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                                                         'Authorization': fhir_auth_token}
                                                    if meds_request_method == "POST":
                                                        fhir_meds_response = session.post(fhir_endpoint, json = meds_bundle, headers = fhir_meds_headers)
                                                    else:
                                                        fume_meds_response_json = fume_meds_response.json()
                                                        fume_meds_response_json["id"] = meds_hapi_id
                                                        fhir_meds_response = session.put(fhir_endpoint + "/MedicationRequest/" + meds_hapi_id, json = fume_meds_response_json, headers = fhir_meds_headers)

                                                    logger.debug("FHIR medication " + meds_request_method + " URL: " + fhir_meds_response.url)

                                                    if fhir_meds_response is not None:
                                                        fhir_meds_reply = fhir_meds_response.json()

                                                        logger.debug("FHIR medication " + meds_request_method + " response: " + json.dumps(fhir_meds_reply))

                                                        if "entry" in fhir_meds_reply:
                                                            meds_hapi_id = fhir_meds_reply["entry"][0]["response"]["location"].split("/")[1]
                                                        if meds_request_method == "POST":
                                                            meds_action = "added"
                                                        else:
                                                            meds_action = "updated"
                                                        logger.info("MedicationRequest ID (" + str(meds_row['uniq_id']) + ") resource " + meds_action + ", HAPI ID (" + str(meds_hapi_id) + ")...")
                                                        meds_cnt = meds_cnt + 1
                                                    else:
                                                        logger.warning("Unable to add medication resource with ID (" + str(meds_row['uniq_id']) + "), skipping...")
                                                else:
                                                    logger.critical("No data returned from FUME... exiting.")
                                                    continue_flag = False
                                            else:
                                                logger.critical("Error code returned from called subroutine... exiting.")
                                                continue_flag = False
                                    else:
                                        logger.info("No medications found for patient, skipping...")

                                    # Delete any existing FHIR medication resources not found in the current list of patient medications from the DAWG
                                    if continue_flag:
                                        for meds_id in list(set(existing_fhir_meds_ids.keys()).difference(dawg_meds_ids)):
                                            fhir_meds_del_response = None
                                            fhir_meds_del_response = session.delete(fhir_endpoint + "/MedicationRequest/" + existing_fhir_meds_ids[meds_id])

                                            logger.debug("FHIR medication DELETE URL: " + fhir_meds_del_response.url)

                                            if fhir_meds_del_response is not None:

                                                if fhir_meds_del_response.headers["content-type"].strip().startswith("application/json"):
                                                    logger.debug("FHIR medication DELETE response: " + json.dumps(fhir_meds_del_response.json()))
                                                else:
                                                    logger.debug("FHIR medication DELETE response: " + fhir_meds_del_response.text)

                                                logger.info("MedicationRequest ID (" + str(meds_id) + ") resource deleted, HAPI ID (" + str(existing_fhir_meds_ids[meds_id]) + ")...")
                                                meds_del_cnt = meds_del_cnt + 1
                                    else:
                                        logger.critical("Prior error condition met... exiting.")
                                        continue_flag = False
                            else:
                                logger.critical("Unable to query FHIR store for medications... exiting.")
                                continue_flag = False
                        else:
                            if continue_flag:
                                logger.info("Not processing medications due to coniguration setting...")
                            else:
                                logger.critical("Prior error condition met... exiting.")
                                continue_flag = False

                        # If configured, process lab orders for patient and insert, update, delete as needed
                        if continue_flag and config_main["INCLUDE_LAB_ORDERS"] == '1':
                            # Pull all existing lab orders for patient from the FHIR store
                            fhir_labs_query_response = None
                            fhir_labs_query_headers = {'Authorization': fhir_auth_token}
                            fhir_labs_query_params = {'subject': 'Patient/' + str(patient_hapi_id),
                                                     "identifier": 'http://www.uwmedicine.org/lab_order_id|'}
                            fhir_labs_query_response = session.get(fhir_endpoint + '/ServiceRequest', headers = fhir_labs_query_headers, params = fhir_labs_query_params)

                            logger.debug("FHIR lab orders query URL: " + fhir_labs_query_response.url)

                            if fhir_labs_query_response is not None:
                                if fhir_labs_query_response.status_code != 200:
                                    logger.critical("FHIR lab orders query failed, status code: " + str(fhir_labs_query_response.status_code) + ", exiting...")
                                    continue_flag = False

                                    logger.debug("FHIR lab orders query response: " + json.dumps(fhir_labs_query_response.json()))

                                else:
                                    fhir_labs_query_reply = fhir_labs_query_response.json()
                                    logger.debug("FHIR lab orders query response: " + json.dumps(fhir_labs_query_reply))

                                    existing_fhir_labs_ids = {}
                                    if "entry" in fhir_labs_query_reply:
                                        for l in range(0, len(fhir_labs_query_reply["entry"])):
                                            lab = fhir_labs_query_reply["entry"][l]
                                            existing_fhir_labs_ids[str(lab["resource"]["identifier"][0]["value"])] = str(lab["resource"]["id"])

                                    dawg_labs_ids = []
                                    if str(pat_data['pat_id']) in labs_data.keys():
                                        logger.info("Processing lab order data for patient...")
                                        for labs_row in labs_data[str(pat_data['pat_id'])]:
                                            labs_request_method = ""
                                            labs_hapi_id = None

                                            # insert/update location resource to reference from lab order resource
                                            location_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_department_id',
                                                                                             'identifier_code': labs_row['visit_dept_id'],
                                                                                             'resource_type': "Location",
                                                                                             'fume_input_data': labs_row,
                                                                                             'fume_map': location_map
                                                                                            })

                                            # insert/update encounter resource to reference from lab order resource
                                            encounter_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_encounter_id',
                                                                                              'identifier_code': labs_row['enc_id'],
                                                                                              'resource_type': "Encounter",
                                                                                              'fume_input_data': labs_row,
                                                                                              'fume_map': encounter_base_map + "'Patient/" + str(patient_hapi_id) + "'" + encounter_location_map + "'Location/" + str(location_result['resource_reference']) + "'"
                                                                                             })

                                            # insert/update practitioner resource to reference from lab order resource
                                            if labs_row['provider_id'] != "-1":
                                                adjusted_practitioner_map = practitioner_base_map
                                                if labs_row['npi'] is not None:
                                                    adjusted_practitioner_map = practitioner_base_map + practitioner_npi_map

                                                practitioner_result = add_update_reference_resource({'identifier_system': 'http://www.uwmedicine.org/epic_provider_id',
                                                                                                     'identifier_code': labs_row['provider_id'],
                                                                                                     'resource_type': "Practitioner",
                                                                                                     'fume_input_data': labs_row,
                                                                                                     'fume_map': adjusted_practitioner_map
                                                                                                    })
                                            else:
                                                practitioner_result = {'return_code': 0}

                                            if location_result['return_code'] == 0 and encounter_result['return_code'] == 0 and practitioner_result['return_code'] == 0:
                                                dawg_labs_ids.append(str(labs_row['uniq_id']))

                                                adjusted_labs_map = labs_base_map + "'Patient/" + str(patient_hapi_id) + "'" + labs_enc_map + "'Encounter/" + str(encounter_result['resource_reference']) + "'"

                                                if labs_row['provider_id'] != "-1":
                                                    adjusted_labs_map +=  labs_requester_map + "'Practitioner/" + str(practitioner_result['resource_reference']) + "'"

                                                post_data = json.dumps({'input': labs_row,
                                                                        'fume': adjusted_labs_map})

                                                if str(labs_row['uniq_id']) in existing_fhir_labs_ids.keys():
                                                    logger.info("Existing lab order found, upadating...")
                                                    labs_request_method = "PUT"
                                                    labs_hapi_id = existing_fhir_labs_ids[str(labs_row['uniq_id'])]

                                                    logger.debug("Existing lab order resource found, HAPI ID (" + str(labs_hapi_id) + ")")

                                                else:
                                                    labs_request_method = 'POST'
                                                    labs_hapi_id = None

                                                fume_labs_response = None
                                                fume_labs_headers = {'Content-type': 'application/json'}
                                                fume_labs_response = session.post(fume_endpoint, data = post_data, headers = fume_labs_headers)

                                                logger.debug("FUME lab order POST URL: " + fume_labs_response.url)

                                                if fume_labs_response is not None:
                                                    logger.info("Got FHIR data from FUME, sending to FHIR server...")

                                                    logger.debug("FUME lab order POST response: " + json.dumps(fume_labs_response.json()))


                                                    labs_bundle = {
                                                                  "resourceType": "Bundle",
                                                                  "type": "transaction",
                                                                  "entry": []
                                                                 }
                                                    labs_bundle["entry"].append({"resource": fume_labs_response.json(),
                                                                                "request": {
                                                                                    "url": "ServiceRequest",
                                                                                    "method": labs_request_method
                                                                                   }
                                                                              })
                                                    fhir_labs_response = None
                                                    fhir_labs_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                                                         'Authorization': fhir_auth_token}
                                                    if labs_request_method == "POST":
                                                        fhir_labs_response = session.post(fhir_endpoint, json = labs_bundle, headers = fhir_labs_headers)
                                                    else:
                                                        fume_labs_response_json = fume_labs_response.json()
                                                        fume_labs_response_json["id"] = labs_hapi_id
                                                        fhir_labs_response = session.put(fhir_endpoint + "/ServiceRequest/" + labs_hapi_id, json = fume_labs_response_json, headers = fhir_labs_headers)

                                                    logger.debug("FHIR lab order " + labs_request_method + " URL: " + fhir_labs_response.url)

                                                    if fhir_labs_response is not None:
                                                        fhir_labs_reply = fhir_labs_response.json()

                                                        logger.debug("FHIR lab order " + labs_request_method + " response: " + json.dumps(fhir_labs_reply))

                                                        if "entry" in fhir_labs_reply:
                                                            labs_hapi_id = fhir_labs_reply["entry"][0]["response"]["location"].split("/")[1]
                                                        if labs_request_method == "POST":
                                                            labs_action = "added"
                                                        else:
                                                            labs_action = "updated"
                                                        logger.info("ServiceRequest ID (" + str(labs_row['uniq_id']) + ") resource " + labs_action + ", HAPI ID (" + str(labs_hapi_id) + ")...")
                                                        labs_cnt = labs_cnt + 1
                                                    else:
                                                        logger.warning("Unable to add lab order resource with ID (" + str(labs_row['uniq_id']) + "), skipping...")
                                                else:
                                                    logger.critical("No data returned from FUME... exiting.")
                                                    continue_flag = False
                                            else:
                                                logger.critical("Error code returned from called subroutine... exiting.")
                                                continue_flag = False
                                    else:
                                        logger.info("No lab orders found for patient, skipping...")

                                    # Delete any existing FHIR lab order resources not found in the current list of patient lab orders from the DAWG
                                    if continue_flag:
                                        for labs_id in list(set(existing_fhir_labs_ids.keys()).difference(dawg_labs_ids)):
                                            fhir_labs_del_response = None
                                            fhir_labs_del_response = session.delete(fhir_endpoint + "/ServiceRequest/" + existing_fhir_labs_ids[labs_id])

                                            logger.debug("FHIR lab order DELETE URL: " + fhir_labs_del_response.url)

                                            if fhir_labs_del_response is not None:

                                                if fhir_labs_del_response.headers["content-type"].strip().startswith("application/json"):
                                                    logger.debug("FHIR lab order DELETE response: " + json.dumps(fhir_labs_del_response.json()))
                                                else:
                                                    logger.debug("FHIR lab order DELETE response: " + fhir_labs_del_response.text)

                                                logger.info("ServiceRequest ID (" + str(labs_id) + ") resource deleted, HAPI ID (" + str(existing_fhir_labs_ids[labs_id]) + ")...")
                                                labs_del_cnt = labs_del_cnt + 1
                                    else:
                                        logger.critical("Prior error condition met... exiting.")
                                        continue_flag = False
                            else:
                                logger.critical("Unable to query FHIR store for lab orders... exiting.")
                                continue_flag = False
                        else:
                            if continue_flag:
                                logger.info("Not processing medications due to coniguration setting...")
                            else:
                                logger.critical("Prior error condition met... exiting.")
                                continue_flag = False
                    else:
                        logger.warning("Unable to add patient resource with ID (" + str(pat_data["pat_id"]) + "), skipping...")
                else:
                    logger.critical("ERROR: No data returned from FUME... exiting.")
                    continue_flag = False
    else:
        logger.critical("Unable to query FHIR store for patients... exiting.")
        continue_flag = False

    if not continue_flag:
        break

logger.info("Total patients added/updated: " + str(pat_cnt))
logger.info("Total procedures added/updated: " + str(proc_cnt))
logger.info("Total procedures deleted: " + str(proc_del_cnt))
logger.info("Total medications added/updated: " + str(meds_cnt))
logger.info("Total medications deleted: " + str(meds_del_cnt))
logger.info("Total lab orders added/updated: " + str(labs_cnt))
logger.info("Total lab orders deleted: " + str(labs_del_cnt))
logger.info("Total locations added/updated: " + str(len(reference_resources['location'].keys())))
logger.info("Total encounters added/updated: " + str(len(reference_resources['encounter'].keys())))
logger.info("Total practitioners added/updated: " + str(len(reference_resources['practitioner'].keys())))

pat_cursor.close()
proc_cursor.close()

logger.info("=========================== FINISH DAILY RUN =============================")
