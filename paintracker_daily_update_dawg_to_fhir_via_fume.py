# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 10:25:13 2023

@author: jsibley
"""

from dotenv import dotenv_values
from os.path import exists
import datetime, os, pathlib, pyodbc, re, requests, signal, simplejson as json, subprocess, time

def handle_bit_type(bit_value):
    if bit_value == b'\x00':
        return 0
    if bit_value == b'\x01':
        return 1

def log_it(message):
    LOG_FILE.write("[" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] ")
    LOG_FILE.write(message + "\n")

now = datetime.datetime.now()

config = dotenv_values("config.env")

LOG_FILE = open(config['LOG_FILE_PATH'], "a", encoding="utf-8")

dawg_server = 'AM-DAWG-SQL-PRT'
dawg_database = 'uwReports'
dawg_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + dawg_server + ';DATABASE=' + dawg_database + ';Trusted_Connection=yes;')
dawg_cnxn.add_output_converter(pyodbc.SQL_BIT, handle_bit_type)

pat_cursor = dawg_cnxn.cursor()
proc_cursor = dawg_cnxn.cursor()
pat_sql = config['PAT_SQL']

proc_sql = config['PROC_SQL']

pat_map = config['FUME_PAT_MAP']

proc_map = config['FUME_PROC_MAP']

fhir_endpoint = config['FHIR_ENDPOINT']
fhir_auth_token = config['FHIR_AUTH_TOKEN']
fume_endpoint = config['FUME_ENDPOINT']

# Get patient data, process 
pat_cursor.execute(pat_sql)
pat_vals = pat_cursor.fetchall()
pat_cols = [column[0] for column in pat_cursor.description]

# Get procedure data, store for later use (this speeds things up considerably over querying individually per patient)
proc_data = {}
if config["INCLUDE_PROCEDURES"] == '1':
    proc_cursor.execute(proc_sql)
    proc_vals = proc_cursor.fetchall()
    proc_cols = [column[0] for column in proc_cursor.description]
    # TODO: Fix this, there must be a way to do this in a single loop, but not finding it now
    for proc_row in proc_vals:
        proc_data[proc_row[0]] = []
    for proc_row in proc_vals:
        proc_data[proc_row[0]].append(dict(zip(proc_cols, proc_row)))

log_it("=========================== STARTING DAILY RUN =============================")

# Set debug level, anything less than 9 is "info/warning", 9 or greater is "debug"
debug_level = config['DEBUG_LEVEL']

pat_cnt = 0
proc_cnt = 0
proc_del_cnt = 0

# Open a session to the FHIR endpoint instead of making individual calls as this speeds things up significantly
session = requests.Session()

for pat_row in pat_vals:
    patient_request_method = ""
    updated_pat_map = None

    pat_data = dict(zip(pat_cols, pat_row))

    # Check if patient already exists in FHIR store, update if found, insert if not
    fhir_query_response = None
    fhir_query_headers = {'Authorization': fhir_auth_token}
    fhir_query_params = {'identifier': 'uwDAL_Clarity|' + str(pat_data['pat_id']) + ',http://www.uwmedicine.org/epic_patient_id|' + str(pat_data['pat_id']), 'active': 'true'}
    fhir_query_response = session.get(fhir_endpoint + '/Patient', headers = fhir_query_headers, params = fhir_query_params)

    if debug_level > '8':
        log_it("FHIR patient query URL: " + fhir_query_response.url)

    if fhir_query_response is not None:
        if fhir_query_response.status_code != 200:
            log_it("FHIR patient query failed, status code: " + str(fhir_query_response.status_code))
            break
        else:
            fhir_query_reply = fhir_query_response.json()
    
            if debug_level > '8':
                log_it("FHIR patient query response: " + json.dumps(fhir_query_reply))
        
            if fhir_query_reply["total"] > 1:
                log_it("ERROR: Multiple existing patients found with same ID (" + str(pat_data['pat_id']) + "), this should never happen... exiting.")
            else:
                if fhir_query_reply["total"] == 1:
                    if "entry" in fhir_query_reply:                                     # Existing patient found, update
                        log_it("Patient ID (" + str(pat_data['pat_id']) + ") found in FHIR store, updating...")
                        patient_request_method = "PUT"
                        patient_hapi_id = fhir_query_reply["entry"][0]["resource"]["id"]
    
                        if debug_level > '8':
                            log_it("Existing patient resource found, HAPI ID (" + str(patient_hapi_id) + ")")
                    
                        # Need to pull any existing identifiers (except 'epic_patient_id' and 'mrn') out of the existing patient resource to add them to the update bundle
                        addl_identifiers = {}
                        for identifier in fhir_query_reply["entry"][0]["resource"]["identifier"]:
                            if identifier["system"] in ["http://www.uwmedicine.org/mrn", "http://www.uwmedicine.org/epic_patient_id"]:
                                continue
    
                            if debug_level > '8':
                                log_it("Adding existing identifier to updated patient resource bundle: " + identifier["system"] + "|" + identifier["value"])
                        
                            updated_pat_map = pat_map + """
  * identifier
    * system = '""" + identifier["system"] + """'
    * value = \"""" + identifier["value"] + """\"
    """
                else:                                                                   # Patient not fouund, insert as new
                    log_it("Patient ID (" + str(pat_data['pat_id']) + ") not found in FHIR store, adding...")
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
    
                if debug_level > '8':
                    log_it("FUME patient POST URL: " + fume_patient_response.url)
            
                if fume_patient_response is not None:
    
                    if debug_level > '8':
                        log_it("FUME patient POST response: " + json.dumps(fume_patient_response.json()))
                
                    log_it("Got FHIR data from FUME, sending to FHIR server...")
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
    
                        if debug_level > '8':
                            log_it("FHIR patient " + patient_request_method + " URL: " + fhir_patient_response.url)
                    
                        fhir_patient_reply = fhir_patient_response.json()
    
                        if debug_level > '8':
                            log_it("FHIR patient " + patient_request_method + " response: " + json.dumps(fhir_patient_reply))
                    
                        if "entry" in fhir_patient_reply:
                            patient_hapi_id = fhir_patient_reply["entry"][0]["response"]["location"].split("/")[1]
                        if patient_request_method == "POST":
                            patient_action = "added"
                        else:
                            patient_action = "updated"
                        log_it("Patient ID (" + str(pat_data['pat_id']) + ") resource " + patient_action + ", HAPI ID (" + str(patient_hapi_id) + ")...")
                        pat_cnt = pat_cnt + 1
                        
                        # If configured, process procedures for patient and insert, update, delete as needed
                        if config["INCLUDE_PROCEDURES"] == '1':
                            # Pull all existing procedures for patient from the FHIR store
                            fhir_proc_query_response = None
                            fhir_proc_query_headers = {'Authorization': fhir_auth_token}
                            fhir_proc_query_params = {'subject': 'Patient/' + str(patient_hapi_id)}
                            fhir_proc_query_response = session.get(fhir_endpoint + '/Procedure', headers = fhir_proc_query_headers, params = fhir_proc_query_params)
    
                            if debug_level > '8':
                                log_it("FHIR procedure query URL: " + fhir_proc_query_response.url)
                        
                            if fhir_proc_query_response is not None:
                                if fhir_proc_query_response.status_code != 200:
                                    log_it("FHIR procedure query failed, status code: " + str(fhir_proc_query_response.status_code))
    
                                    if debug_level > '8':
                                        log_it("FHIR procedure query response: " + json.dumps(fhir_proc_query_response.json()))
    
                                    break
                                else:
                                    fhir_proc_query_reply = fhir_proc_query_response.json()
                                    
                                    if debug_level > '8':
                                        log_it("FHIR procedure query response: " + json.dumps(fhir_proc_query_reply))
    
                                    existing_fhir_proc_ids = {}
                                    if "entry" in fhir_proc_query_reply:
                                        for l in range(0, len(fhir_proc_query_reply["entry"])):
                                            proc = fhir_proc_query_reply["entry"][l]
                                            existing_fhir_proc_ids[str(proc["resource"]["identifier"][0]["value"])] = str(proc["resource"]["id"])
    
                                    dawg_proc_ids = []
                                    if str(pat_data['pat_id']) in proc_data.keys():
                                        log_it("Processing procedure data for patient...")
                                        for proc_row in proc_data[str(pat_data['pat_id'])]:
                                            proc_request_method = ""
                                            proc_hapi_id = None
        
                                            dawg_proc_ids.append(str(proc_row['uniq_id']))
                                            post_data = json.dumps({'input': proc_row,
                                                                    'fume': proc_map + "'Patient/" + str(patient_hapi_id) + "'"})
            
                                            if str(proc_row['uniq_id']) in existing_fhir_proc_ids.keys():
                                                log_it("Existing procedure found, upadating...")
                                                proc_request_method = "PUT"
                                                proc_hapi_id = existing_fhir_proc_ids[str(proc_row['uniq_id'])]
                            
                                                if debug_level > '8':
                                                    log_it("Existing procedure resource found, HAPI ID (" + str(proc_hapi_id) + ")")
                                            
                                            else:
                                                proc_request_method = 'POST'
                                                proc_hapi_id = None
        
                                            fume_proc_response = None
                                            fume_proc_headers = {'Content-type': 'application/json'}
                                            fume_proc_response = session.post(fume_endpoint, data = post_data, headers = fume_proc_headers)
            
                                            if debug_level > '8':
                                                log_it("FUME procedure POST URL: " + fume_proc_response.url)
                                        
                                            if fume_proc_response is not None:
                                                log_it("Got FHIR data from FUME, sending to FHIR server...")
            
                                                if debug_level > '8':
                                                    log_it("FUME procedure POST response: " + json.dumps(fume_proc_response.json()))
                                            
                                    
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
            
                                                if debug_level > '8':
                                                    log_it("FHIR procedure " + proc_request_method + " URL: " + fhir_proc_response.url)
                                            
                                                if fhir_proc_response is not None:
                                                    fhir_proc_reply = fhir_proc_response.json()
                            
                                                    if debug_level > '8':
                                                        log_it("FHIR procedure " + proc_request_method + " response: " + json.dumps(fhir_proc_reply))
                                                
                                                    if "entry" in fhir_proc_reply:        
                                                        proc_hapi_id = fhir_proc_reply["entry"][0]["response"]["location"].split("/")[1]
                                                    if proc_request_method == "POST":
                                                        proc_action = "added"
                                                    else:
                                                        proc_action = "updated"
                                                    log_it("Procedure ID (" + str(proc_row['uniq_id']) + ") resource " + proc_action + ", HAPI ID (" + str(proc_hapi_id) + ")...")
                                                    proc_cnt = proc_cnt + 1
                                                else:
                                                    log_it("ERROR: Unable to add procedure resource with ID (" + str(proc_row['uniq_id']) + "), skipping...")
                                    else:
                                        log_it("No procedures found for patient, skipping...")
    
                                    # Delete any existing FHIR procedure resources not found in the current list of patient procedures from the DAWG
                                    for proc_id in list(set(existing_fhir_proc_ids.keys()).difference(dawg_proc_ids)):
                                        fhir_proc_del_response = None
                                        fhir_proc_del_response = session.delete(fhir_endpoint + "/Procedure/" + existing_fhir_proc_ids[proc_id])
    
                                        if debug_level > '8':
                                            log_it("FHIR procedure DELETE URL: " + fhir_proc_del_response.url)
                                    
                                        if fhir_proc_del_response is not None:
    
                                            if debug_level > '8':
                                                if fhir_proc_del_response.headers["content-type"].strip().startswith("application/json"):
                                                    log_it("FHIR procedure DELETE response: " + json.dumps(fhir_proc_del_response.json()))
                                                else:
                                                    log_it("FHIR procedure DELETE response: " + fhir_proc_del_response.text)
    
                                            log_it("Procedure ID (" + str(proc_id) + ") resource deleted, HAPI ID (" + str(existing_fhir_proc_ids[proc_id]) + ")...")
                                            proc_del_cnt = proc_del_cnt + 1
                            else:
                                log_it("ERROR: Unable to query FHIR store for procedures... exiting.")
                        else:
                            log_it("Not processing procedures due to coniguration setting...")
                    else:
                        log_it("ERROR: Unable to add patient resource with ID (" + str(pat_data["pat_id"]) + "), skipping...")
                else:
                    log_it("ERROR: No data returned from FUME... exiting.")
    else:
        log_it("ERROR: Unable to query FHIR store for patients... exiting.")

log_it("Total patients added/updated: " + str(pat_cnt))
log_it("Total procedures added: " + str(proc_cnt))
log_it("Total procedures deleted: " + str(proc_del_cnt))

pat_cursor.close()
proc_cursor.close()

log_it("=========================== FINISH DAILY RUN =============================")

LOG_FILE.close()
