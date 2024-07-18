# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 10:25:13 2023

@author: jsibley
"""

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
LOG_FILE = open("C:\\Users\\jsibley\\Desktop\\paintracker_daily_update_dawg_to_fhir_via_fume.log", "a", encoding="utf-8")

dawg_server = 'AM-DAWG-SQL-PRT'
dawg_database = 'uwReports'
dawg_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + dawg_server + ';DATABASE=' + dawg_database + ';Trusted_Connection=yes;')
dawg_cnxn.add_output_converter(pyodbc.SQL_BIT, handle_bit_type)

pat_cursor = dawg_cnxn.cursor()
proc_cursor = dawg_cnxn.cursor()
pat_sql = """
            /* CPR patients who have/had a scheduled visit in -2 to +7 days */
            select distinct pd.PatientEpicId pat_id, case when pd.PatientMrnUwmc is not NULL then pd.PatientMrnUwmc when pd.PatientMrnHmc is not NULL then pd.PatientMrnHmc when pd.PatientMrnUwpn is not NULL then pd.PatientMrnUwpn end mrn,
            pd.FirstName first_name, pd.LastName last_name, pd.BirthDateDurableKey birth_date, case when pd.SexAbbreviation = 'M' then 'male' when pd.SexAbbreviation = 'F' then 'female' when pd.SexAbbreviation in ('X', 'NB', 'I', 'ANL') then 'other' else 'unknown' end sex
            from MDW_DEEP.Dimensional.PatientDim pd
            join MDW_DEEP.Dimensional.VisitFact aef on pd.PatientDurableKey = aef.PatientDurableKey
            join MDW_DEEP.Dimensional.DepartmentDim dd on dd.DepartmentDurableKey = aef.DepartmentDurableKey
            where dd.DepartmentDurableKey = '894933337'
            and aef.AppointmentStatus in ('Completed', 'Arrived', 'Scheduled')
            and convert(date, aef.AppointmentDateDurableKey) between dateadd(day, -2, convert(date, getdate())) and dateadd(day, 7, convert(date, getdate()))
          """

proc_sql = """
            /* List of the procedures of interest (for the last 5 years) for a particular CPR patient */
            select distinct case when cpef.OrderStatusCode in (1, 2) then 'in-progress'
            					 when cpef.OrderStatusCode in (3, 5) then 'completed'
            					 when cpef.OrderStatusCode = 4 then 'not-done'
            					 else 'unknown'
            				end status_code, cpef.ProcedureCode cpt_code, cpef.Description descr, format(cpef.ProcedureStartInstant, 'yyyy-MM-dd') proc_date,
                            cpef.SourceSystemId uniq_id
            from MDW_DEEP.Dimensional.CombinedProcedureEventFact cpef
            join MDW_DEEP.Dimensional.PatientDim pd on cpef.PatientDurableKey = pd.PatientDurableKey
            where cpef.ProcedureCodeSet in ('cp', 'cpt4')
            and cpef.ProcedureCode in ('1009020', '1009077', '1009065', '1009019', '1009032', '20552', '20553', '20526', '20550', '20551', '20560', '20561', '64490', '64491', '64492', '0213T', '0214T', '0215T', '64493', '64494', '64495', '0216T', '0217T', '0218T', '20600', '20604', '20605', '20606', '20610', '20611', '27096', '20552', '64451', '62321', '62320', '62325', '62324', '62323', '62322', '62327', '62326', '64479', '64480', '64483', '64484', '62323', '62322', '62318', '62319', '62273', '64400', '64999X', '64405', '64615', '64408', '64999Y', '64999Z', '64415', '64417', '64418', '64461', '64462', '64999V', '64420', '64421', '64486', '64488', '64425', '64425', '64430', '64445', '64447', '64449', '64454', '64455', '64450', '64505', '64999U', '64510', '64517', '64520', '64520', '64530', '96372', '96369', '96365', '96366', '64633', '64634', '64635', '64636', '64600', '64620', '64630', '64624', '64625', '64640', '64680', '64681', '63650', '63655', '63661', '63662', '63663', '63664', '63685', '63688', '95970', '95971', '95972', '95974', '95975', '64555', '64585', '95970', '95971', '95972', 'XFLNP', '77003', '77012', '76942', '76882', '76536', '99156', '99157')
            and cpef.ProcedureStartInstant is not NULL
            and convert(date, cpef.ProcedureStartInstant) between convert(date, dateadd(year, -5, getdate())) and convert(date, getdate())
            and pd.PatientEpicId = 
           """

pat_map = """  Instance: $pid := $uuid()
  InstanceOf: Patient
  * identifier
    * system = 'http://www.uwmedicine.org/mrn'
    * value = mrn
  * identifier
    * system = 'http://www.uwmedicine.org/epic_patient_id'
    * value = pat_id
  * active = true
  * name
    * given = first_name
    * family = last_name
  * birthDate = birth_date
  * gender = sex"""

proc_map = """  InstanceOf: Procedure
  * identifier
    * system = 'http://www.uwmedicine.org/procedure_id'
    * value = uniq_id
  * code
    * coding
      * code = cpt_code
      * display = descr
      * system = 'http://www.ama-assn.org/go/cpt'
    * text = descr
  * performedDateTime = proc_date
  * status = status_code
  * subject
    * reference = """

fhir_endpoint = 'https://fhir-auth.uwmedicine.stage.cosri.app/fhir'
fhir_auth_token = 'Bearer <place secret here>'
fume_endpoint = 'http://localhost:42424'

# Get patient data, process 
pat_cursor.execute(pat_sql)
pat_vals = pat_cursor.fetchall()
pat_cols = [column[0] for column in pat_cursor.description]

log_it("=========================== STARTING DAILY RUN =============================")

# Set debug level, anything less than 9 is "info/warning", 9 or greater is "debug"
debug_level = 1

pat_cnt = 0
proc_cnt = 0
for pat_row in pat_vals:
    request_method = ""
    updated_pat_map = None

    pat_data = dict(zip(pat_cols, pat_row))

    # Check if patient already exists in FHIR store, update if found, insert if not
    fhir_query_response = None
    fhir_query_headers = {'Authorization': fhir_auth_token}
    fhir_query_params = {'identifier': 'uwDAL_Clarity|' + str(pat_data['pat_id']) + ',http://www.uwmedicine.org/epic_patient_id|' + str(pat_data['pat_id'])}
    fhir_query_response = requests.get(fhir_endpoint + '/Patient', headers = fhir_query_headers, params = fhir_query_params)

    if debug_level > 8:
        log_it("FHIR patient query URL: " + fhir_query_response.url)

    if fhir_query_response is not None:
        if fhir_query_response.status_code != '200':
            log_it("FHIR patient query failed, status code: " + str(fhir_query_response.status_code))
            break
        else:
            fhir_query_reply = fhir_query_response.json()
    
            if debug_level > 8:
                log_it("FHIR patient query response: " + json.dumps(fhir_query_reply))
        
            if fhir_query_reply["total"] > 1:
                log_it("ERROR: Multiple existing patients found with same ID (" + str(pat_data['pat_id']) + "), this should never happen... exiting.")
            else:
                if fhir_query_reply["total"] == 1:
                    if "entry" in fhir_query_reply:                                     # Existing patient found, update
                        log_it("Patient ID (" + str(pat_data['pat_id']) + ") found in FHIR store, updating...")
                        request_method = "PUT"
                        patient_hapi_id = fhir_query_reply["entry"][0]["resource"]["id"]
    
                        if debug_level > 8:
                            log_it("Existing patient resource found, HAPI ID (" + str(patient_hapi_id) + ")")
                    
                        # Need to pull any existing identifiers (except 'epic_patient_id' and 'mrn') out of the existing patient resource to add them to the update bundle
                        addl_identifiers = {}
                        for identifier in fhir_query_reply["entry"][0]["resource"]["identifier"]:
                            if identifier["system"] in ["http://www.uwmedicine.org/mrn", "http://www.uwmedicine.org/epic_patient_id"]:
                                continue
    
                            if debug_level > 8:
                                log_it("Adding existing identifier to updated patient resource bundle: " + identifier["system"] + "|" + identifier["value"])
                        
                            updated_pat_map = pat_map + """
  * identifier
    * system = '""" + identifier["system"] + """'
    * value = \"""" + identifier["value"] + """\"
    """
                else:                                                                   # Patient not fouund, insert as new
                    log_it("Patient ID (" + str(pat_data['pat_id']) + ") not found in FHIR store, adding...")
                    request_method = "POST"           
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
                fume_patient_response = requests.post(fume_endpoint, data = fume_post_data, headers = fume_patient_headers)
    
                if debug_level > 8:
                    log_it("FUME patient POST URL: " + fume_patient_response.url)
            
                if fume_patient_response is not None:
    
                    if debug_level > 8:
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
                                                    "method": request_method
                                                   }
                                              })
                    
                    # Send patient resource to FHIR server
                    fhir_patient_response = None
                    fhir_patient_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                            'Authorization': fhir_auth_token}
                    if request_method == "POST":
                        fhir_patient_response = requests.post(fhir_endpoint, json = pat_bundle, headers = fhir_patient_headers)
                    else:
                        fume_patient_response_json = fume_patient_response.json()
                        fume_patient_response_json["id"] = patient_hapi_id
                        fhir_patient_response = requests.put(fhir_endpoint + "/Patient/" + patient_hapi_id, json = fume_patient_response_json, headers = fhir_patient_headers)
                    if fhir_patient_response is not None:
    
                        if debug_level > 8:
                            log_it("FHIR patient " + request_method + " URL: " + fhir_patient_response.url)
                    
                        fhir_patient_reply = fhir_patient_response.json()
    
                        if debug_level > 8:
                            log_it("FHIR patient " + request_method + " response: " + json.dumps(fhir_patient_reply))
                    
                        if "entry" in fhir_patient_reply:
                            patient_hapi_id = fhir_patient_reply["entry"][0]["response"]["location"]
                        if request_method == "POST":
                            patient_action = "added"
                        else:
                            patient_action = "updated"
                        log_it("Patient ID (" + str(pat_data['pat_id']) + ") resource " + patient_action + ", HAPI ID (" + str(patient_hapi_id) + ")...")
                        pat_cnt = pat_cnt + 1
                        
                        # Get procedures for patient, process and post to FHIR server (unless already exists)
                        proc_cursor.execute(proc_sql + "'" + pat_row[0] + "'")
                        proc_vals = proc_cursor.fetchall()
                        proc_cols = [column[0] for column in proc_cursor.description]
                        
                        for proc_row in proc_vals:
                            post_data = json.dumps({'input': dict(zip(proc_cols, proc_row)),
                                                    'fume': proc_map + "'Patient/" + patient_hapi_id + "'"})
                            
                            # Check if procedure resource already exists
                            fhir_proc_query_response = None
                            fhir_proc_query_headers = {'Authorization': fhir_auth_token}
                            fhir_proc_query_params = {'identifier': 'http://www.uwmedicine.org/procedure_id|' + str(proc_row[4])}
                            fhir_proc_query_response = requests.get(fhir_endpoint + '/Procedure', headers = fhir_proc_query_headers, params = fhir_proc_query_params)
    
                            if debug_level > 8:
                                log_it("FHIR procedure query URL: " + fhir_proc_query_response.url)
                        
                            if fhir_proc_query_response is not None:
                                if fhir_proc_query_response.status_code != '200':
                                    log_it("FHIR procedure query failed, status code: " + str(fhir_proc_query_response.status_code))
                                    break
                                else:
                                    fhir_proc_query_reply = fhir_proc_query_response.json()
                            
                                    if debug_level > 8:
                                        log_it("FHIR procedure query response: " + json.dumps(fhir_proc_query_reply))
        
                                    if fhir_proc_query_reply["total"] > 0:
                                        log_it("Existing procedure found, skipping...")
                                    else:
                                        fume_proc_response = None
                                        fume_proc_headers = {'Content-type': 'application/json'}
                                        fume_proc_response = requests.post(fume_endpoint, data = post_data, headers = fume_proc_headers)
        
                                        if debug_level > 8:
                                            log_it("FUME procedure POST URL: " + fume_proc_response.url)
                                    
                                        if fume_proc_response is not None:
                                            log_it("Got FHIR data from FUME, sending to FHIR server...")
        
                                            if debug_level > 8:
                                                log_it("FUME procedure POST response: " + json.dumps(fume_proc_response.json()))
                                        
                                
                                            proc_bundle = {
                                                          "resourceType": "Bundle",
                                                          "type": "transaction",
                                                          "entry": []
                                                         }
                                            proc_bundle["entry"].append({"resource": fume_proc_response.json(),
                                                                        "request": {
                                                                            "url": "Procedure",
                                                                            "method": "POST"
                                                                           }
                                                                      })
                                        
                                            fhir_proc_response = None
                                            fhir_proc_headers = {'Content-type': 'application/fhir+json;charset=utf-8',
                                                                 'Authorization': fhir_auth_token}
                                            fhir_proc_response = requests.post(fhir_endpoint, json = proc_bundle, headers = fhir_proc_headers)
        
                                            if debug_level > 8:
                                                log_it("FHIR procedure POST URL: " + fhir_proc_response.url)
                                        
                                            if fhir_proc_response is not None and "entry" in fhir_proc_response.json():
        
                                                if debug_level > 8:
                                                    log_it("FHIR procedure POST response: " + json.dumps(fhir_proc_response.json()))
        
                                                fhir_proc_id = fhir_proc_response.json()["entry"][0]["response"]["location"].split("/")[1]
                                                log_it("Procedure ID (" + str(proc_row[4]) + ") resource added, HAPI ID (" + str(fhir_proc_id) + ")...")
                                                proc_cnt = proc_cnt + 1
                                            else:
                                                log_it("ERROR: Unable to add procedure resource with ID (" + str(proc_row[4]) + "), skipping...")
                            else:
                                log_it("ERROR: Unable to query FHIR store... exiting.")
                    else:
                        log_it("ERROR: Unable to add patient resource with ID (" + str(pat_data["pat_id"]) + "), skipping...")
                else:
                    log_it("ERROR: No data returned from FUME... exiting.")
    else:
        log_it("ERROR: Unable to query FHIR store... exiting.")

log_it("Total patients added/updated: " + str(pat_cnt))
log_it("Total procedures added: " + str(proc_cnt))

pat_cursor.close()
proc_cursor.close()

log_it("=========================== FINISH DAILY RUN =============================")

LOG_FILE.close()
