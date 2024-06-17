# -*- coding: utf-8 -*-
"""
Created on Mon Jan 16 10:25:13 2023

@author: jsibley
"""

from os.path import exists
import datetime, json, os, pathlib, pyodbc, re, requests

def handle_bit_type(bit_value):
    if bit_value == b'\x00':
        return 0
    if bit_value == b'\x01':
        return 1

now = datetime.datetime.now()

dawg_server = 'AM-DAWG-SQL-PRT'
dawg_database = 'uwReports'
dawg_cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + dawg_server + ';DATABASE=' + dawg_database + ';Trusted_Connection=yes;')
dawg_cnxn.add_output_converter(pyodbc.SQL_BIT, handle_bit_type)

pat_cursor = dawg_cnxn.cursor()
proc_cursor = dawg_cnxn.cursor()
pat_sql = """
            /* CPR patients who've had a visit in the last 5 years and also had one of the procedures of interest (in same time period) */
            select distinct pd.PatientEpicId, case when pd.PatientMrnUwmc is not NULL then pd.PatientMrnUwmc when pd.PatientMrnHmc is not NULL then pd.PatientMrnHmc when pd.PatientMrnUwpn is not NULL then pd.PatientMrnUwpn end mrn,
            pd.FirstName first_name, pd.LastName last_name, pd.BirthDateDurableKey birth_date, case when pd.SexAbbreviation = 'M' then 'male' when pd.SexAbbreviation = 'F' then 'female' when pd.SexAbbreviation in ('X', 'NB', 'I', 'ANL') then 'other' else 'unknown' end sex
            from MDW_DEEP.Dimensional.CombinedProcedureEventFact cpef
            join MDW_DEEP.Dimensional.PatientDim pd on cpef.PatientDurableKey = pd.PatientDurableKey
            where cpef.PatientDurableKey in (
            select distinct pd.PatientDurableKey
            from MDW_DEEP.Dimensional.VisitFact aef
            join MDW_DEEP.Dimensional.DepartmentDim dd on dd.DepartmentDurableKey = aef.DepartmentDurableKey
            join MDW_DEEP.Dimensional.PatientDim pd on pd.PatientDurableKey = aef.PatientDurableKey
            where dd.DepartmentDurableKey = '894933337'
            and aef.AppointmentStatus in ('Completed', 'Arrived')
            and convert(date, aef.AppointmentDateDurableKey) between convert(date, dateadd(year, -5, getdate())) and convert(date, getdate())
            )
            and cpef.ProcedureCodeSet in ('cp', 'cpt4')
            and cpef.ProcedureCode in ('1009020', '1009077', '1009065', '1009019', '1009032')
            and cpef.ProcedureStartInstant is not NULL
            and convert(date, cpef.ProcedureStartInstant) between convert(date, dateadd(year, -5, getdate())) and convert(date, getdate())
          """

proc_sql = """
            /* List of the procedures of interest (for the last 5 years) for a particular CPR patient */
            select distinct case when cpef.OrderStatusCode in (1, 2) then 'in-progress'
            					 when cpef.OrderStatusCode in (3, 5) then 'completed'
            					 when cpef.OrderStatusCode = 4 then 'not-done'
            					 else 'unknown'
            				end status_code, cpef.ProcedureCode cpt_code, cpef.Description descr, format(cpef.ProcedureStartInstant, 'yyyy-MM-dd') proc_date
            from MDW_DEEP.Dimensional.CombinedProcedureEventFact cpef
            join MDW_DEEP.Dimensional.PatientDim pd on cpef.PatientDurableKey = pd.PatientDurableKey
            where cpef.ProcedureCodeSet in ('cp', 'cpt4')
            and cpef.ProcedureCode in ('1009020', '1009077', '1009065', '1009019', '1009032')
            and cpef.ProcedureStartInstant is not NULL
            and convert(date, cpef.ProcedureStartInstant) between convert(date, dateadd(year, -5, getdate())) and convert(date, getdate())
            and pd.PatientEpicId = 
           """

pat_map = """  Instance: $pid := $uuid()
  InstanceOf: Patient
  * identifier
    * system = 'http://www.uwmedicine.org/mrn'
    * value = mrn
  * active = status='active'
  * name
    * given = first_name
    * family = last_name
  * birthDate = birth_date
  * gender = sex"""

proc_map = """  InstanceOf: Procedure
  * identifier
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

# Get patient data, process 
pat_cursor.execute(pat_sql)
pat_vals = pat_cursor.fetchall()
pat_cols = [column[0] for column in pat_cursor.description]

cnt = 1
for pat_row in pat_vals:
    if cnt == 1: 
        post_data = json.dumps({'input': dict(zip(pat_cols, pat_row)),
                                'fume': pat_map})
        print(post_data)
        response = None
        headers = {'Content-type': 'application/json'}
        response = requests.post('http://localhost:42424', data = post_data, headers = headers)
        if response is not None:
            print('Request complete, status code (' + str(response.status_code) + ')...')
            print(response.json())
        cnt = cnt + 1
        
        pat_bundle = {
                      "resourceType": "Bundle",
                      "type": "transaction",
                      "entry": []
                     }
        pat_bundle["entry"].append({"resource": response.json(),
                                    "request": {
                                        "url": "Patient",
                                        "method": "POST"
                                       }
                            
                                  })
        
        # Post patient to FHIR server
        response = None
        headers = {'Content-type': 'application/fhir+json;charset=utf-8'}
        response = requests.post('http://localhost:18090/fhir', json = pat_bundle, headers = headers)
        if response is not None:
            print('Request complete, status code (' + str(response.status_code) + ')...')
            print(response.json())
            pat_id = response.json()["entry"][0]["response"]["location"]

        # Get procedures for patient, process and post to FHIR server
        proc_cursor.execute(proc_sql + "'" + pat_row[0] + "'")
        proc_vals = proc_cursor.fetchall()
        proc_cols = [column[0] for column in proc_cursor.description]
        
        for proc_row in proc_vals:
            post_data = json.dumps({'input': dict(zip(proc_cols, proc_row)),
                                    'fume': proc_map + "'" + pat_id + "'"})
            print(post_data)
            response = None
            headers = {'Content-type': 'application/json'}
            response = requests.post('http://localhost:42424', data = post_data, headers = headers)
            if response is not None:
                print('Request complete, status code (' + str(response.status_code) + ')...')
                print(response.json())

            proc_bundle = {
                          "resourceType": "Bundle",
                          "type": "transaction",
                          "entry": []
                         }
            proc_bundle["entry"].append({"resource": response.json(),
                                        "request": {
                                            "url": "Procedure",
                                            "method": "POST"
                                           }
                                
                                      })
        
            response = None
            headers = {'Content-type': 'application/fhir+json;charset=utf-8'}
            response = requests.post('http://localhost:18090/fhir', json = proc_bundle, headers = headers)
            if response is not None:
                print('Request complete, status code (' + str(response.status_code) + ')...')
                print(response.json())

pat_cursor.close()
proc_cursor.close()
