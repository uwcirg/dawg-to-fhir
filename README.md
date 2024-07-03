# dawg-to-fhir
Retrieve patient and other data from the DAWG for PainTracker patients and transform it via FUME into FHIR resources.

1. Get the [FUME community edition](https://github.com/Outburn-IL/fume-community) and install using their instructions.

2. Edit the __.env__ file to configure FUME port number and FHIR standards to include.

3. Edit configurable items in the __paintracker_daily_update_dawg_to_fhir_via_fume.py__ file (this file handles the tasks of pulling patient and procedure data from the DAWG, converting to FHIR via FUME and adding/updating the FHIR store as needed):
   - Log file location
   - FHIR server endpoint
   - FHIR server auth token
   - FUME endpoint (if changed in __.env__ file
   - Debug level (anything less than 9 is "info/warning", 9 or greater is "debug")

4. Edit configurable items in the __paintracker_daily_update_dawg_to_fhir_via_fume.ps1__ file (this file starts the FUME API, runs the Python script above and then shuts down the FUME API.  It's meant to be run via Windows Task Scheduler):
   - Log file location
   - Path to local FUME instance
   - Path to Python script above
