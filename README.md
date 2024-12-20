# dawg-to-fhir
Retrieve patient and other data from the DAWG for PainTracker patients and transform it via FUME into FHIR resources.

1. Get the [FUME community edition](https://github.com/Outburn-IL/fume-community) and install using their instructions.

2. Edit the FUME __.env__ file to configure FUME port number and FHIR standards to include.

3. Edit configurable items in the __config_*.env__ files:
   - Log file location
   - FHIR server endpoint
   - FHIR server auth token
   - FUME endpoint (if changed in the FUME __.env__ file)
   - Debug level (anything less than 9 is "info/warning", 9 or greater is "debug")
   - Types of resources to include/exclude

4. Edit configurable items in the __paintracker_daily_update_dawg_to_fhir_via_fume_job.ps1__ file (this file starts the FUME API, runs the Python script above and then shuts down the FUME API.  It's meant to be run via Windows Task Scheduler):
   - Log file location
   - Path to local FUME instance
   - Path to Python script [__paintracker_daily_update_dawg_to_fhir_via_fume.py__]
