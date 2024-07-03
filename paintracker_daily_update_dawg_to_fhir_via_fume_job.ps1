$Logfile = "C:\Users\jsibley\Desktop\paintracker_daily_update_dawg_to_fhir_via_fume.log"
$Timestamp = Get-Date -UFormat "%Y-%m-%d %T"
Add-content $Logfile -value "[$Timestamp] Starting up FUME API..."
Start-Job { cd C:\Users\jsibley\Documents\fume-community; npm start }
Start-Sleep -Seconds 30
$Timestamp = Get-Date -UFormat "%Y-%m-%d %T"
Add-content $Logfile -value "[$Timestamp] Running PaInTRaCkEr daIlY UpDaTe daWg-tO-FhIr script..."
cd "C:\Users\jsibley\Documents"
Invoke-Expression "& python 'C:\Users\jsibley\Documents\paintracker_daily_update_dawg_to_fhir_via_fume.py' "
$Timestamp = Get-Date -UFormat "%Y-%m-%d %T"
Add-content $Logfile -value "[$Timestamp] Stopping FUME API..." 
Stop-Process -Name "node"