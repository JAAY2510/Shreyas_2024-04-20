# Shreyas_2024-04-20
API Monitoring System

  Setup instructions: 
  1. download store/status.csv from https://drive.google.com/file/d/1iXuwTOcCsBFlFofEMgZUC8DMJlNtFxdQ/view?usp=sharing

  2. Run the files in the repository titiled store_monitoring.py

  3. now if the flask server is shown copy the address here: http://127.0.0.1:5000

  4. Open command prompt and type the command to trigger api 1 curl -X POST http://127.0.0.1:5000/trigger_report

  5. now if it is sucess you will get report_id copy it and past it in the place of <report_id>  curl http://127.0.0.1:5000/get_report/<report_id>

  6. should return runnning if the report is being created or csv downloadable file if the report is done.  

  7. exit flask by ctrl + c 
