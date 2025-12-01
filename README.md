# Malicious URL Detection
This project utilizes a public dataset of over 650,000 malicious links to identify patterns in packet rates, flow duration, and TCP flags and attempt to detect malicious URLs.

## Requirements
- Google Cloud account with Storage access
- Google Cloud VM with PySpark installed
- Python 3
- JAVA 11
- PySpark libraries

## Output files of PA6 script
- freq_label – frequency table of traffic labels  
- top_ports – top 20 destination ports  
- flow_summary – flow duration summary statistics  

## How to Run PA6 script (assumed script is in VM or bucket already for use)
- download dataset into cloud storage bucket
- set variables in script: OUT_DIR to desired location for output, and DATASET to location of your dataset in your bucket
- Launch configured VM and ensure everything is set up correctly. (java, python, pyspark, etc.)
- Run script using spark-submit <file-name> or spark-submit --conf spark.ui.enabled=true --conf spark.ui.port=4040 ~/pyspark_job.py
- if you ran the 2nd command you can see the Spark UI by first logging in to your gcloud account locally(in terminal) using gcloud auth login
- then run gcloud compute ssh <VM-name> --zone=<VM-ZONE> -- -L 4040:localhost:4040 ,in your local terminal/command prompt
- from there you would go to http://localhost:4040/ in your browser, where you should be able to see information on how the script/job ran
- There is a 3 minute buffer for you to view your sparkUI tabs.
- After job is ran you should be able to see the CSV files outputed into your desired location. 
