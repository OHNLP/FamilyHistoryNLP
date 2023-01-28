# Installation Instructions for OHNLP Family History NLP Module

## I.	Prerequisites:

-	Java 8-14 (11+ preferred)
-	An active OHNLP Toolkit Install (OHNLP Backbone + MedTagger). Installation instructions have been included for your convenience

## II.	OHNLP Toolkit Installation Instructions

**NB**: If you already have an active OHNLP toolkit installation, these instructions can be skipped, please proceed directly to section III
**NB**: These steps require an internet connection to download requisite libraries. Once download is complete (through step 6), the entire OHNLPTK folder can be copied to a separate machine if execution in an isolated environment is desired. 

1.	Copy the installation shell script into a linux/unix system from 
2.	Run the installation script (note that you may need to first enable execution via chmod +x your_file_name_here.sh)
3.	Change directory into the created *OHNLPTK/* directory
4.	chmod +x ./run_pipeline_local.sh and then run *./run_pipeline_local.sh*
5.	Follow the instructions presented onscreen to change configuration settings/job parallelism to suit your local execution environment
6.	Instead of pressing enter once configuration options are changed, ctrl+c to exit out. At this point you have a working base OHNLP Toolkit install

## III.	Installation and Update Instructions:
1.	Download OHNLP_FamilyHistory.zip from the [Github repo](https://github.com/OHNLP/FamilyHistoryNLP) 
2.	The zip file will contain three folders, *configs/*, *modules/* and *resources/*. Copy the contents to their respective folders in your OHNLP Tookit installation
3.	Go to *configs/example_fh_nlp_filesystem_to_csv.json* and make a copy.  Do not modify this example json directly as changes will be overwritten on updates
4.	Pick one of a or b
	a.	If files in/files out is suitable for your use case, change lines 8 and 28 to the appropriate input/output directories. 
	b.	If you wish to change input/output formats, replace lines 5-12 and 23-44 with the correct backbone input and output function respectively. Supported formats include SQL, BigQuery, HCatalog, and JSON


## IV.	Execution Instructions:
**NB**: this assumes you are using a local run. If you wish to run on some distributed platform e.g. Spark or GCP, use the appropriate run script
-	If on Windows (via WSL) or Linux/Unix and wish to run via interactive mode: Run ./run_pipeline_local.sh from the OHNLP Toolkit root directory and enter the number corresponding to the fh config when prompted
-	If on Mac or if you wish to run in non-interactive/headless mode: Run ./run_pipeline_local.sh *your_config_name_here.json* Note that the preceding configs/ is intentionally omitted


