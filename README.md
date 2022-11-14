# Assignment_test_for_Lead_QA_Engineer_role
## Table of contents
* [General info](#general-info)
* [Technologies](#technologies)
* [Setup](#setup)

## General info
This project consists to create functional tests for the data provided in the file "elastic_data.json" containing 4675 records capturing purchases of different users.

The project is in the Task folder.
The file test_cases.py contains all the implementation: test cases and functions.

The stucture of the test cases is equal among all the test cases and follows the steps:

* Create a list of record IDs referring to the failed records (i.e. id_test_name_fail)
* Store the ID's list length in a variable
* If the lenght of the list is not 0 would be created the file test_name_failed_record_ids.json containig the test name and the record's IDs failed
* Assert statement: list lenght = 0 
* Reports

The list id_test_name_fail is returned by the functoin in which resides the test case implementation.
For each test case there is a functoin (or more) in which are implemented the checking to the considered record's field of "elastic_data.json"
The output of this function is a list of record's IDs failed that is returned to the test case
In the test case is called the function to export the failed id list to a json file, as mentioned above
The check in the assert statement provides the pass/fail condition to pytest

The reports are generated in three ways:

* Directly from the test case, if the fail condition is verified, creating the respective IDs json

* Test_report.html: Executing the test suite using pytest-html is generated an html report file that summarizes each tests outcome 
  In the Setup section the instruction to install and run the plugin 
   
* Report.json: Executing the test suite using Pytest JSON Report is generate a detailed test report in json format that can be post processed in other applications



## Technologies
Project is created with:
* Python version: 3.11.0
* Pytest version: 7.2.0
* Pytest-HTML version: 3.2.0
* Pytest JSON Report version: 1.5.0


 
## Setup

* Pytest-HTML Setup:  pip install pytest-html        
    * Pytest-HTML project run:  pytest -rA --html=Test_report.html     

* Pytest JSON Report Setup: pip install pytest-json-report --upgrade 
    * Pytest JSON Report run  pytest --json-report --json-report-indent=6 --json-report-tests   pip install pytest-json-report --upgrade
