# Python_ETL_Process
## Acquiring and processing information on world's largest banks

### Scenario:

You are tasked with creating a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD, with information extracted from the website link below.


**website_link:** https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks


Further, the data needs to be transformed and stored in **GBP, EUR** and **INR** as well, in accordance with the exchange rate information that has been made available to you as a CSV file named **exchange_rate.csv**. 


The processed information table is to be saved locally in a **CSV format** and as a **database table.** Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.


**N/B:** We will use the light-weight disk-based **Sqlite3** database for our project. Finally, create a **log_progress() function** to log the progress of the code at different stages in a file **'Bank_project_logs.txt'**



