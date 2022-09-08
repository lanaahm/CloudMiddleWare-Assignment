## 1. ETL Process

![image](https://user-images.githubusercontent.com/57904007/189003432-5a2e0c9e-39a5-4c1d-9cc2-a54709c07aef.png)

ETL is a process that consists of Extract, Transform, and Load. ETL is the process of retrieving data from one or many sources. The data will be cleaned before being stored on new storage ready for analysis.
This project will implement a simple ETL process from data sources. The following is a description of the data source consisting of several CSV files.

| File                       | Description                 |
| -------------------------- | --------------------------- |
| final_superstore.csv       | contain order information   |
| Customer_ID_Superstore.csv | contain Customerinformation |
| Product_ID_Superstore.csv  | contain product information |

---

## Implementation of each ETL function process

1. Extract

- [[ Create Folder ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/extract.py#L13)] => Create a new folder if it doesn't exists
- [[ Extract Raw Data ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/extract.py#L19)] => Extract new raw data from the current source

2. Transfrom

- [[ Lowercase ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/transform.py#L17)] => Lowercase string fields
- [[ Update Date ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/transform.py#L23)] => Update date format (e.g. from DD/MM/YYYY to YYYY-MM-DD)
- [[ Update Class ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/transform.py#L37)] => Update Class (e.g. return "Second" if string contains "Second class" substring)
- [[ Update Number ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/transform.py#L46)] => Return number as integer or float. "," to convert the number into float first (e.g. from "100,000.00" to "100000.00")

3. Load

- [[ Load ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/dags/load.py)] => Insert data from raw table into clean table and check data in clean table to raw table

## 2. Airflow

Apache Airflow is a platform used to schedule and manage data pipelines or workflows. This data pipeline refers to the management, coordination, scheduling, and management of complex pipeline data from disparate sources. The data pipeline is data that is ready to be used by business intelligence and data scientists.

Advantages of Apache Airflow

- Dependency Management represents the data flow in a data pipeline. An example of this is task A -> task B -> task C.
- Apache Airflow has many operators that can be used when creating pipelines.
- Apache Airflow has a very modern interface, where we can see the state of the DAG, check the runtime, check the log, re-run the task, etc.

Disadvantages of Apache Airflow

- If we want to change the schedule, we have to change the GAD because the processes from the previous task will not match the new schedule.
- If we deploy Airflow on a docker container when restarting, it will kill all current processes.
- It's not easy to run Airflow natively on windows.

![image](https://user-images.githubusercontent.com/57904007/189002658-8aedd0a1-92a6-4add-9472-5d666cb31403.png)

This is the output of log generated by Airflow.

1. Extract

- [[ Extract Zip File ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Extract)]

2. Transfrom

- [[ Init Transfrom ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Transfrom)]
- [[ Transfrom Customer ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Transfrom)]
- [[ Transfrom Product ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Transfrom)]
- [[ Transfrom Transaction ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Transfrom)]

3. Load

- [[ Load ](https://github.com/lanaahm/CloudMiddleWare-Assignment/blob/main/logs/READMME.md#Load)]

## 3. Data Visualization

After the data is processed through ETL and Airflow, data is ready to be used for various needs, such as business intelligence and data scientist. This is the result of data processing using Power BI.

![Screenshot Power BI](https://user-images.githubusercontent.com/57904007/189000388-275f77af-3429-4312-8172-99c632394c44.jpg)
