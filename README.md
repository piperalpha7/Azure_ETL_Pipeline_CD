# Azure_ETL_Pipeline_CD

I did this project as part of a course 'Ultimate Big Data Master's Program' by Mr. Sumit Mittal. Credits for the dataset and teaching go to him. I would recommend his program to anyone interested in this space.

![image](https://github.com/user-attachments/assets/03d29f41-3797-433f-aa62-c0c3d33f0374)

Above is the basic diagram of the project. There are however many changes I have done iteratively to automate and optimize the pipeline, which I explain as I move on.

## Iteration 1

1. Create a storage account(ADLS Gen2)
   The structure is as follows:

    └── ADLS GEN2(Storage Account- DataLake)/\
     &nbsp; &nbsp;└── Sales(Container)/\
     &nbsp; &nbsp;&nbsp;&nbsp;├── Landing(Folder)\
     &nbsp; &nbsp;&nbsp;&nbsp;├── Staging(Folder)\
     &nbsp; &nbsp;&nbsp;&nbsp;└── Discarded(Folder)\

2. Created an Azure Databricks Workspace
  
      ![image](https://github.com/user-attachments/assets/2ce241fd-ac0c-458c-993c-efda1fe11561)

      
3. The Azure Databricks workspace will be used to write Data Validation or any Pyspark code. Hence it should be able to access the files in ADLS Gen2. For this purpose we require Azure DataFactory which is used to build pipelines and also used to connect different services with each other. However for Databricks to access the ADLS Gen2 Data, a 'Storage Key' is required. We can therefore hardcode the storage key in the Databricks Notebook

~~~
    extra_configs = {
    'fs.azure.account.key.cdstorageaccount1.blob.core.windows.net': 'sdsdffreeee'}

    dbutils.fs.mount(
        source='wasbs://sales@cdstorageaccount1.blob.core.windows.net',
        mount_point='/mnt/sales',
        extra_configs=extra_configs
    )
~~~
where 'sdsdffreeee' is the secret key required to access ADLS Gen2. But this is not a good practice. Hence its better to store this secret key in a 'Key Vault' which guards all these secret keys well. The Code could then be modified as 

~~~
    extra_configs = {
    'fs.azure.account.key.cdstorageaccount1.blob.core.windows.net': dbutils.secrets.get('cd-secrets-databricks-scope', 'storagekey')}

    dbutils.fs.mount(
        source='wasbs://sales@cdstorageaccount1.blob.core.windows.net',
        mount_point='/mnt/sales',
        extra_configs=extra_configs
    )
~~~
where 'cd-secrets-databricks-scope' is the name of the 'Key Vault' and 'storagekey' is the name of the variable our 'secret key' is stored under.

So the portion of the pipeline where Databricks accesses key vault looks like:

![image](https://github.com/user-attachments/assets/5be9a5d0-1b4d-491e-b203-408bcd2a335d)


4. Create an Azure SQL table which contains the vaid order status which are

ON_HOLD
PAYMENT_REVIEW
PROCESSING
CLOSED
SUSPECTED_FRAUD
COMPLETE
PENDING
CANCELLED
PENDING_PAYMENT

This table will be referenced to check if all the data in the 'Landing' folder have one of the above 'Order' status. If they do then those records will be transferred to the 'Staging' folder else they will be transferred to the 'Discarded' folder. The Azure SQL Database and tables are easily accessible through 'Azure Data Studio'.


## Iteration 2

Now Inside the Databricks Notebook, we need to follow a certain flow:

1. Creating a Mount Point(If Not Created) -  We mount the ADLS Gen2 Container such that it can be accessed through Databricks through the secret scope.


~~~
   # Check if storage account is mounted
mounted = False
for x in dbutils.fs.mounts():
    if x.mountPoint == '/mnt/sales':
        mounted = True
        break
    else:
        mounted = False

        if mounted == False:
    extra_configs = {
    'fs.azure.account.key.cdstorageaccount1.blob.core.windows.net': dbutils.secrets.get('cd-secrets-databricks-scope', 'storagekey')}

    dbutils.fs.mount(
        source='wasbs://sales@cdstorageaccount1.blob.core.windows.net',
        mount_point='/mnt/sales',
        extra_configs=extra_configs
    )
~~~

2. Load the orders.csv into a dataframe

   ~~~
   orders_df = spark.read.csv(f'dbfs:/mnt/sales/Landing/orders.csv',inferSchema=True,header=True)
   ~~~


3. Check for 1st Data Validation viz. check if order_ids are repeated. If order_ids are repeated , it means data is faulty , hence entire data will be moved to discarded folder else we can go to the second validation. While exiting we flag a variable called 'errorMsg' which will suggest wherther order_id is repeated or not.

~~~
errorflg = False
orders_count = orders_df.count()
dist_orders = orders_df.select('order_id').distinct().count()
if orders_count!=dist_orders:
    errorflg = True
if errorflg == True:
    dbutils.fs.mv(f'/mnt/sales/Landing/{filename}','/mnt/sales/Discarded')
    dbutils.notebook.exit('{"errorflg":"True","errorMsg":"Orderid is repeated"}')
~~~

4. Second Data Validation...Check if all order_statuses are valid while referencing the Azure SQL Table which has all the valid order statuses.

~~~
jdbcHostname = "cd-server-24.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "cd_database_24"
jdbcTable = "dbo.valid_order_status"
jdbcUsername = "cdadmin"
jdbcPassword = dbutils.secrets.get('cd-secrets-databricks-scope', 'sql-passw')
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user ={jdbcUsername};password={jdbcPassword}"


df_status = spark.read.format("jdbc").option("url",jdbcUrl).option("dbtable",jdbcTable).load()

values_to_check = [df_status.collect()[i][0] for i in range (0,df_status.count())]
filtered_df = orders_df.filter(~orders_df.order_status.isin(values_to_check)).select('order_status')

if filtered_df.count()>0:
    errorflg = True

if errorflg == True:
    dbutils.fs.mv(f'/mnt/sales/Landing/{filename}','/mnt/sales/Discarded')
    dbutils.notebook.exit('{"errorflg":"True","errorMsg":"Invalid order status found"}')
else:
    dbutils.fs.mv(f'/mnt/sales/Landing/{filename}','/mnt/sales/Staging')
    #dbutils.notebook.exit('{"errorflg":"False","errorMsg":"All Good"}')

~~~

Once both these checks are successful, notebook will be exited and 'errorMsg' variable will show 'All Good' 


## Iteration 3

So far we have self-triggered or self-run our pipeline, we therefore can schedule a trigger to automate the pipeline. Also Notice in 'Iteration 2(point4), we are passing or hardcoding the filename(orders.csv). Since we plan to deploy a 'Schedule Trigger', we need to note that 'trigger' is a first point of contact and has the capability to capture the filenames of the newly added files in the Storage account and pass it to the pipeline. We will then no loger need to hardcode the name of the file. We therefore declare a parameter called 'filename' and give it the value '@triggerBody().filename'. We then access this value in the databricks notebook with the help of the following code:-

~~~
filename = dbutils.widgets.get('filename')
Fnamewithoutext = filename.split('.')[0]
~~~

So currently our pipeline looks like:

![AZURE1](https://github.com/user-attachments/assets/c9d74496-f493-434a-bc1b-9eb078fb6961)


## Iteration 4

1. We have a file called 'order_items.json' in AWS S3. This needs to be brought to the Azure Databricks notebook in our Pipeline. We again need Linked Services to access the S3 bucket. We also would need IAM permissions in the AWS account. The secret key of the AWS bucket needs to be stored in the Key vault as well.

2.  We have a 'customers' table in Azure SQL Database . We just need Linked services to connect.

   Once we have orders,order_items and customers data inside the Databricks Notebook. We will Join and aggregate further Results and store it in an Azure SQL Database. 

   The final Pipeline would now Look like:


![Azure2](https://github.com/user-attachments/assets/b8dc4521-acea-4e29-a319-4db599784834)



Hence by just initiating a Storage Trigger the entire pipeline is activated as shown in the Diagram above

   
