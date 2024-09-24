# Azure_ETL_Pipeline_CD

I did this project as part of a course 'Ultimate Big Data Master's Program' by Mr. Sumit Mittal. Credits for the dataset and teaching go to him. I would recommend his program to anyone interested in this space.

![image](https://github.com/user-attachments/assets/03d29f41-3797-433f-aa62-c0c3d33f0374)

Above is the basic diagram of the project. There are however many changes I have done iteratively to automate and optimize the pipeline, which I explain as I move on.

## Iteration 1

1. Create a storage account(ADLS Gen2)
   The structure is as follows:

    └── ADLS GEN2(Storage Account- DataLake)/
        └── Sales(Container)/
            ├── Landing(Folder)
            ├── Staging(Folder)
            └── Discarded(Folder)

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
