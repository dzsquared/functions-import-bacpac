# Azure Function for populating a database by importing a sample bacpac or publishing a sample dacpac

Invoke this Function either via a POST request or in an Azure Logic App to perform DacFx import/publish operations from a bacpac/dacpac in Azure Blob Storage.

## Required settings
The following parameters must be set either in the POST body or in app settings - if set in both places the POST body value will take precedence.

- [AzureStorageConnectionString](https://docs.microsoft.com/azure/storage/blobs/storage-quickstart-blobs-dotnet?tabs=environment-variable-windows#copy-your-credentials-from-the-azure-portal)
- AzureStorageContainerName
- FileName (.bacpac or .dacpac files only)
- sqlServerName (prefix to .database.windows.net)
- sqlDatabaseName
- sqlPassword (*it is recommended that this value is stored in app settings*)
- sqlUser

## Includes contributor
In this example, a deployment contributor is included to adjust the import/publish process by moving all trigger creation to the end of the plan.