// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

using Microsoft.SqlServer.Dac;
using Microsoft.Data.SqlClient;
using Azure.Storage.Blobs;

namespace Azure.Samples
{
    public static class PopulateDatabase
    {
        [FunctionName("PopulateDatabase")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try {
                
                log.LogInformation("C# HTTP trigger function processed a request.");

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);

                // get blob storage values from post body or app settings
                string storageConnectionString = data?.AzureStorageConnectionString ?? Environment.GetEnvironmentVariable("AzureStorageConnectionString");
                string containerName = data?.AzureStorageContainerName ?? Environment.GetEnvironmentVariable("AzureStorageContainerName");
                string fileName = data?.FileName ?? Environment.GetEnvironmentVariable("FileName");

                // get sql values from post body or app settings
                string sqlServerName = data?.sqlServerName ?? Environment.GetEnvironmentVariable("sqlServerName");
                string sqlDatabaseName = data?.sqlDatabaseName ?? Environment.GetEnvironmentVariable("sqlDatabaseName");
                string sqlPassword = data?.sqlPassword ?? Environment.GetEnvironmentVariable("sqlPassword");
                string sqlUser = data?.sqlUser ?? Environment.GetEnvironmentVariable("sqlUser");

                // check if bacpac name ends in .bacpac
                if (!(fileName.ToLower().EndsWith(".bacpac") || fileName.ToLower().EndsWith(".dacpac"))) {
                    return new BadRequestObjectResult("File to import must end in .bacpac or .dacpac");
                }

                // load the bacpac into a stream
                BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
                var blobStream = new MemoryStream();
                BlobClient blobClient = containerClient.GetBlobClient(fileName);
                await blobClient.DownloadToAsync(blobStream);

                // check if the database is active
                Boolean isConnectable = false;
                string connectionString = $"Server=tcp:{sqlServerName}.database.windows.net,1433;Initial Catalog=master;User ID={sqlUser};Password={sqlPassword};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
                int retryCount = 0;
                while (!isConnectable && retryCount < 20) {
                    retryCount++;
                    log.LogInformation($"Checking if server is available, attempt {retryCount}");
                    using (SqlConnection connection = new SqlConnection {ConnectionString = connectionString }) {
                        try {
                            await connection.OpenAsync();
                            isConnectable = true;
                        } catch (Exception ex) {
                            log.LogError($"Failed to connect to server, exception: {ex.Message}");
                        }
                    }
                    if (!isConnectable) {
                        log.LogInformation("Server is not connectable, sleeping for 3 seconds");
                        await Task.Delay(3000);
                    }
                }
                if (!isConnectable) {
                    return new BadRequestObjectResult("Master database is not connectable");
                }

                // import bacpac
                if (fileName.ToLower().EndsWith(".bacpac")) {
                    log.LogInformation("Server is connectable, importing bacpac");

                    var dacServices = new DacServices(connectionString);
                    var bacPackage = BacPackage.Load(blobStream);
                    var options = new DacImportOptions
                        {
                            ImportContributors = "Azure.Samples.ImportFixer"
                        };
                    dacServices.ImportBacpac(bacPackage, sqlDatabaseName, options, null);
                    log.LogInformation("Successfully imported bacpac");

                    return new OkObjectResult("Successfully imported bacpac");
                } else if (fileName.ToLower().EndsWith(".dacpac")) {
                    log.LogInformation("Server is connectable, importing dacpac");

                    var dacServices = new DacServices(connectionString);
                    var dacPackage = DacPackage.Load(blobStream);
                    var options = new DacDeployOptions
                        {
                            AdditionalDeploymentContributors = "Azure.Samples.ImportFixer"
                        };
                    dacServices.Deploy(dacPackage, sqlDatabaseName, false, options, null);
                    log.LogInformation("Successfully imported dacpac");

                    return new OkObjectResult("Successfully imported dacpac");
                } else {
                    return new BadRequestObjectResult("File to import must end in .bacpac or .dacpac");
                }
            } catch (Exception ex) {
                log.LogError($"Failed to import bacpac, exception: {ex.Message}");
                return new BadRequestObjectResult($"Failed to import bacpac: {ex.Message}");
            }
        }
    }
}
