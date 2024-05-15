using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.File;

namespace UnzipFileFunction
{
    public class UnzipFiles
    {
        [FunctionName("UnzipFiles")]
        public async Task Run([BlobTrigger("reject-files/{name}")]Stream myBlob, string name, ILogger logger)
        {
            logger.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");

            try
            {
                // Define the output container where unzipped files will be stored
                var outputContainerName = "unzipped-files";


                // Connect to Azure Blob Storage
                var storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                var blobServiceClient = new BlobServiceClient(storageConnectionString);
                var blobContainerClient = blobServiceClient.GetBlobContainerClient(outputContainerName);

                // Create the output container if it doesn't exist
                await blobContainerClient.CreateIfNotExistsAsync();


                // Read the content of the blob into a MemoryStream
                using (var archive = new ZipArchive(myBlob, ZipArchiveMode.Read))
                {
                    foreach (var entry in archive.Entries)
                    {
                        // Skip directories
                        if (string.IsNullOrEmpty(entry.Name))
                        {
                            continue;
                        }
                        
                        
                        // Create a blob client for the output file
                        var blobClient = blobContainerClient.GetBlobClient(entry.FullName);

                        // Upload the extracted file to the output blob
                        using (var entryStream = entry.Open())
                        {
                            byte[] buffer = new byte[16 * 1024];
                            using (var outputStream = await blobClient.OpenWriteAsync(true))
                            {
                                int read;
                                while ((read = entryStream.Read(buffer, 0, buffer.Length)) > 0)
                                {
                                    outputStream.Write(buffer, 0, read);
                                }

                                await entryStream.CopyToAsync(outputStream);
                            }
                        }

                        logger.LogInformation($"Extracted file '{entry.FullName}'");
                    }
                }

                logger.LogInformation("Extraction complete.");
            }
            catch (Exception ex)
            {
                logger.LogError("Error: {0}", ex.Message);
            }
        }
    }
}
