using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure;
using Microsoft.Azure.Management.CosmosDB.Fluent;
using Microsoft.Azure.Cosmos;

namespace FunctionApp2
{
    public static class Function1
    {

        [FunctionName("Function1_HttpStart")]
        public static async Task<IActionResult> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequest req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic body = JsonConvert.DeserializeObject(requestBody);
            string csvContent = body?.content;
            if (csvContent == null)
            {
                return new BadRequestObjectResult("Please pass a name on the query string or in the request body");
            }
            byte[] data = Convert.FromBase64String(csvContent);
            string decodedString = System.Text.Encoding.UTF8.GetString(data);
            string[] dataArray = decodedString.Split(new[] { "\n" }, StringSplitOptions.None);
            await InsertFileToCosmosDB(JsonConvert.SerializeObject(dataArray));
            string instanceId = await starter.StartNewAsync("FanOutFanIn", null, JsonConvert.SerializeObject(dataArray));

            log.LogInformation($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

        [FunctionName("FanOutFanIn")]
        public static async Task<int> Run(
        [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var parallelTasks = new List<Task<int>>();
            var data = context.GetInput<string>();
            dynamic csvArray = JsonConvert.DeserializeObject<string[]>(data);
            for (int i = 0; i < 10; i++)
            {
                string[] subArray = new string[10000];
                Array.Copy(csvArray, i * 10000, subArray, 0, 10000);
                Task<int> task = context.CallActivityAsync<int>("getSumOfArray", subArray);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);

            int sum = parallelTasks.Sum(t => t.Result);
            return sum;
        }

        [FunctionName("getSumOfArray")]
        public static long getSumOfArray([ActivityTrigger] string[] arrayData)
        {
            long result = 10;
            for (int i = 0; i < arrayData.Length - 1; i++)
            {
                result += Int32.Parse(arrayData[i]);
            }
            return result;
        }

        [FunctionName("InsertFileToCosmosDB")]
        public async static Task InsertFileToCosmosDB([ActivityTrigger] string jsonData)
        {
            CosmosClient cosmosClient = new CosmosClient("AccountEndpoint=https://cloud-common.documents.azure.com:443/;AccountKey=tGxjzHJjk50peP5cOddqsLTCn4BTelafzbCmxLsoSw9LjsGbd85rekPyymH9VYZokmEzjpU4AKEKUgMI5yPBFw==;");
            Database database = cosmosClient.GetDatabase("LucND-Container");
            Container container = database.GetContainer("LucND-Container");
            Random rnd = new Random();
            string id = rnd.Next(0, 10000).ToString();
            FileModel file = new FileModel(id, "test.csv", jsonData);
            await container.CreateItemAsync<FileModel>(file);
        }

           
    }

    public class FileModel
    {
        [JsonProperty(PropertyName = "id")]
        public string ID { get; set; }

        [JsonProperty(PropertyName = "filename")]
        public string FileName { get; set; }

        [JsonProperty(PropertyName ="content")]
        public string Content { get; set; }

        public FileModel(string id, string filename, string content)
        {
            this.ID = id;
            this.FileName = filename;
            this.Content = content;
        }
    
    }
}