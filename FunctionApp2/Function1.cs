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
    }
}