using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace SQSTestProject
{
    public static class AmazonSQSHelpers
    {

        public static AmazonSQSClient GetSQSClient(string region)
        {
            var regionEndpoint = RegionEndpoint.GetBySystemName(region);
            var SQSClient = new AmazonSQSClient(regionEndpoint);
            return SQSClient;
        }

        public static async Task<string> GetFirstMessageBodyAsync(this AmazonSQSClient sqsClient, string queueName)
        {
            var queueUrl = sqsClient.GetQueueUrlAsync(queueName).Result.QueueUrl;
            var response = await sqsClient.ReceiveMessageAsync(queueUrl);
            var message = response.Messages.First().Body;
            return message;
        }

        public static async Task<SQSQueue> GetQueue(this AmazonSQSClient sqsClient,
            string queueName, bool createIfNotExists = true)
        {
            return await SQSQueue.GetQueue(sqsClient, queueName, createIfNotExists);
        }

        public static async Task<bool> QueueExists(this AmazonSQSClient sqsClient,
            string queueName)
        {
            try
            {
                var queueUrlResponse = await sqsClient.GetQueueUrlAsync(queueName);
                var queueUrl = queueUrlResponse.QueueUrl;
                return true;
            }
            catch (QueueDoesNotExistException)
            {
                return false;
            }
        }
    }
}
