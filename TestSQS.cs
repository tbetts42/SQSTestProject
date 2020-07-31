using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SQSTestProject
{
    [TestClass]
    public class TestSQS
    {
        [TestMethod]
        public void CheckQueueExists()
        {
            var awsRegion = "us-east-1";
            var sqsClient = AmazonSQSHelpers.GetSQSClient(awsRegion);
            var queueName = "";
            var sqsQueue = sqsClient.GetQueue(queueName).Result;
            Assert.IsNotNull(sqsQueue);

            // enqueue a message
            // Create a message and add it to the queue.
            var jsonMessage = "{'test':'hello'}";
            var messageResponse = sqsQueue.EnqueueMessageAsync(jsonMessage).Result;

            Assert.AreEqual(System.Net.HttpStatusCode.OK, messageResponse.HttpStatusCode);

            // peek at the queue
            var message = sqsQueue.GetFirstMessageBodyAsync().Result;
            Assert.IsTrue(message != null, "No message found on the Queue.");
        }
    }
}
