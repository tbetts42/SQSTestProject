using Amazon.SQS;
using Amazon.SQS.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SQSTestProject
{
    public class SQSQueue
    {
        private const int WAIT_TIME_SECONDS = 20;
        private const int MAX_NUMBER_OF_MESSAGES = 10;
        private AmazonSQSClient SQSClient { get; }
        public string QueueName { get; }
        private string _queueUrl;
        public string QueueUrl
        {
            get
            {
                if (_queueUrl == null)
                {
                    _queueUrl = SQSClient.GetQueueUrlAsync(QueueName).Result.QueueUrl;
                }
                return _queueUrl;
            }
        }

        private SQSQueue(AmazonSQSClient sqsClient, string queueName)
        {
            SQSClient = sqsClient;
            QueueName = queueName;
        }
        private SQSQueue(AmazonSQSClient sqsClient, string queueName, string queueUrl)
        {
            SQSClient = sqsClient;
            QueueName = queueName;
            _queueUrl = queueUrl;
        }

        /// <summary>
        /// Gets a reference to the SQS Queue,
        /// Creates it if it does not exist
        /// </summary>
        /// <param name="sqsClient"></param>
        /// <param name="queueName"></param>
        /// <returns></returns>
        internal static async Task<SQSQueue> GetQueue(AmazonSQSClient sqsClient,
            string queueName, bool createIfNotExists = true)
        {
            try
            {
                var queueUrlResponse = await sqsClient.GetQueueUrlAsync(queueName);
                var queueUrl = queueUrlResponse.QueueUrl;
                var queue = new SQSQueue(sqsClient, queueName, queueUrl);
                //await queue.ConfigureForLongPolling();
                return queue;
            }
            catch (QueueDoesNotExistException)
            {
                if (createIfNotExists)
                {
                    // create the queue
                    var createQueueRequest = new CreateQueueRequest(queueName);
                    var createQueueResponse = await sqsClient.CreateQueueAsync(createQueueRequest);
                    var queueUrl = createQueueResponse.QueueUrl;
                    var queue = new SQSQueue(sqsClient, queueName, queueUrl);
                    //await queue.ConfigureForLongPolling();
                    return queue;
                }
                else
                {
                    var nonExistantQueue = new SQSQueue(sqsClient, queueName);
                    return nonExistantQueue;
                }
            }
        }

        public async Task<string> GetFirstMessageBodyAsync()
        {
            var response = await SQSClient.ReceiveMessageAsync(QueueUrl);
            var message = response.Messages.First();
            return message.Body;

        }
        public async Task<SendMessageResponse> EnqueueMessageAsync(string jsonMessage)
        {
            var sendMessageRequest = new SendMessageRequest(QueueUrl, jsonMessage);
            var response = await SQSClient.SendMessageAsync(sendMessageRequest);
            return response;
        }

        /// <summary>
        /// Creates the SQS Queue if it doesn't exist.
        /// This should no longer be necessary, but left in for legacy checks
        /// </summary>
        /// <returns></returns>
        public async Task<bool> EnsureQueueExists()
        {
            if (await Exists())
            {
                return true;
            }

            // create the queue
            var createQueueRequest = new CreateQueueRequest(QueueName);
            var createQueueResponse = await SQSClient.CreateQueueAsync(createQueueRequest);
            _queueUrl = createQueueResponse.QueueUrl;
            return true;
        }

        public async Task<bool> Exists()
        {
            if (_queueUrl != null)
            {
                return true;
            }

            try
            {
                // check again if it exists before trying to create
                var queueUrlResponse = await SQSClient.GetQueueUrlAsync(QueueName);
                var _queueUrl = queueUrlResponse.QueueUrl;
                return true;
            }
            catch (QueueDoesNotExistException)
            {
                return false;
            }
        }
        private bool _longPollingConfigured = false;

        public async Task ConfigureForLongPolling()
        {
            if (!_longPollingConfigured)
            {
                var setQueueAttributesRequest = new SetQueueAttributesRequest()
                {
                    QueueUrl = this.QueueUrl
                };
                setQueueAttributesRequest.Attributes.Add("ReceiveMessageWaitTimeSeconds", WAIT_TIME_SECONDS.ToString());
                await SQSClient.SetQueueAttributesAsync(setQueueAttributesRequest);
                _longPollingConfigured = true;
            }
        }

        public async Task<string> DequeueMessage()
        {
            var receiveRequest = new ReceiveMessageRequest()
            {
                QueueUrl = QueueUrl,
                MaxNumberOfMessages = 1,
                WaitTimeSeconds = WAIT_TIME_SECONDS
            };

            var messageResponse = await SQSClient.ReceiveMessageAsync(receiveRequest);
            var firstMessage = messageResponse?.Messages?.FirstOrDefault();
            var messageBody = firstMessage?.Body;

            // Delete the message from the queue.

            // If this was re-designed, it would be better to ensure
            // the message has been fully handled before deleting the message.
            // But that process MUST finish in less than 7 minutes.
            if (firstMessage != null)
            {
                var deleteRequest = new DeleteMessageRequest(QueueUrl, firstMessage.ReceiptHandle);
                await SQSClient.DeleteMessageAsync(deleteRequest);
            }
            return messageBody;
        }

        /// <summary>
        /// Retrieves multiple messages from the queue (0..10)
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> DequeueMessages()
        {
            var receiveRequest = new ReceiveMessageRequest()
            {
                QueueUrl = QueueUrl,
                MaxNumberOfMessages = MAX_NUMBER_OF_MESSAGES,
                WaitTimeSeconds = WAIT_TIME_SECONDS
            };

            var messageResponse = await SQSClient.ReceiveMessageAsync(receiveRequest);
            var allMessages = messageResponse?.Messages;
            if (allMessages != null && allMessages.Any())
            {
                var allMessageBodies = allMessages.Select(a => a.Body).ToList();

                // delete the messages from the queue
                foreach (var message in allMessages)
                {
                    var deleteRequest = new DeleteMessageRequest(QueueUrl, message.ReceiptHandle);
                    await SQSClient.DeleteMessageAsync(deleteRequest);
                }

                return allMessageBodies;
            }
            else
            {
                return new List<string>();
            }
        }
    }
}
