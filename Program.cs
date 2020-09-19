using Microsoft.Azure.ServiceBus;
using System;
using System.Text;
using System.Threading.Tasks;

namespace QueueExample
{
    class Program
    {
        // making service bus in Azure potal ... 
        // https://docs.microsoft.com/en-us/azure/service-bus-messaging/service-bus-quickstart-topics-subscriptions-portal

        public const string ServiceBusConnectionString = "Service Bus Conneciton String"; // copy from Azure portal
        public const string Queue_Name = "kereiQueue";
        public const string Topic_Name = "kereiTopic";
        public const string Subscription_Name1 = "subscription1";
        public const string Subscription_Name2 = "subscription2";
        static void Main(string[] args)
        {

         //   TestQueue();
            TestTopic();

            Console.ReadLine();

        }

        public static void TestTopic()
        {
            // Topic programming...
            // https://github.com/Azure/azure-service-bus/tree/master/samples/DotNet/GettingStarted/Microsoft.Azure.ServiceBus/BasicSendReceiveUsingTopicSubscriptionClient
            ISubscriptionClient client1 = new SubscriptionClient(ServiceBusConnectionString, Topic_Name, Subscription_Name1);
            ISubscriptionClient client2 = new SubscriptionClient(ServiceBusConnectionString, Topic_Name, Subscription_Name2);
            ISubscriptionClient client3 = new SubscriptionClient(ServiceBusConnectionString, Topic_Name, Subscription_Name2);
            ITopicClient topicClient = new TopicClient(ServiceBusConnectionString, Topic_Name);

            Console.WriteLine("Regist First Handler");
            client1.RegisterMessageHandler(async (msg, token) =>
            {
                Console.WriteLine($"consume 1: {Encoding.UTF8.GetString(msg.Body)}");
                await client1.CompleteAsync(msg.SystemProperties.LockToken);
            }
            , new MessageHandlerOptions((exp) => Task.CompletedTask)
            {
                MaxConcurrentCalls = 1, // Maximum number of Concurrent calls to the callback
                AutoComplete = true // Indicates whether MessagePump should automatically complete the messages after returning from User Callback
            });

            Console.WriteLine("Regist Second Handler");
            client2.RegisterMessageHandler(async (msg, token) =>
            {
                Console.WriteLine($"consume 2: {Encoding.UTF8.GetString(msg.Body)}");
                await client2.CompleteAsync(msg.SystemProperties.LockToken);
            }
            , new MessageHandlerOptions((exp) => Task.CompletedTask)
            {
                MaxConcurrentCalls = 4, // Maximum number of Concurrent calls to the callback
                AutoComplete = false // Indicates whether MessagePump should automatically complete the messages after returning from User Callback
            });

            Console.WriteLine("Regist third Handler");
            client3.RegisterMessageHandler(async (msg, token) =>
            {
                Console.WriteLine($"consume 3: {Encoding.UTF8.GetString(msg.Body)}");
                await client3.CompleteAsync(msg.SystemProperties.LockToken);
            }
            , new MessageHandlerOptions((exp) => Task.CompletedTask)
            {
                MaxConcurrentCalls = 1, // Maximum number of Concurrent calls to the callback
                AutoComplete = false // Indicates whether MessagePump should automatically complete the messages after returning from User Callback
            });

            Console.WriteLine("Send message to Queue");
            for (int x = 0; x < 30; x++)
            {
                topicClient.SendAsync(new Message(Encoding.UTF8.GetBytes($"message : {x}")));
            }

            Console.WriteLine("Send message completed!!");

        }

        public static void TestQueue()
        {
            // Queue Programming...
            // https://github.com/Azure/azure-service-bus/tree/master/samples/DotNet/GettingStarted/Microsoft.Azure.ServiceBus/BasicSendReceiveUsingQueueClient
            Console.WriteLine("Create Client");
            IQueueClient client1 = new QueueClient(ServiceBusConnectionString, Queue_Name);
            IQueueClient client2 = new QueueClient(ServiceBusConnectionString, Queue_Name);
            IQueueClient client3 = new QueueClient(ServiceBusConnectionString, Queue_Name);

            Console.WriteLine("Regist First Handler");
            client1.RegisterMessageHandler(async (msg, token) =>
            {
                Console.WriteLine($"consume 1: {Encoding.UTF8.GetString(msg.Body)}");
                await client1.CompleteAsync(msg.SystemProperties.LockToken);
            }
            , new MessageHandlerOptions((exp) => Task.CompletedTask)
            {
                MaxConcurrentCalls = 1, // Maximum number of Concurrent calls to the callback
                AutoComplete = true // Indicates whether MessagePump should automatically complete the messages after returning from User Callback
            });

            Console.WriteLine("Regist Second Handler");
            client2.RegisterMessageHandler(async (msg, token) =>
            {
                Console.WriteLine($"consume 2: {Encoding.UTF8.GetString(msg.Body)}");
                await client2.CompleteAsync(msg.SystemProperties.LockToken);
            }
            , new MessageHandlerOptions((exp) => Task.CompletedTask)
            {
                MaxConcurrentCalls = 4, // Maximum number of Concurrent calls to the callback
                AutoComplete = false // Indicates whether MessagePump should automatically complete the messages after returning from User Callback
            });

            Console.WriteLine("Send message to Queue");
            for (int x = 0; x < 30; x++)
            {
                client3.SendAsync(new Message(Encoding.UTF8.GetBytes($"message : {x}")));
            }
            Console.WriteLine("Send message completed!!");
        }
    }
}


