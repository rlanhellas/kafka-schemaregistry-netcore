using System;
using System.Threading;
using Confluent.Kafka;

namespace kafka_schemaregistry_ssl_netcore
{
    class Program
    {
        static void Main(string[] args)
        {
            StartConsumer();
        }

        private static void StartConsumer()
        {
            var config = new ConsumerConfig
            {
                GroupId = "netcore-consumer-test-02",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SslCertificateLocation =  "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.crt",
                SslKeyLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.key",
                SslCaLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/rootCA.crt",
                SecurityProtocol = SecurityProtocol.Ssl
            };

            using (var consumer = new ConsumerBuilder<string, string>(config).Build())
            {
                consumer.Subscribe("netcore-topic-test");
                CancellationTokenSource tokenSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    tokenSource.Cancel();
                };

                try
                {
                    while(true)
                    {
                        try
                        {
                            var cr = consumer.Consume(tokenSource.Token);
                            Console.WriteLine($"Key={cr.Key}, Value={cr.Value}");
                            // consumer.Commit();
                        }catch(ConsumeException e)
                        {
                            Console.WriteLine($"Error {e.Error.Reason}");
                        }
                    }
                }catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}
