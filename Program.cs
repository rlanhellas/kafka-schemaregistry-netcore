using System;
using System.Threading;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using io.confluent.examples.clients.basicavro;
using Confluent.Kafka.SyncOverAsync;

namespace kafka_schemaregistry_ssl_netcore
{
    class Program
    {

        private const string TOPIC_NAME = "netcore-topic-test-avro";

        static async System.Threading.Tasks.Task Main(string[] args)
        {
            if (args.Length > 0 && args[0] == "-p")
            {
                await StartProducerAsync();
            }
            else
            {
                StartConsumer();
            }
        }

        private static async System.Threading.Tasks.Task StartProducerAsync()
        {
            var schema = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081",
                RequestTimeoutMs = 5000,
                MaxCachedSchemas = 10
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                AutoRegisterSchemas = true,
                BufferBytes = 100
            };

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                SslCertificateLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.crt",
                SslKeyLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.key",
                SslCaLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/rootCA.crt",
                SecurityProtocol = SecurityProtocol.Ssl,
                Acks = Acks.None,
                ClientId = "producer-netcore-001"
            };

            using (var schemaClient = new CachedSchemaRegistryClient(schema))
            using (var producer = new ProducerBuilder<string, Payment>(config)
                                    .SetKeySerializer(new AvroSerializer<string>(schemaClient))
                                    .SetValueSerializer(new AvroSerializer<Payment>(schemaClient))
                                    .Build())
            {
                var msg = new Message<string, Payment>()
                {
                    Key = "1",
                    Value = new Payment
                    {
                        amount = 30,
                        id = "111"
                    }
                };

                await producer.ProduceAsync(TOPIC_NAME, msg).ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}");
                producer.Flush(TimeSpan.FromSeconds(10));
            }
        }

        private static void StartConsumer()
        {

            var schema = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081",
                RequestTimeoutMs = 5000,
                MaxCachedSchemas = 10
            };

            var config = new ConsumerConfig
            {
                GroupId = "netcore-consumer-test-02",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SslCertificateLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.crt",
                SslKeyLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/client.key",
                SslCaLocation = "/home/ronaldo.lanhellas/Workspace/tools/kafka/certs/rootCA.crt",
                SecurityProtocol = SecurityProtocol.Ssl
            };

            using (var schemaClient = new CachedSchemaRegistryClient(schema))
            using (var consumer = new ConsumerBuilder<string, Payment>(config)
                                .SetKeyDeserializer(new AvroDeserializer<string>(schemaClient).AsSyncOverAsync())
                                .SetValueDeserializer(new AvroDeserializer<Payment>(schemaClient).AsSyncOverAsync())
                                .Build())
            {
                consumer.Subscribe(TOPIC_NAME);
                CancellationTokenSource tokenSource = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    tokenSource.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(tokenSource.Token);
                            Console.WriteLine($"Key={cr.Key}, Value={cr.Value.amount}");
                            consumer.Commit();
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }
}
