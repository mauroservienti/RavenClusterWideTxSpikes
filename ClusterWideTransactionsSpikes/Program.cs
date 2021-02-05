using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;

namespace ClusterWideTransactionsSpikes
{
    static class Program
    {
        static async Task Main(string[] args)
        {

            var store = new DocumentStore()
            {
                Urls = new[] { "http://localhost:8080" },
                Database = "ClusterWideTransactionsSpikes"
            }.Initialize();

            var rand = new Random((int)DateTime.Now.Ticks);

            var messages = Enumerable.Range(1, 10000).OrderBy(x => rand.Next());
            var duplicates = new []{3, 7};
            //var duplicates = Enumerable.Range(1, 1000).OrderBy(x => rand.Next());

            var knownCorrelationValues = messages.Concat(duplicates).OrderBy(x => rand.Next()).ToArray();

            var collector = new List<string>();

            Console.WriteLine("Started...");

            var tasks = new List<Task>();

            foreach (var correlationValue in knownCorrelationValues)
            {
                var t = ProcessUsingCompareExchange(store, correlationValue, collector);
                //var t = ProcessWithoutCompareExchange(store, correlationValue, collector);

                tasks.Add(t);
            }

            await Task.WhenAll(tasks);

            Console.WriteLine("Completed...");
            Console.WriteLine("------------");
            Console.WriteLine();

            foreach (var item in collector)
            {
                Console.WriteLine(item);
                Console.WriteLine();
            }

            Console.Read();
        }

        static async Task ProcessWithoutCompareExchange(IDocumentStore store, int correlationValue, List<string> collector)
        {
            using var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            });
            try
            {
                var sagaData = await session.LoadAsync<NewEmployeeSagaData>("NewEmployeeSagaDatas/" + correlationValue);
                if (sagaData != null)
                {
                    Console.WriteLine($"Updating saga for {correlationValue}");

                    sagaData.RandomValue = Guid.NewGuid().ToString() + DateTime.Now.Ticks;
                }
                else
                {
                    Console.WriteLine($"Creating a new saga for {correlationValue}");

                    await session.StoreAsync(new NewEmployeeSagaData()
                    {
                        Id = "NewEmployeeSagaDatas/" + correlationValue,
                        CorrelationProperty = correlationValue.ToString(),
                        FirstName = "John" + correlationValue,
                        LastName = "Doe" + correlationValue
                    });
                }

                await session.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                collector.Add($"Failed at Correlation {correlationValue}: {ex.Message}");
            }
        }

        static async Task ProcessUsingCompareExchange(IDocumentStore store, int correlationValue, List<string> collector)
        {
            using var session = store.OpenAsyncSession(new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            });
            try
            {
                var (compareExchange, sagaData) = await session.GetSagaData(correlationValue);
                if (sagaData != null)
                {
                    Console.WriteLine($"Updating saga for {correlationValue}");

                    sagaData.RandomValue = Guid.NewGuid().ToString() + DateTime.Now.Ticks;
                    await session.UpdateSagaData(compareExchange, sagaData);
                }
                else
                {
                    Console.WriteLine($"Creating a new saga for {correlationValue}");

                    await session.CreateNewSagaData(new NewEmployeeSagaData()
                    {
                        Id = "NewEmployeeSagaDatas/" + correlationValue,
                        CorrelationProperty = correlationValue.ToString(),
                        FirstName = "John" + correlationValue,
                        LastName = "Doe" + correlationValue
                    });
                }

                await session.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                collector.Add($"Failed at Correlation {correlationValue}: {ex.Message}");
            }
        }

        static async Task<(CompareExchangeValue<string>, NewEmployeeSagaData)> GetSagaData(this IAsyncDocumentSession session, int correlationPropertyValue)
        {
            var ce = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>($"transactions/{nameof(NewEmployeeSagaData.CorrelationProperty)}/{correlationPropertyValue}");

            if (ce != null)
            {
                var docId = ce.Value;
                var sagaData = await session.LoadAsync<NewEmployeeSagaData>(docId);

                return (ce, sagaData);
            }

            return (null, null);
        }

        static Task UpdateSagaData(this IAsyncDocumentSession session, CompareExchangeValue<string> currentCompareExchange, NewEmployeeSagaData sagaData)
        {
            var putOperation = new PutCompareExchangeValueOperation<string>(
                key: $"transactions/{nameof(NewEmployeeSagaData.CorrelationProperty)}/{sagaData.CorrelationProperty}",
                value: sagaData.Id,
                index: currentCompareExchange.Index);

            CompareExchangeResult<string> cmpXchgResult = session.Advanced.DocumentStore.Operations.Send(putOperation);

            if (cmpXchgResult.Successful == false)
            {
                throw new Exception($"Saga already updated by someone else. cmpXchgResult.Index {cmpXchgResult.Index}, sent index {currentCompareExchange.Index}.");
            }

            return Task.CompletedTask;
        }

        static async Task CreateNewSagaData(this IAsyncDocumentSession session, NewEmployeeSagaData newEmployeeSagaData)
        {
            await session.StoreAsync(newEmployeeSagaData);

            //this is assuming that only 1 saga instance of type NewEmployeeSagaData can have the given correlation value
            //it implies that there is no support for a message that comes in and activates 2 saga instances of the same type
            //which sounds reasonable
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"transactions/{nameof(NewEmployeeSagaData.CorrelationProperty)}/{newEmployeeSagaData.CorrelationProperty}", newEmployeeSagaData.Id);

            //Delete should be called only when deleting the saga instance (MarkAsComplete)
            //session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(ce);
            //await session.SaveChangesAsync();
        }
    }

    public class NewEmployeeSagaData
    {
        public string Id { get; set; }

        public string CorrelationProperty { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string RandomValue { get; internal set; }
    }

}