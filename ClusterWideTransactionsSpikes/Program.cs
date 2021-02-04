using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;

namespace ClusterWideTransactionsSpikes
{
    static class Program
    {
        static void Main(string[] args)
        {

            var store = new DocumentStore()
            {
                Urls = new []{"http://localhost:8080"},
                Database = "ClusterWideTransactionsSpikes"
            }.Initialize();

            //simulates 10 messages coming in parallel all with different correlation IDs, and a duplicate
            var knownCorrelationValues = new[] { 1,2,3,4,10,6,7,8,9,10 };
            Parallel.ForEach(knownCorrelationValues, async i =>
            {
                using var session = store.OpenAsyncSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                });
                try
                {
                    var (compareExchange, sagaData) = await session.GetSagaData(i);
                    if (sagaData != null)
                    {
                        Console.WriteLine($"Updating saga for {i}");

                        sagaData.RandomValue = Guid.NewGuid().ToString() + DateTime.Now.Ticks;
                        await session.UpdateSagaData(compareExchange, sagaData);
                    }
                    else
                    {
                        Console.WriteLine($"Creating a new saga for {i}");

                        await session.CreateNewSagaData(new NewEmployeeSagaData()
                        {
                            Id = Guid.NewGuid().ToString(),
                            CorrelationProperty = i.ToString(),
                            FirstName = "John",
                            LastName = "Doe"
                        });
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed at Correlation {i}");
                    Console.WriteLine(ex.Message);
                }
            });

            Console.WriteLine("Started...");
            Console.Read();
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

        static async Task UpdateSagaData(this IAsyncDocumentSession session, CompareExchangeValue<string> ce, NewEmployeeSagaData sagaData)
        {
            var putOperation = new PutCompareExchangeValueOperation<string>(
                key: $"transactions/{nameof(NewEmployeeSagaData.CorrelationProperty)}/{sagaData.CorrelationProperty}",
                value: sagaData.Id,
                index: ce.Index);

            CompareExchangeResult<string> cmpXchgResult = session.Advanced.DocumentStore.Operations.Send(putOperation);

            if (cmpXchgResult.Successful == false) 
            {
                throw new Exception("Saga already updated by someone else.");
            }

            await session.SaveChangesAsync();
        }

        static async Task CreateNewSagaData(this IAsyncDocumentSession session, NewEmployeeSagaData newEmployeeSagaData)
        {
            await session.StoreAsync(newEmployeeSagaData);

            //this is assuming that only 1 saga instance of type NewEmployeeSagaData can have the given correlation value
            //it implies that there is no support for a message that comes in and activates 2 saga instances of the same type
            //which sounds reasonable
            session.Advanced.ClusterTransaction.CreateCompareExchangeValue($"transactions/{nameof(NewEmployeeSagaData.CorrelationProperty)}/{newEmployeeSagaData.CorrelationProperty}", newEmployeeSagaData.Id);

            await session.SaveChangesAsync();

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