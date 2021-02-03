using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;

namespace ClusterWideTransactionsSpikes
{
    class Program
    {
        static void Main(string[] args)
        {

            var store = new DocumentStore()
            {
                Urls = new []{"http://localhost:8080"},
                Database = "ClusterWideTransactionsSpikes"
            }.Initialize();

            var knownCorrelationValues = new[] { 1,2,3,4,10,6,7,8,9,10 };
            Parallel.ForEach(knownCorrelationValues, async i =>
            {
                try
                {
                    await StoreSagaData(store, new NewEmployeeSagaData()
                    {
                        CorrelationProperty = i.ToString(),
                        FirstName = "John",
                        LastName = "Doe"
                    });
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

        static async Task StoreSagaData(IDocumentStore store, NewEmployeeSagaData newEmployeeSagaData)
        {
            var sessionOptions = new SessionOptions
            {
                TransactionMode = TransactionMode.ClusterWide
            };

            using var session = store.OpenAsyncSession(sessionOptions);
            await session.StoreAsync(newEmployeeSagaData);

            var ce = session.Advanced.ClusterTransaction
                 .CreateCompareExchangeValue($"transactions/saga-correlation", newEmployeeSagaData.CorrelationProperty);

            await session.SaveChangesAsync();

            session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(ce);
            await session.SaveChangesAsync();
        }
    }

    public class NewEmployeeSagaData
    {
        public string Id { get; set; }

        public string CorrelationProperty { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

}