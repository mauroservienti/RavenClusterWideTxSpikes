using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;

namespace ClusterWideTransactionsSingleDocument
{
    class Program
    {
        const string sagaDataIdPrefix = "SampleSagaDatas";
        private const string sagaDataCevPrefix = "cev";

        static async Task Main(string[] args)
        {
            var dbName = "ClusterWideTransactionsSingleDocument";
            var store = new DocumentStore()
            {
                Urls = new[] { "http://localhost:8080", "http://localhost:8084", "http://localhost:52891" },
                Database = dbName
            }.Initialize();

            // await DeleteDatabase(store);
            // var dbRecord = new DatabaseRecord(dbName);
            // store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));

            var sagaDataStableId = sagaDataIdPrefix + "/" + Guid.NewGuid();
            Console.WriteLine($"Saga stable ID: {sagaDataStableId}");

            using var storeItOnceSession = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide});
            storeItOnceSession.Advanced.UseOptimisticConcurrency = false;
            await storeItOnceSession.StoreAsync(new SampleSagaData() {Id = sagaDataStableId});
            storeItOnceSession.Advanced.ClusterTransaction.CreateCompareExchangeValue($"{sagaDataCevPrefix}/{sagaDataStableId}", sagaDataStableId);
            await storeItOnceSession.SaveChangesAsync();

            var pendingTasks = new List<Task>();
            for (var i = 0; i < 10; i++)
            {
                pendingTasks.Add(TouchSaga(i, store, sagaDataStableId));
            }

            await Task.WhenAll(pendingTasks);
        }

        private static async Task TouchSaga(int index, IDocumentStore store, string sagaDataStableId)
        {
            var attempts = 0;
            Exception lastError = null;

            while (attempts <= 10)
            {
                try
                {
                    using var session = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide});
                    session.Advanced.UseOptimisticConcurrency = false;

                    var sagaData = await session.LoadAsync<SampleSagaData>(sagaDataStableId);
                    var cev = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>($"{sagaDataCevPrefix}/{sagaDataStableId}");

                    sagaData.HandledMessages.Add(index);
                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(new CompareExchangeValue<string>($"{sagaDataCevPrefix}/{sagaDataStableId}", cev.Index, sagaDataStableId));

                    await session.SaveChangesAsync();

                    Console.WriteLine($"Index {index} updated successfully.");

                    return;
                }
                catch (Exception ex)
                {
                    attempts++;
                    lastError = ex;
                }
            }

            Console.WriteLine($"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");
        }

        public static async Task DeleteDatabase(IDocumentStore store)
        {
            // Periodically the delete will throw an exception because Raven has the database locked
            // To solve this we have a retry loop with a delay
            var triesLeft = 3;

            while (triesLeft-- > 0)
            {
                try
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(store.Database, hardDelete: true));
                }
                catch
                {
                    if (triesLeft == 0)
                    {
                        throw;
                    }

                    await Task.Delay(250);
                }
            }
        }
    }

    class SampleSagaData
    {
        public string Id { get; set; }
        public List<int> HandledMessages { get; set; } = new List<int>();
    }
}