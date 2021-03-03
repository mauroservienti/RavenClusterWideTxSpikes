using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide.Operations;

namespace ClusterWideTransactionsSingleDocument
{
    class Program
    {
        //database must exist and must be replicated across nodes
        static readonly string[] clusterNodesUrls = new[] {"http://localhost:7070", "http://localhost:7071", "http://localhost:7072"};
        const string dbName = "ClusterWideTransactionsSingleDocument";

        const string sagaDataIdPrefix = "SampleSagaDatas";
        const string sagaDataCevPrefix = "cev";
        const int numberOfConcurrentUpdates = 50;
        const int maxRetryAttempts = 50;

        static async Task Main(string[] args)
        {
            var store = new DocumentStore()
            {
                Urls = clusterNodesUrls,
                Database = dbName
            }.Initialize();

            // await DeleteDatabase(store);
            // var dbRecord = new DatabaseRecord(dbName);
            // store.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));

            (bool Succeeded, int Index, string ErrorMessage)[] results = null;
            var succeeded = true;
            string sagaDataStableId = null;

            while (succeeded)
            {
                sagaDataStableId = sagaDataIdPrefix + "/" + Guid.NewGuid();
                results = await Execute(store, sagaDataStableId);

                succeeded = await ValidateResults(store, sagaDataStableId, results);
                if (succeeded)
                {
                    Console.WriteLine();
                    Console.WriteLine("Execution completed, going to try again...");
                }
            }

            Console.WriteLine();
            Console.WriteLine("-----------------------------------------------------------------------------------------------------");
            Console.WriteLine($"Execution completed with errors for document '{sagaDataStableId}'");
            Console.WriteLine("-----------------------------------------------------------------------------------------------------");
            Console.WriteLine();

            var succeededUpdates = results.Where(r => r.Succeeded).ToList();
            var failedUpdates = results.Where(r => !r.Succeeded).ToList();

            if (failedUpdates.Any())
            {
                Console.WriteLine($"{failedUpdates.Count} updated failed.");
                foreach (var failure in failedUpdates)
                {
                    Console.WriteLine($"\t{failure.ErrorMessage}");
                }
            }

            using var checkSession = store.OpenAsyncSession();
            var sagaData = await checkSession.LoadAsync<SampleSagaData>(sagaDataStableId);

            Console.WriteLine($"# of indexes stored in the document:\t{sagaData.HandledIndexes.Count}");
            Console.WriteLine($"# of reported successful updates:\t{succeededUpdates.Count}");
            Console.WriteLine();

            var diff = Enumerable.Range(0, numberOfConcurrentUpdates - 1)
                .Except(failedUpdates.Select(fu => fu.Index))
                .Except(sagaData.HandledIndexes)
                .ToList();

            if (diff.Any())
            {
                Console.WriteLine("Cannot find an update for the following index(es): ");
                foreach (var idx in diff)
                {
                    Console.WriteLine($"\t{idx}");
                }
            }
            else
            {
                Console.WriteLine("Found all expected indexes.");
            }
        }

        private static async Task<bool> ValidateResults(IDocumentStore store, string sagaDataStableId, (bool Succeeded, int Index, string ErrorMessage)[] results)
        {
            using var checkSession = store.OpenAsyncSession();
            var sagaData = await checkSession.LoadAsync<SampleSagaData>(sagaDataStableId);

            var updatesFailedDueToConcurrency = results.Where(r => !r.Succeeded).ToList();

            var diff = Enumerable.Range(0, numberOfConcurrentUpdates - 1)
                .Except(updatesFailedDueToConcurrency.Select(fu => fu.Index))
                .Except(sagaData.HandledIndexes)
                .ToList();

            return diff.Count == 0;
        }

        static async Task<(bool, int, string)[]> Execute(IDocumentStore store, string sagaDataStableId)
        {
            Console.WriteLine($"Test document ID: {sagaDataStableId}");

            using (var storeItOnceSession = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide}))
            {
                storeItOnceSession.Advanced.UseOptimisticConcurrency = false;

                await storeItOnceSession.StoreAsync(new SampleSagaData()
                {
                    Id = sagaDataStableId,
                    SyncGuid = Guid.Empty.ToString()
                });
                storeItOnceSession.Advanced.ClusterTransaction.CreateCompareExchangeValue($"{sagaDataCevPrefix}/{sagaDataStableId}", new SampleSagaDataCevValue
                {
                    Id = sagaDataStableId,
                    SyncGuid = Guid.Empty.ToString()
                });
                await storeItOnceSession.SaveChangesAsync();
            }

            Console.WriteLine($"Test document created. Ready to try to concurrently update document {numberOfConcurrentUpdates} times.");
            Console.WriteLine();

            var pendingTasks = new List<Task<(bool Succeeded, int Index, string ErrorMessage)>>();
            for (var i = 0; i < numberOfConcurrentUpdates; i++)
            {
                pendingTasks.Add(TouchSaga(i, store, sagaDataStableId));
            }

            var results = await Task.WhenAll(pendingTasks);

            return results;
        }

        static async Task<(bool, int, string)> TouchSaga(int index, IDocumentStore store, string sagaDataStableId)
        {
            var attempts = 0;
            Exception lastError = null;

            while (attempts <= maxRetryAttempts)
            {
                try
                {
                    using var session = store.OpenAsyncSession(new SessionOptions() {TransactionMode = TransactionMode.ClusterWide});
                    session.Advanced.UseOptimisticConcurrency = false;

                    var sagaData = await session.LoadAsync<SampleSagaData>(sagaDataStableId);
                    var cev = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<SampleSagaDataCevValue>($"{sagaDataCevPrefix}/{sagaDataStableId}");

                    // handle concurrency between the reads
                    if (sagaData.SyncGuid != cev.Value.SyncGuid)
                    {
                        throw new Exception("Saga and compare exchange value are out of sync."); //retry
                    }

                    var newSyncGuid = Guid.NewGuid().ToString();
                    sagaData.HandledIndexes.Add(index);

                    cev.Value.SyncGuid = newSyncGuid;
                    sagaData.SyncGuid = newSyncGuid;

                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(cev);

                    await session.SaveChangesAsync();

                    Console.WriteLine($"Index {index} updated successfully after {attempts} attempts.");

                    return (true, index, string.Empty);
                }
                catch (Exception ex)
                {
                    attempts++;
                    lastError = ex;
                }
            }

            Console.WriteLine($"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");

            return (false, index, $"Failed after {attempts} attempts, while handling index {index}, last error: {lastError?.Message}");
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
        public string SyncGuid { get; set; }
        public List<int> HandledIndexes { get; set; } = new List<int>();
    }

    class SampleSagaDataCevValue
    {
        public string Id { get; set; }
        public string SyncGuid { get; set; }
    }
}