﻿using System;
using System.Collections.Generic;
using System.Linq;
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
        const string sagaDataCevPrefix = "cev";
        const int numberOfConcurrentUpdates = 50;
        const int maxRetryAttempts = 50;

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

            var pendingTasks = new List<Task<(bool Succeeded, int Index, string ErrorMessage)>>();
            for (var i = 0; i < numberOfConcurrentUpdates; i++)
            {
                pendingTasks.Add(TouchSaga(i, store, sagaDataStableId));
            }

            var results = await Task.WhenAll(pendingTasks);

            Console.WriteLine();
            Console.WriteLine("Execution completed");
            Console.WriteLine("--------------------------------------------------------------");

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
                .Except(failedUpdates.Select(fu=>fu.Index))
                .Except(sagaData.HandledIndexes)
                .ToList();

            if (diff.Any())
            {
                Console.WriteLine("Cannot find an update for the following indexes: ");
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

        private static async Task<(bool, int, string)> TouchSaga(int index, IDocumentStore store, string sagaDataStableId)
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
                    var cev = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>($"{sagaDataCevPrefix}/{sagaDataStableId}");

                    sagaData.HandledIndexes.Add(index);
                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(new CompareExchangeValue<string>($"{sagaDataCevPrefix}/{sagaDataStableId}", cev.Index, sagaDataStableId));

                    await session.SaveChangesAsync();

                    Console.WriteLine($"Index {index} updated successfully.");

                    return (true, index, String.Empty);
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
        public List<int> HandledIndexes { get; set; } = new List<int>();
    }
}