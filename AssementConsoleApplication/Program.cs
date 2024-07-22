using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using Dapper;

public class Worker
{
    private readonly string workerName;
    private readonly string connectionString;
    private readonly Timer timer;
    private readonly List<Guid> assignedItems = new List<Guid>();

    public Worker(string workerName, string connectionString)
    {
        this.workerName = workerName;
        this.connectionString = connectionString;
        timer = new Timer(ProcessItems, null, 0, 30000);
        //timer = new Timer(ProcessItems, null, 0, 1000);
    }

    public void AssignItems(List<Guid> items)
    {
        assignedItems.Clear();
        assignedItems.AddRange(items);
    }

    private void ProcessItems(object state)
    {
        //using (var connection = new SqlConnection(connectionString))
        using (var connection = new SqlConnection(connectionString))
        {
            var itemsToProcess = new List<Guid>(assignedItems);
            foreach (var id in assignedItems)
            {
                connection.Execute("UPDATE Items SET Value = Value + 1, CurrentWorker = @workerName WHERE ID = @id", new { workerName, id });
            }

            Console.WriteLine($"{workerName} is processing items: {string.Join(", ", assignedItems)}");
        }
    }

    public void Shutdown()
    {
        timer.Dispose();
    }
}

class Program
{
    static List<Worker> workers = new List<Worker>();
    static string connectionString = "Data Source=CIPL1162DOTNET\\SQLEXPRESS2022;Initial Catalog=WorkerDb;Persist Security Info=True;User ID=sa;Password=Colan123;TrustServerCertificate=true";

    static void Main(string[] args)
    {
        InitializeWorkers();

        Console.WriteLine("Press Enter to add a new worker...");
        Console.ReadLine();
        AddWorker();

        //Console.WriteLine("Press Enter to add a new worker...");
        //Console.ReadLine();
        //AddWorker();

        //Console.WriteLine("Press Enter to remove a worker...");
        //Console.ReadLine();
        //RemoveWorker();

        Console.WriteLine("Press Enter to remove a worker...");
        Console.ReadLine();
        RemoveWorker();

        Console.WriteLine("Press Enter to exit...");
        Console.ReadLine();
    }

    static void InitializeWorkers()
    {
        workers.Add(new Worker("worker1", connectionString));
        workers.Add(new Worker("worker2", connectionString));
        workers.Add(new Worker("worker3", connectionString));
        DistributeWorkload();
    }

    static void AddWorker()
    {
        var newWorker = new Worker("worker" + (workers.Count + 1), connectionString);
        workers.Add(newWorker);
        DistributeWorkload();
    }

    static void RemoveWorker()
    {
        if (workers.Count == 0) return;

        var workerToRemove = workers[0];
        workerToRemove.Shutdown();
        workers.RemoveAt(0);
        DistributeWorkload();
    }

    static void DistributeWorkload()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            var items = connection.Query<Guid>("SELECT ID FROM Items").ToList();
            int workersCount = workers.Count;
            for (int i = 0; i < workersCount; i++)
            {
                var workerItems = items.Where((item, index) => index % workersCount == i).ToList();
                workers[i].AssignItems(workerItems);
            }
        }
    }
}
