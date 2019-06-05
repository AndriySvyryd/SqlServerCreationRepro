using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace DatabaseCreationRepro
{
    static class Program
    {
        static async Task Main()
        {
            var tasks = new List<Task>();
            for (var i = 0; i < 128; i++)
            {
                tasks.Add(CreationTestAsync("DatabaseCreationRepro" + i));
            }

            await Task.WhenAll(tasks);
        }

        private static async Task CreationTestAsync(string databaseName)
        {
            var connectionString = new SqlConnectionStringBuilder()
            {
                InitialCatalog = databaseName,
                DataSource = "(localdb)\\MSSQLLocalDB",
                IntegratedSecurity = true,
                ConnectTimeout = 600,
                ConnectRetryCount = 0
            }.ToString();

            using (var connection = new SqlConnection(connectionString))
            {
                if (await ExistsAsync(connection))
                {
                    Console.WriteLine($"Database {databaseName} already exists, dropping...");
                    await DeleteAsync(connectionString);
                }
                else
                {
                    Console.WriteLine($"Database {databaseName} doesn't exists, creating...");
                    if (!await CreateAsync(connectionString))
                    {
                        return;
                    }

                    SqlConnection.ClearPool(connection);
                    while (true)
                    {
                        if (await ExistsAsync(connection))
                        {
                            Console.WriteLine($"Database {databaseName} created succesfully, dropping...");
                            await DeleteAsync(connectionString);
                            return;
                        }
                        else
                        {
                            Console.WriteLine($"Database {databaseName} still doesn't exist, waiting");
                            Thread.Sleep(TimeSpan.FromSeconds(30));
                        }
                    }
                }
            }
        }

        private static async Task<bool> ExistsAsync(SqlConnection connection)
        {
            while (true)
            {
                try
                {
                    if (connection.State != ConnectionState.Closed)
                    {
                        connection.Close();
                    }
                    await connection.OpenAsync();
                    connection.Close();

                    return true;
                }
                catch (SqlException e)
                {
                    if (e.Number == 4060 || e.Number == 1832 || e.Number == 5120)
                    {
                        return false;
                    }

                    if (ShouldRetryOn(e))
                    {
                        Console.WriteLine("Retrying connection on error number: " + e.Number);
                        Thread.Sleep(TimeSpan.FromSeconds(15));
                        continue;
                    }

                    Console.WriteLine("Unable to connect: " + e.ToString());
                }
            }
        }

        private static async Task DeleteAsync(string connectionString)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var database = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = "master";

            while (true)
            {
                try
                {
                    using (var masterConnection = new SqlConnection(connectionStringBuilder.ToString()))
                    {
                        masterConnection.Open();
                        using (var command = masterConnection.CreateCommand())
                        {
                            command.CommandText = @"IF SERVERPROPERTY('EngineEdition') <> 5
BEGIN
    ALTER DATABASE " + database + @" SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
END
DROP DATABASE " + database + ";";
                            command.CommandTimeout = 600;
                            await command.ExecuteNonQueryAsync();
                            return;
                        }
                    }
                }
                catch (SqlException e)
                {
                    if (ShouldRetryOn(e))
                    {
                        Console.WriteLine($"Retrying {database} deletion on error number: " + e.Number);
                        Thread.Sleep(TimeSpan.FromSeconds(15));
                        continue;
                    }

                    Console.WriteLine($"Deleting {database} failed: " + e.ToString());
                    return;
                }
            }
        }

        private static async Task<bool> CreateAsync(string connectionString)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var database = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = "master";

            while (true)
            {
                try
                {
                    using (var masterConnection = new SqlConnection(connectionStringBuilder.ToString()))
                    {
                        masterConnection.Open();
                        using (var command = masterConnection.CreateCommand())
                        {
                            command.CommandText = "CREATE DATABASE " + database + ";";
                            command.CommandTimeout = 600;
                            await command.ExecuteNonQueryAsync();

                            return true;
                        }
                    }
                }
                catch (SqlException e)
                {
                    if (ShouldRetryOn(e))
                    {
                        Console.WriteLine($"Retrying {database} creation on error number: " + e.Number);
                        Thread.Sleep(TimeSpan.FromSeconds(15));
                        continue;
                    }

                    Console.WriteLine($"Creating {database} failed: " + e.ToString());
                    return false;
                }
            }
        }

        public static bool ShouldRetryOn(Exception ex)
        {
            if (ex is SqlException sqlException)
            {
                foreach (SqlError err in sqlException.Errors)
                {
                    switch (err.Number)
                    {
                        // SQL Error Code: 49920
                        // Cannot process request. Too many operations in progress for subscription "%ld".
                        // The service is busy processing multiple requests for this subscription.
                        // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for operation status.
                        // Wait until pending requests are complete or delete one of your pending requests and retry your request later.
                        case 49920:
                        // SQL Error Code: 49919
                        // Cannot process create or update request. Too many create or update operations in progress for subscription "%ld".
                        // The service is busy processing multiple create or update requests for your subscription or server.
                        // Requests are currently blocked for resource optimization. Query sys.dm_operation_status for pending operations.
                        // Wait till pending create or update requests are complete or delete one of your pending requests and
                        // retry your request later.
                        case 49919:
                        // SQL Error Code: 49918
                        // Cannot process request. Not enough resources to process request.
                        // The service is currently busy.Please retry the request later.
                        case 49918:
                        // SQL Error Code: 41839
                        // Transaction exceeded the maximum number of commit dependencies.
                        case 41839:
                        // SQL Error Code: 41325
                        // The current transaction failed to commit due to a serializable validation failure.
                        case 41325:
                        // SQL Error Code: 41305
                        // The current transaction failed to commit due to a repeatable read validation failure.
                        case 41305:
                        // SQL Error Code: 41302
                        // The current transaction attempted to update a record that has been updated since the transaction started.
                        case 41302:
                        // SQL Error Code: 41301
                        // Dependency failure: a dependency was taken on another transaction that later failed to commit.
                        case 41301:
                        // SQL Error Code: 40613
                        // Database XXXX on server YYYY is not currently available. Please retry the connection later.
                        // If the problem persists, contact customer support, and provide them the session tracing ID of ZZZZZ.
                        case 40613:
                        // SQL Error Code: 40501
                        // The service is currently busy. Retry the request after 10 seconds. Code: (reason code to be decoded).
                        case 40501:
                        // SQL Error Code: 40197
                        // The service has encountered an error processing your request. Please try again.
                        case 40197:
                        // SQL Error Code: 10929
                        // Resource ID: %d. The %s minimum guarantee is %d, maximum limit is %d and the current usage for the database is %d.
                        // However, the server is currently too busy to support requests greater than %d for this database.
                        // For more information, see http://go.microsoft.com/fwlink/?LinkId=267637. Otherwise, please try again.
                        case 10929:
                        // SQL Error Code: 10928
                        // Resource ID: %d. The %s limit for the database is %d and has been reached. For more information,
                        // see http://go.microsoft.com/fwlink/?LinkId=267637.
                        case 10928:
                        // SQL Error Code: 10060
                        // A network-related or instance-specific error occurred while establishing a connection to SQL Server.
                        // The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server
                        // is configured to allow remote connections. (provider: TCP Provider, error: 0 - A connection attempt failed
                        // because the connected party did not properly respond after a period of time, or established connection failed
                        // because connected host has failed to respond.)"}
                        case 10060:
                        // SQL Error Code: 10054
                        // A transport-level error has occurred when sending the request to the server.
                        // (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)
                        case 10054:
                        // SQL Error Code: 10053
                        // A transport-level error has occurred when receiving results from the server.
                        // An established connection was aborted by the software in your host machine.
                        case 10053:
                        // SQL Error Code: 1205
                        // Deadlock
                        case 1205:
                        // SQL Error Code: 233
                        // The client was unable to establish a connection because of an error during connection initialization process before login.
                        // Possible causes include the following: the client tried to connect to an unsupported version of SQL Server;
                        // the server was too busy to accept new connections; or there was a resource limitation (insufficient memory or maximum
                        // allowed connections) on the server. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by
                        // the remote host.)
                        case 233:
                        // SQL Error Code: 121
                        // The semaphore timeout period has expired
                        case 121:
                        // SQL Error Code: 64
                        // A connection was successfully established with the server, but then an error occurred during the login process.
                        // (provider: TCP Provider, error: 0 - The specified network name is no longer available.)
                        case 64:
                        // DBNETLIB Error Code: 20
                        // The instance of SQL Server you attempted to connect to does not support encryption.
                        case 20:
                        // This exception can be thrown even if the operation completed successfully, so it's safer to let the application fail.
                        // DBNETLIB Error Code: -2
                        case -2:
                            return true;
                    }
                }

                return false;
            }

            return ex is TimeoutException;
        }
    }
}
