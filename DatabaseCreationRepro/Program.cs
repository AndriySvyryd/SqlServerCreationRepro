using System;
using System.Data.SqlClient;
using System.Threading;

namespace DatabaseCreationRepro
{
    class Program
    {
        static void Main(string[] args)
        {
            for (var i = 0; i < 50; i++)
            {
                CreationTest("DatabaseCreationRepro" + i);
            }
        }

        private static void CreationTest(string databaseName)
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
                if (Exists(connection))
                {
                    Console.WriteLine($"Database {databaseName} already exists, dropping...");
                    Delete(connectionString);
                }
                else
                {
                    Console.WriteLine($"Database {databaseName} doesn't exists, creating...");
                    Create(connectionString);
                    SqlConnection.ClearPool(connection);
                    while (true)
                    {
                        if (Exists(connection))
                        {
                            //Console.WriteLine("Database created succesfully, dropping...");
                            Delete(connectionString);
                            return;
                        }
                        else
                        {
                            Console.WriteLine("Database still doesn't exist, waiting");
                            Thread.Sleep(TimeSpan.FromSeconds(30));
                        }
                    }
                }
            }
        }

        private static bool Exists(SqlConnection connection)
        {
            while (true)
            {
                try
                {
                    connection.Open();
                    connection.Close();

                    return true;
                }
                catch (SqlException e)
                {
                    if (e.Number == 4060 || e.Number == 1832 || e.Number == 5120)
                    {
                        return false;
                    }

                    Console.WriteLine("Error number: " + e.Number);
                    throw;
                }
            }
        }

        private static void Delete(string connectionString)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var database = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = "master";

            using (var masterConnection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                masterConnection.Open();
                using (var command = masterConnection.CreateCommand())
                {
                    command.CommandText = "DROP DATABASE " + database + ";";
                    command.CommandTimeout = 600;
                    command.ExecuteNonQuery();
                }
            }
        }

        private static void Create(string connectionString)
        {
            var connectionStringBuilder = new SqlConnectionStringBuilder(connectionString);
            var database = connectionStringBuilder.InitialCatalog;
            connectionStringBuilder.InitialCatalog = "master";

            using (var masterConnection = new SqlConnection(connectionStringBuilder.ToString()))
            {
                masterConnection.Open();
                using (var command = masterConnection.CreateCommand())
                {
                    command.CommandText = "CREATE DATABASE " + database + ";";
                    command.CommandTimeout = 600;
                    command.ExecuteNonQuery();
                }
            }
        }
    }
}
