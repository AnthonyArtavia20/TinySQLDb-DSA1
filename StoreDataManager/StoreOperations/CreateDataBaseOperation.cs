using System;
using System.IO;
using System.Linq;
using Entities;

namespace StoreDataManager.StoreOperations
{
    public class CreateDatabaseOperation
    {
        private readonly string DataPath;
        private readonly string SystemCatalogPath;

        public CreateDatabaseOperation(string dataPath, string systemCatalogPath)
        {
            DataPath = dataPath;
            SystemCatalogPath = systemCatalogPath;
        }

        public OperationStatus Execute(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName))
            {
                Console.WriteLine("Error: El nombre de la base de datos no puede estar vacío.");
                return OperationStatus.Error;
            }

            if (DatabaseExists(databaseName))
            {
                Console.WriteLine($"Error: La base de datos '{databaseName}' ya existe.");
                return OperationStatus.Error;
            }

            int newDatabaseId = GetNextDatabaseId();
            AddDatabaseToCatalog(newDatabaseId, databaseName);

            string databasePath = Path.Combine(DataPath, databaseName);
            Directory.CreateDirectory(databasePath);

            Console.WriteLine($"Base de datos '{databaseName}' creada exitosamente.");
            return OperationStatus.Success;
        }

        private bool DatabaseExists(string databaseName)
        {
            var databases = ReadFromSystemDatabases();
            return databases.Any(db => db.Name == databaseName);
        }

        private List<(int Id, string Name)> ReadFromSystemDatabases()
        {
            var databaseList = new List<(int, string)>();
            string systemDatabasesFilePath = Path.Combine(SystemCatalogPath, "SystemDatabases.databases");

            using (var reader = new BinaryReader(File.Open(systemDatabasesFilePath, FileMode.Open)))
            {
                while (reader.BaseStream.Position != reader.BaseStream.Length)
                {
                    int id = reader.ReadInt32();
                    string name = reader.ReadString();
                    databaseList.Add((id, name));
                }
            }
            return databaseList;
        }

        private int GetNextDatabaseId()
        {
            var databases = ReadFromSystemDatabases();
            if (databases.Count == 0)
            {
                return 1;
            }
            return databases.Max(db => db.Id) + 1;
        }

        private void AddDatabaseToCatalog(int id, string databaseName)
        {
            string systemDatabasesFilePath = Path.Combine(SystemCatalogPath, "SystemDatabases.databases");

            using (var writer = new BinaryWriter(File.Open(systemDatabasesFilePath, FileMode.Append)))
            {
                writer.Write(id);
                writer.Write(databaseName);
            }
        }
    }
}