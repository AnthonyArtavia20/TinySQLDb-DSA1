using Entities;

namespace StoreDataManager.StoreOperations
{
    public class SetOperation
    {
        private readonly string DataPath;
        private readonly string SystemCatalogPath;

        public SetOperation(string dataPath, string systemCatalogPath)
        {
            DataPath = dataPath;
            SystemCatalogPath = systemCatalogPath;
        }

        public (OperationStatus Status, string? Message, string? NewPath, int? DatabaseId) Execute(string databaseToSet)
        {
            string databasePath = Path.Combine(DataPath, databaseToSet);

            // Verifica que la base de datos exista
            if (!Directory.Exists(databasePath))
            {
                return (OperationStatus.Error, $"Error: La base de datos '{databaseToSet}' no existe.", null, null);
            }

            // Obtiene el ID de la base de datos seleccionada
            int? databaseId = GetDatabaseId(databaseToSet);

            if (databaseId == null)
            {
                return (OperationStatus.Error, $"Error: No se pudo encontrar el ID de la base de datos '{databaseToSet}'.", null, null);
            }

            return (OperationStatus.Success, $"Base de datos seleccionada: {databaseToSet}", databasePath, databaseId);
        }

        private int? GetDatabaseId(string databaseName)
        {
            var databases = ReadFromSystemDatabases();
            var database = databases.FirstOrDefault(db => db.Name == databaseName);
            return database != default ? database.Id : (int?)null;
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
    }
}