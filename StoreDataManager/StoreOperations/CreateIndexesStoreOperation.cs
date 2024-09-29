using Entities;

namespace StoreDataManager.StoreOperations
{
    public class CreateIndexesStoreOperation
    {
        private readonly string dataPath;
        private readonly string systemCatalogPath;
        private readonly string currentDatabasePath;

        public CreateIndexesStoreOperation(string dataPath, string systemCatalogPath, string currentDatabasePath)
        {
            this.dataPath = dataPath;
            this.systemCatalogPath = systemCatalogPath;
            this.currentDatabasePath = currentDatabasePath;
        }

        public OperationStatus Execute(string indexName, string tableName, string columnName, string indexType)
        {
            try
            {
                // 1. Registrar el índice en SystemIndexes
                ActualizarIndexInSystemCatalog(indexName, tableName, columnName, indexType);

                // 2. Crear la estructura del índice en memoria

                // 3. Poblar el índice con los datos existentes


                Console.WriteLine($"Índice '{indexName}' creado exitosamente para la tabla '{tableName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void ActualizarIndexInSystemCatalog(string indexName, string tableName, string columnName, string indexType)
        {
            string SystemIndexesFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes");

            using (var writer = new BinaryWriter(File.Open(SystemIndexesFilePath, FileMode.Append)))
            {
                writer.Write(indexName);
                writer.Write(tableName);
                writer.Write(columnName);
                writer.Write(indexType);
            }
        }
    }
}