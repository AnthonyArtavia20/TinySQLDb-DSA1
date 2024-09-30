using Entities;

namespace StoreDataManager.StoreOperations
{
    public class DropTableOperation
    {
        private readonly string dataPath;
        private readonly string systemCatalogPath;

        public DropTableOperation(string dataPath, string systemCatalogPath)
        {
            this.dataPath = dataPath;
            this.systemCatalogPath = systemCatalogPath;
        }

        public OperationStatus Execute(string tableName, string currentDatabasePath)
        {
            string tablePath = Path.Combine(currentDatabasePath, tableName + ".Table");
            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"Tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            try
            {
                bool tableHasData = TableHasInfo(tableName, currentDatabasePath);

                if (tableHasData)
                {
                    Console.WriteLine($"La tabla '{tableName}' no puede ser eliminada porque contiene datos.");
                    return OperationStatus.Error;
                }

                File.Delete(tablePath);
                RemoveTableFromSystemCatalog(tableName);
                Console.WriteLine($"La tabla '{tableName}' se ha eliminado correctamente.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al eliminar la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private bool TableHasInfo(string tableName, string currentDatabasePath)
        {
            string tablePath = Path.Combine(currentDatabasePath, tableName + ".Table");
            bool dataStarted = false;

            try
            {
                using (var reader = new StreamReader(tablePath))
                {
                    string? line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        if (line.Contains("DATASTART"))
                        {
                            dataStarted = true;
                            continue;
                        }

                        if (dataStarted && !string.IsNullOrWhiteSpace(line))
                        {
                            Console.WriteLine($"La tabla '{tableName}' tiene datos y no puede ser eliminada.");
                            return true;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la tabla {tableName}: {ex.Message}");
            }

            return false;
        }

        private bool RemoveTableFromSystemCatalog(string tableName)
        {
            string systemTablesFilePath = Path.Combine(systemCatalogPath, "SystemTables.tables");
            var tableList = new List<(int DbId, int TableId, string TableName)>();

            try
            {
                using (var reader = new BinaryReader(File.Open(systemTablesFilePath, FileMode.Open)))
                {
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        int dbId = reader.ReadInt32();
                        int tableId = reader.ReadInt32();
                        string currentTableName = reader.ReadString();

                        if (currentTableName != tableName)
                        {
                            tableList.Add((dbId, tableId, currentTableName));
                        }
                        else 
                        {
                            RemoveColumsFromSystemCatalog(tableId);
                        }
                    }
                }

                using (var writer = new BinaryWriter(File.Open(systemTablesFilePath, FileMode.Create)))
                {
                    foreach (var table in tableList)
                    {
                        writer.Write(table.DbId);
                        writer.Write(table.TableId);
                        writer.Write(table.TableName);
                    }
                }

                Console.WriteLine($"La tabla '{tableName}' ha sido eliminada del SystemCatalog.");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al eliminar la tabla del catálogo: {ex.Message}");
                return false;
            }
        }

        private bool RemoveColumsFromSystemCatalog(int tableId)
        {
            string systemColumnsFilePath = Path.Combine(systemCatalogPath, "SystemColumns.columns");
            
            var columnsList = new List<(int TableId, string Name, string DataType, bool IsNullable, bool IsPrimaryKey, int VarcharLength)>();

            try
            {
                using (var reader = new BinaryReader(File.Open(systemColumnsFilePath, FileMode.Open)))
                {
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        int currentTableId = reader.ReadInt32();
                        string Name = reader.ReadString();
                        string DataType = reader.ReadString();
                        bool IsNullable = reader.ReadBoolean();
                        bool IsPrimaryKey = reader.ReadBoolean();
                        int VarcharLength = reader.ReadInt32();

                        if (currentTableId != tableId)
                        {
                            columnsList.Add((currentTableId, Name, DataType, IsNullable, IsPrimaryKey, VarcharLength));
                        }
                    }
                }

                using (var writer = new BinaryWriter(File.Open(systemColumnsFilePath, FileMode.Create)))
                {
                    foreach (var column in columnsList)
                    {
                        
                        writer.Write(column.TableId);
                        writer.Write(column.Name);
                        writer.Write(column.DataType);
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);
                        if (column.VarcharLength != null) {
                            writer.Write(column.VarcharLength);
                        }
                        
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al eliminar la columna del catálogo: {ex.Message}");
                return false;
            }
        }
    }
}