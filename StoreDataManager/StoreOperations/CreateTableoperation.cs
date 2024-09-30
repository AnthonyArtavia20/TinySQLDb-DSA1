using Entities;

namespace StoreDataManager.StoreOperations
{
    public class CreateTableOperation
    {
        private readonly string DataPath;
        private readonly string SystemCatalogPath;
        private readonly int? CurrentDatabaseId;

        public CreateTableOperation(string dataPath, string systemCatalogPath, int? currentDatabaseId)
        {
            DataPath = dataPath;
            SystemCatalogPath = systemCatalogPath;
            CurrentDatabaseId = currentDatabaseId;
        }

        public OperationStatus Execute(string tableName, List<ColumnDefinition> columns, string currentDatabasePath)
        {
            if (string.IsNullOrWhiteSpace(currentDatabasePath) || currentDatabasePath == DataPath + "\\" || CurrentDatabaseId == null)
            {
                Console.WriteLine("Error: No se ha seleccionado una base de datos. Use el comando SET primero.");
                return OperationStatus.Error;
            }

            if (TableExists(tableName))
            {
                Console.WriteLine($"Error: La tabla '{tableName}' ya existe.");
                return OperationStatus.Error;
            }

            int tableId = GetNextTableId();
            AddTableToCatalog(CurrentDatabaseId.Value, tableId, tableName);
            AddColumnsToCatalog(tableId, columns);

            string tablePath = Path.Combine(currentDatabasePath, tableName + ".Table");

            try
            {
                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write("TINYSQLSTART");
                    writer.Write(columns.Count);

                    foreach (var column in columns)
                    {
                        if (string.IsNullOrEmpty(column.Name) || string.IsNullOrEmpty(column.DataType))
                            throw new InvalidOperationException("El nombre de la columna o el tipo de dato no pueden ser nulos o vacíos.");

                        writer.Write(column.Name);
                        writer.Write(column.DataType);
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);
                        writer.Write(column.VarcharLength ?? 0);
                    }

                    writer.Write("ENDSTRUCTURE");
                    writer.Write("DATASTART");
                }

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en {currentDatabasePath}");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }
        private int GetNextTableId()
        {
            var tables = ReadFromSystemTables();
            if (tables.Count == 0)
            {
                return 1;
            }
            return tables.Max(t => t.TableId) + 1;
        }

        public static bool TableExists(string tableName) //Se volvieron staticas para que puedan ser usados en otras clases
        {
            var tables = ReadFromSystemTables();
            return tables.Any(tbl => tbl.TableName == tableName);
        }

        public static List<(int DbId, int TableId, string TableName)> ReadFromSystemTables()//Se volvieron staticas para que puedan ser usados en otras clases
        {
            string systemTablesFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemTables.tables");
            
            if (!File.Exists(systemTablesFilePath))
            {
                Console.WriteLine("Error: El archivo SystemTables.tables no existe.");
                return new List<(int, int, string)>();
            }
        
            var tableList = new List<(int, int, string)>();
            using (var reader = new BinaryReader(File.Open(systemTablesFilePath, FileMode.Open)))
            {
                while (reader.BaseStream.Position != reader.BaseStream.Length)
                {
                    int dbId = reader.ReadInt32();
                    int tableId = reader.ReadInt32();
                    string tableName = reader.ReadString();
                    tableList.Add((dbId, tableId, tableName));
                }
            }
            return tableList;
        }

        private void AddTableToCatalog(int dbId, int tableId, string tableName)
        {
            string systemTablesFilePath = Path.Combine(SystemCatalogPath, "SystemTables.tables");

            using (var writer = new BinaryWriter(File.Open(systemTablesFilePath, FileMode.Append)))
            {
                writer.Write(dbId);
                writer.Write(tableId);
                writer.Write(tableName);
            }
        }

        private void AddColumnsToCatalog(int tableId, List<ColumnDefinition> columns)
        {
            string systemColumnsFilePath = Path.Combine(SystemCatalogPath, "SystemColumns.columns");

            using (var writer = new BinaryWriter(File.Open(systemColumnsFilePath, FileMode.Append)))
            {
                foreach (var column in columns)
                {
                    if (string.IsNullOrWhiteSpace(column.Name) || string.IsNullOrWhiteSpace(column.DataType))
                    {
                        throw new InvalidOperationException("El nombre de la columna o el tipo de dato no pueden ser nulos o vacíos.");
                    }

                    Console.WriteLine($"Guardando columna: {column.Name} con tipo {column.DataType} en SystemColumns.columns para la tabla con ID {tableId}");
                    writer.Write(tableId);
                    writer.Write(column.Name);
                    writer.Write(column.DataType);
                    writer.Write(column.IsNullable);
                    writer.Write(column.IsPrimaryKey);
                    writer.Write(column.VarcharLength ?? 0);
                }
            }
        }
    }
}