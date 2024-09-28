using Entities;

namespace StoreDataManager.StoreOperations
{
    public class InsertIntoTableOperation
    {
        private readonly string dataPath;
        private readonly string systemCatalogPath;

        public InsertIntoTableOperation(string dataPath, string systemCatalogPath)   
        {
            this.dataPath = dataPath;
            this.systemCatalogPath = systemCatalogPath;
        }

        public OperationStatus Execute(string tableName, string[] columnas, string[] valores, string currentDatabasePath)
        {
            string fullPath = Path.Combine(currentDatabasePath, tableName + ".Table");

            if (!File.Exists(fullPath))
            {
                Console.WriteLine($"Error: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Lee la estructura de la tabla
                    reader.ReadString(); // TINYSQLSTART
                    int columnCount = reader.ReadInt32();
                    List<ColumnDefinition> tableColumns = new List<ColumnDefinition>();

                    for (int i = 0; i < columnCount; i++)
                    {
                        var column = new ColumnDefinition
                        {
                            Name = reader.ReadString(),
                            DataType = reader.ReadString(),
                            IsNullable = reader.ReadBoolean(),
                            IsPrimaryKey = reader.ReadBoolean(),
                            VarcharLength = reader.ReadInt32()
                        };
                        tableColumns.Add(column);
                    }

                    // Busca DATASTART
                    while (reader.ReadString() != "DATASTART") { }

                    // Posicionar al final del archivo para agregar los nuevos valores
                    stream.Seek(0, SeekOrigin.End);

                    // Inserta los valores
                    for (int i = 0; i < tableColumns.Count; i++)
                    {
                        var column = tableColumns[i];
                        var value = valores[Array.IndexOf(columnas, column.Name)];

                        switch (column.DataType)
                        {
                            case "INTEGER":
                                writer.Write(int.Parse(value));
                                break;
                            case "DOUBLE":
                                writer.Write(double.Parse(value));
                                break;
                            case "DATETIME":
                                writer.Write(long.Parse(value));
                                break;
                            default: // VARCHAR
                                writer.Write(value.Length);
                                writer.Write(value.ToCharArray());
                                break;
                        }
                    }
                }

                Console.WriteLine("¡Inserción completada correctamente!");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al insertar en la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }
    }
}