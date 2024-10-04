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
        
                    int idColumnIndex = -1; //// Para almacenar el índice de la columna ID
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
        
                        // Detectar la columna que es PrimaryKey (ID)
                        if (column.IsPrimaryKey && column.DataType == "INTEGER")
                        {
                            idColumnIndex = i; //Si se detecta, le damos el valor del contador actual, es decir almacenamos su valor.
                            //variable que contiene el índice de la columna que es la Primary Key (ID),ndica la posición de esta columna dentro de la lista tableColumns.
                        }
                        tableColumns.Add(column);
                    }
        
                    // Si no hay columna de ID, error, igual esto nunca va pasar debido al esquema de la tabla 
                    //dado desde tableSchema.
                    if (idColumnIndex == -1)
                    {
                        Console.WriteLine("Error: No se encontró una columna de ID en la tabla.");
                        return OperationStatus.Error;
                    }
        
                    //Cuando encontremos la marca de "apartir de aquí comeinza la info", básicamente los inserts.
                    while (reader.ReadString() != "DATASTART") { }
                    
                    // Verificar duplicados en la columna ID
                    //Nota: tableColumns[idColumnIndex] accede a la columna que es la Primary Key (la columna de ID)

                    //Además: tableColumns[idColumnIndex].Name obtiene el nombre de esa columna de ID. Por ejemplo, si la 
                    //columna de ID se llama "ID", entonces el valor de tableColumns[idColumnIndex].Name sería "ID".
                    int newIdValue = int.Parse(valores[Array.IndexOf(columnas, tableColumns[idColumnIndex].Name)]);
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        int currentId = reader.ReadInt32();
                        if (currentId == newIdValue)
                        {
                            Console.WriteLine("Error: El valor del ID ya existe en la tabla.");
                            return OperationStatus.Warning;
                        }
                        // Omitir los valores de las otras columnas
                        for (int i = 0; i < columnCount; i++)
                        {
                            if (i != idColumnIndex)
                            {
                                SkipColumn(reader, tableColumns[i].DataType);
                            }
                        }
                    }
        
                    // Posicionar al final del archivo para agregar los nuevos valores
                    stream.Seek(0, SeekOrigin.End);
        
                    // Insertar los valores
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
                                if (value.Length > column.VarcharLength)
                                {
                                    throw new Exception($"El valor para la columna {column.Name} excede la longitud permitida.");
                                }
                                writer.Write(value.Length); // Escribir longitud
                                writer.Write(value.ToCharArray()); // Escribir valor
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
        
        void SkipColumn(BinaryReader reader, string? dataType)
        {
            switch (dataType)
            {
                case "INTEGER":
                    reader.ReadInt32();
                    break;
                case "DOUBLE":
                    reader.ReadDouble();
                    break;
                case "DATETIME":
                    reader.ReadInt64();
                    break;
                default: // VARCHAR
                    int strLength = reader.ReadInt32();
                    reader.ReadChars(strLength);
                    break;
            }
        }
    }
}