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
                BinarySearchTree? indexStructure = null;

                if (indexType == "BST") 
                {
                    indexStructure = new BinarySearchTree(); // Instancia del BST
                }
                else if (indexType == "BTREE") 
                {
                    return OperationStatus.Error; // Falta implementar BTREE
                }
                else 
                {
                    throw new Exception("Tipo de índice no soportado.");
                }

                // 3. Poblar el índice con los datos de la columna especificada
                ActualizarIndice(indexStructure, tableName, columnName);

                Console.WriteLine($"Índice '{indexName}' creado exitosamente para la tabla '{tableName}' y columna '{columnName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al crear el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void ActualizarIndice(BinarySearchTree indexStructure, string tableName, string columnName)
        {
            // Ruta del archivo binario de la tabla
            string fullPath = Path.Combine(currentDatabasePath, $"{tableName}.Table");
        
            using (FileStream stream = File.Open(fullPath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                Console.WriteLine("Iniciando el proceso de actualización del índice.");
        
                // Leer la estructura de la tabla
                reader.ReadString(); // "TINYSQLSTART"
                int columnCount = reader.ReadInt32();
                List<ColumnDefinition> columns = new List<ColumnDefinition>();
        
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
                    columns.Add(column);
                }
        
                // Encontrar la columna solicitada
                var targetColumn = columns.FirstOrDefault(c => c.Name == columnName);
                if (targetColumn == null)
                {
                    throw new Exception($"Columna '{columnName}' no encontrada en la tabla '{tableName}'.");
                }
        
                // Mover el lector hasta la sección de datos
                while (reader.ReadString() != "DATASTART") { }
        
                // Calcular el tamaño fijo de cada registro
                int tamañoRegistro = CalcularTamañoRegistro(columns);
        
                // Leer los registros y poblar el índice
                while (reader.BaseStream.Position < reader.BaseStream.Length)
                {
                    long posicionRegistro = reader.BaseStream.Position;
        
                    // Leer el valor de la columna especificada
                    int valorColumna = LeerValorDeColumna(reader, targetColumn);
        
                    // Insertar en el índice el valor de la columna junto con la posición
                    indexStructure.Insert(valorColumna, posicionRegistro);
        
                    // Saltar al siguiente registro
                    reader.BaseStream.Seek(tamañoRegistro - CalcularTamañoColumna(targetColumn), SeekOrigin.Current);
                }
            }
        }
        
        private int CalcularTamañoRegistro(List<ColumnDefinition> columns)
        {
            int tamaño = 0;
            foreach (var column in columns)
            {
                tamaño += CalcularTamañoColumna(column);
            }
            return tamaño;
        }
        
        private int CalcularTamañoColumna(ColumnDefinition column)
        {
            if (column.DataType.StartsWith("VARCHAR"))
            {
                return column.VarcharLength ?? 0; // Usamos la longitud máxima declarada para VARCHAR
            }

            switch (column.DataType)
            {
                case "INTEGER":
                    return 4; // Tamaño fijo de 4 bytes
                case "DOUBLE":
                    return 8; // Tamaño fijo de 8 bytes
                case "DATETIME":
                    return 8; // Tamaño fijo de 8 bytes
                default:
                    throw new Exception($"Tipo de dato '{column.DataType}' no soportado.");
            }
        }
        
        private int LeerValorDeColumna(BinaryReader reader, ColumnDefinition column)
        {
            if (column.DataType.StartsWith("VARCHAR"))
            {
                int varcharLength = column.VarcharLength ?? 0; // Usamos la longitud máxima
                char[] chars = reader.ReadChars(varcharLength); // Leer la longitud máxima de VARCHAR
                string strValue = new string(chars).Trim(); // Eliminar los espacios en blanco extra
                return strValue.GetHashCode(); // Devolver un hash del valor string
            }

            switch (column.DataType)
            {
                case "INTEGER":
                    return reader.ReadInt32();
                case "DOUBLE":
                    return (int)reader.ReadDouble();
                case "DATETIME":
                    return (int)reader.ReadInt64();
                default:
                    throw new Exception($"Tipo de dato '{column.DataType}' no soportado.");
            }
        }

        private void ActualizarIndexInSystemCatalog(string indexName, string tableName, string columnName, string indexType)
        {
            string SystemIndexesFilePath = Path.Combine(systemCatalogPath, "SystemIndexes.Indexes");

            using (var writer = new BinaryWriter(File.Open(SystemIndexesFilePath, FileMode.Append)))
            {
                writer.Write(indexName);
                writer.Write(tableName);
                writer.Write(columnName);
                writer.Write(indexType);
            }
            Console.WriteLine("Se ingresó el índice a SystemIndexes");
        }
    }
}
