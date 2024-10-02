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
                BinarySearchTree? indexStructure = null; // Se inicializa en null porque nada error al no aignarle ningun valor inicial.

                if (indexType == "BST") //BinarySearchTree
                {
                    indexStructure = new BinarySearchTree(); // Se hace una instancia de la estructura de BST
                }
                else if (indexType == "BTREE") //Para los Balanced Trees
                {
                    return OperationStatus.Error;//Falta implementar este árbol
                }
                else //Desde "CreateIndexes" ya se verifica esto pero es para evitar NullReferences.
                {
                    throw new Exception("Tipo de índice no soportado.");
                }

                // 3. Poblar el índice/Árbol con la key y posición en el archivo de la tabla actual.
                ActualizarIndice(indexStructure, tableName, columnName);

                Console.WriteLine($"Índice '{indexName}' creado exitosamente para la tabla '{tableName}' y ColumnaKeyValue: {columnName}.");
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
            string SystemIndexesFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes"); //Buscamos el archivo para almacenar el nuevo indice.

            using (var writer = new BinaryWriter(File.Open(SystemIndexesFilePath, FileMode.Append)))
            {
                //Se escribe el formato esperado para ser leido más tarde cuando el server se reinicie.
                writer.Write(indexName); 
                writer.Write(tableName);
                writer.Write(columnName);
                writer.Write(indexType);
            }
        }

        private void ActualizarIndice(BinarySearchTree indexStructure, string tableName, string columnName)
        {
            // Ruta del archivo binario de la tabla
            string path = Path.Combine(currentDatabasePath, $"{tableName}.Table");

            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                // Leer la estructura de la tabla (adaptar según cómo esté almacenada)
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

                // Buscar la columna solicitada
                var targetColumn = columns.FirstOrDefault(c => c.Name == columnName);
                if (targetColumn == null)
                {
                    throw new Exception($"Columna '{columnName}' no encontrada en la tabla '{tableName}'.");
                }

                // Leer registros y poblar el índice
                while (reader.BaseStream.Position < reader.BaseStream.Length)
                {
                    long posicionRegistro = reader.BaseStream.Position; // Guardar la posición del registro
                    int valorColumna = LeerValorDeColumna(reader, targetColumn); // Leer el valor de la columna

                    // Insertar en el índice
                    indexStructure.Insert(valorColumna, posicionRegistro);
                }
            }
        }

        private int LeerValorDeColumna(BinaryReader reader, ColumnDefinition column)
        {
            switch (column.DataType)
            {
                case "INTEGER":
                    return reader.ReadInt32();
                case "DOUBLE":
                    return (int)reader.ReadDouble(); // Convertir a int para simplificar
                case "VARCHAR":
                    int length = reader.ReadInt32();
                    string strValue = new string(reader.ReadChars(length));
                    return strValue.GetHashCode(); // O usar otro método para manejar strings
                default:
                    throw new Exception($"Tipo de dato '{column.DataType}' no soportado.");
            }
        }
    }
}