using Entities;

namespace StoreDataManager.StoreOperations
{
    public class CreateIndexesStoreOperation //Clase principal para poder llevar a cabo la operación de creación de Índices
    {
        private readonly string dataPath; //Ruta hacía la carpeta "Data"
        private readonly string systemCatalogPath; //Ruta hacía el SystemCatalog
        private readonly string currentDatabasePath; // Para posteriormente acceder a las tablas desde la base de datos actual.

        public CreateIndexesStoreOperation(string dataPath, string systemCatalogPath, string currentDatabasePath) //Constructor de la clase
        {
            this.dataPath = dataPath;
            this.systemCatalogPath = systemCatalogPath;
            this.currentDatabasePath = currentDatabasePath;
        }

        public OperationStatus Execute(string indexName, string tableName, string columnName, string indexType)
        {
            try
            {
                InterfaceIndexStructure indexStructure; //Se crea una variable del tipo interaz con valor nulo inicialmente.

                if (indexType == "BST") 
                {
                    indexStructure = new BinarySearchTree(); //Se le asigna el nuevo objeto de BST
                }
                else if (indexType == "BTREE") 
                {
                    indexStructure = new BTree(3); // Asumiendo un grado mínimo de 3 para el BTree REVISAR ESTO!!!!!!!!!!!!!!!!!!!!!!!
                }
                else 
                {
                    throw new Exception("Tipo de índice no soportado."); //En caso de que la estrucutra que se ingrese no existe, aunque esta
                    //validación ya está comprobada desde QueryProccesor.
                }

                // 2. Poblar el índice con los datos de la columna especificada
                ActualizarIndice(indexStructure, tableName, columnName);

                // 3. Registrar el índice en SystemIndexes
                ActualizarIndexInSystemCatalog(indexName, tableName, columnName, indexType);

                Console.WriteLine($"Índice '{indexName}' creado exitosamente para la tabla '{tableName}' y columna '{columnName}'.");
                return OperationStatus.Success;
            }
            catch (Exception ex) //Para los errores...
            {
                Console.WriteLine($"Error al crear el índice: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        /*Método especializado para poder leer los valores de las tablas e insertarlos en la estructura determianda anteriormente, pasandolos 
        por la interfaz, Objetivo: Leer las columnas y sus datos, identificar la posición de dichos datos e insertarlos en las estructuras.*/
        private void ActualizarIndice(InterfaceIndexStructure indexStructure, string tableName, string columnName)
        {
            string fullPath = Path.Combine(currentDatabasePath, $"{tableName}.Table"); //Ruta a la tabla
        
            using (FileStream stream = File.Open(fullPath, FileMode.Open)) //Se abre la tabla para poder leer su estructura y datos.
            using (BinaryReader reader = new BinaryReader(stream))
            {
                Console.WriteLine("Iniciando el proceso de actualización del índice.");
        
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
                    columns.Add(column); //Estructura de las columnas
                }
        
                var targetColumn = columns.FirstOrDefault(c => c.Name == columnName); //Se compara si la llave a buscar existe como columna
                if (targetColumn == null)
                {
                    throw new Exception($"Columna '{columnName}' no encontrada en la tabla '{tableName}'.");
                }
        
                // Mover el lector hasta la sección de datos
                while (reader.ReadString() != "DATASTART") { }

                while (reader.BaseStream.Position < reader.BaseStream.Length) //Leemos desde la la marca de "DATASTART" hasta que se acabe el archivo.
                {
                    long posicionRegistro = reader.BaseStream.Position;

                    // Leer todos los valores del registro
                    var valores = new object[columnCount];
                    for (int i = 0; i < columnCount; i++)
                    {
                        valores[i] = LeerValorDeColumna(reader, columns[i]); //Se van leyendo los datos y obteniendo los valores de los mismos
                    }

                    // Obtener el valor de la columna indexada
                    int columnIndex = columns.IndexOf(targetColumn);
                    var valorColumna = valores[columnIndex];

                    Console.WriteLine($"Valor leído de la columna '{columnName}' en la posición {posicionRegistro}: {valorColumna}");

                    // Insertar en el índice el valor de la columna junto con la posición
                    if (valorColumna is int intValue)
                    {
                        indexStructure.Insert(intValue, posicionRegistro); //Se utiliza la interfaz para reutilizar código.
                    }
                    else
                    {
                        throw new Exception($"Tipo de dato no soportado para indexación: {valorColumna.GetType()}");
                        //En caso de que en el futuro se quieran impplementar más tipos de datos.
                    }
                }
            }

            //Para poder comprobar que se insertó bien se realiza una lectura de la estructura de datos.
            Console.WriteLine($"Recorrido en orden del índice ({indexStructure.GetType().Name}):");
            indexStructure.InOrderTraversal();
        }

        private object LeerValorDeColumna(BinaryReader reader, ColumnDefinition column)
        {
            switch (column.DataType)
            {
                case "INTEGER":
                    return reader.ReadInt32();
                case "DOUBLE":
                    return reader.ReadDouble();
                case "DATETIME":
                    return new DateTime(reader.ReadInt64());
                default:
                    if (column.DataType.StartsWith("VARCHAR"))
                    {
                        int length = reader.ReadInt32();
                        return new string(reader.ReadChars(length)).TrimEnd('\0');
                    }
                    throw new Exception($"Tipo de dato '{column.DataType}' no soportado.");
            }
        }

        //Este método abre el archivo SystemIndexes para poder guardar el registro de que se creó un Indice, con el objetivo de que cuando se
        //inicie el programa, el índice pueda ser creado nuevamente.
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