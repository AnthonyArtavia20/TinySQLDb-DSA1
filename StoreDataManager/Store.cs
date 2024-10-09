using Entities;
using System.Data;
using System.Text;
using StoreDataManager.StoreOperations;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();

        public static Store GetInstance()
        {
            lock(_lock)
            {
                if (instance == null) 
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        private const string DatabaseBasePath = @"C:\TinySql\"; //Se crea la carpeta que contendrá todo lo relacionado a la data del programa.
        private const string DataPath = $@"{DatabaseBasePath}\Data"; //Además se crea una carpeta que contendrá la data, es decir las bases de datos y demás.
        //Pasó a ser global en el archivo de configuración de rutas "ConfigPaths" -- > private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";//Contendrá archivos binarios con la información total de todas las bases de datos, con sus tablas y demás.
        private string RutaDeterminadaPorSet = $@"{DataPath}\"; //Ruta que cambiará constantemente ya que la determina la operación SET
        private int? currentDatabaseId; // Variable para almacenar el ID de la base de datos seleccionada

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog() {
            // Preparamos los archivos a crear dentro de SystemCatalog
            string[] catalogFiles = {
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemDatabases.databases"), //Almacena ID y nombre de DB
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemTables.tables"), //Almacena ID de Db, ID de tabla y nombre de Tabla
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemColumns.columns"), //Almacena ID Table, Nombre columna, Tipo de dato, Indicadores de si es nullable, primary key, y la longitud en caso de ser un VARCHAR
                Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes") // x
            };

            // Crear cada archivo si no existe
            foreach (string filePath in catalogFiles) {
                if (!File.Exists(filePath)) {
                    using (var stream = File.Create(filePath)) {
                        // Archivo creado vacío
                    }
                }
            }
        }

        //-----------------------------Apartir de aquí son las operaciones-------------------------------------------
        public OperationStatus CreateDataBase(string databaseName)
        {
            var createDatabaseOperation = new CreateDatabaseOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return createDatabaseOperation.Execute(databaseName);
        }

        public OperationStatus Set(string DataBaseToSet) 
        { //Cambia la ruta donde crear tablas , es decir, en que base de datos crear las tablas.
            var setOperation = new SetOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            var (status, message, newPath, newDatabaseId) = setOperation.Execute(DataBaseToSet);

            if (status == OperationStatus.Success)
            {
                RutaDeterminadaPorSet = newPath;
                currentDatabaseId = newDatabaseId;
                Console.WriteLine(message);
            }
            else
            {
                Console.WriteLine(message);
            }

            return status;
        }

        public OperationStatus CreateTable(string tableName, List<ColumnDefinition> columns) //Operación para poder crear tablas vacías pero con encabezados a los cuales agregarles datos.
        {
            var createTableOperation = new CreateTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath, currentDatabaseId);
            return createTableOperation.Execute(tableName, columns, RutaDeterminadaPorSet);
        }
        // Update implementacion 
        public OperationStatus Update(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue) //Operación para poder crear tablas vacías pero con encabezados a los cuales agregarles datos.
        {
            var updateOperation = new UpdateOperation(RutaDeterminadaPorSet);
            return updateOperation.Execute(tableName,columnToUpdate, newValue, whereColumn, whereValue);
        }
        // Delete implementacion 
        public OperationStatus Delete(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue)  
        {
            var DeleteOperation = new DeleteOperation();
            return DeleteOperation.Execute(tableName,columnToUpdate, newValue, whereColumn, whereValue);
        }
        public OperationStatus InsertIntoTable(string tableName, string[] columnas, string[] valores) //Permite insertar los datos en alguna tabla
        {//pero solo si se verificaron que dichos datos cumplen con la estructura esperada, esto se logra comparar en la clase dedicada para la operación
            // Insert.cs en Operations en QueryProcessor.

            var insertOperation = new InsertIntoTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return insertOperation.Execute(tableName, columnas, valores, RutaDeterminadaPorSet);
        }

        public OperationStatus DropTable(string tableName)
        {
            var dropTableOperation = new DropTableOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath);
            return dropTableOperation.Execute(tableName, RutaDeterminadaPorSet);
        }

                //!!!!!!!!!!!!!!!!!!!!Este método tiene que ser reestructurado según como se pide en el documento.!!!!!!!!!!!!!!!!!!!!!
        public (OperationStatus Status, string Data) Select(string NombreDeTableASeleccionar)
{
    string tableName = NombreDeTableASeleccionar + ".Table";
    string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName);

    Console.WriteLine($"Attempting to select from table: {tableName}");
    Console.WriteLine($"Full path: {fullPath}");

    if (!File.Exists(fullPath))
    {
        Console.WriteLine($"Error: The table file '{fullPath}' does not exist.");
        return (OperationStatus.Error, $"Error: La tabla '{NombreDeTableASeleccionar}' no existe.");
    }

    StringBuilder resultBuilder = new StringBuilder();
    try
    {
        using (FileStream stream = File.Open(fullPath, FileMode.Open))
        using (BinaryReader reader = new BinaryReader(stream))
        {
            // Verificar la marca de inicio
            string startMarker = reader.ReadString();
            Console.WriteLine($"Start marker: {startMarker}");
            if (startMarker != "TINYSQLSTART")
            {
                throw new InvalidDataException("Formato de archivo inválido.");
            }

            // Leer la estructura de la tabla
            int columnCount = reader.ReadInt32();
            Console.WriteLine($"Column count: {columnCount}");
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
                Console.WriteLine($"Columna agregada: Name={column.Name}, Type={column.DataType}, Nullable={column.IsNullable}, PrimaryKey={column.IsPrimaryKey}, VarcharLength={column.VarcharLength}");
                columns.Add(column);
            }
            // Verificar la marca de fin de estructura
            string endStructureMarker = reader.ReadString();
            Console.WriteLine($"End structure marker: {endStructureMarker}");
            if (endStructureMarker != "ENDSTRUCTURE")
            {
                throw new InvalidDataException("Invalid file structure");
            }
            // Construye el encabezado del resultado
            resultBuilder.AppendLine(string.Join(",", columns.Select(c => c.Name)));
            // Buscar el inicio de los datos
            string dataStartMarker = reader.ReadString();
            Console.WriteLine($"Data start marker: {dataStartMarker}");
            if (dataStartMarker != "DATASTART")
            {
                throw new InvalidDataException("Marca donde comienza la información no encontrada");
            }

            Console.WriteLine($"Longitud del archivo: {stream.Length}, Posición actual: {stream.Position}");

            int rowCount = 0;
            // Lee los datos de cada fila
            while (stream.Position < stream.Length)
            {
                StringBuilder rowBuilder = new StringBuilder();
                try
                {
                    foreach (var column in columns)
                    {
                        object value = null;
                        switch (column.DataType)
                        {
                            case "INTEGER":
                                value = reader.ReadInt32();
                                break;
                            case "DOUBLE":
                                value = reader.ReadDouble();
                                break;
                            case "DATETIME":
                                long ticks = reader.ReadInt64();
                                value = new DateTime(ticks).ToString("yyyy-MM-dd HH:mm:ss");
                                break;
                            default: // VARCHAR
                                int length = reader.ReadInt32();
                                if (length > 0)
                                {
                                    value = new string(reader.ReadChars(length)).Trim();
                                }
                                break;
                        }
                        Console.WriteLine($"Read value for {column.Name}: {value}");
                        rowBuilder.Append(value).Append(",");
                    }
                    string row = rowBuilder.ToString().TrimEnd(',');
                    Console.WriteLine($"Row data: {row}");
                    resultBuilder.AppendLine(row);
                    rowCount++;
                }
                catch (EndOfStreamException)
                {
                    Console.WriteLine("Reached end of stream.");
                    break;
                }
            }

            Console.WriteLine($"Total rows read: {rowCount}");
            Console.WriteLine("¡Operación SELECT ejecutada correctamente!");
            return (OperationStatus.Success, resultBuilder.ToString());
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error reading file: {ex.Message}");
        Console.WriteLine($"Stack trace: {ex.StackTrace}");
        return (OperationStatus.Error, $"Error: {ex.Message}");
    }
}
        public OperationStatus CreateIndexes(string indexName, string tableName, string columnName, string indexType)
        {
            var CreateIndexesStoreOperation = new CreateIndexesStoreOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath, RutaDeterminadaPorSet);
            return CreateIndexesStoreOperation.Execute(indexName, tableName, columnName, indexType);
        }
    }
}