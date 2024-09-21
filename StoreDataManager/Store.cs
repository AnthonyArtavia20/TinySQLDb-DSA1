using Entities;
using System.Data;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

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
        private string RutaDeterminadaPorSet = $@"{DataPath}\"; //Ruta que cambiará constantemente ya que la determina la operación SET
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";//Contendrá archivos binarios con la información total de todas las bases de datos, con sus tablas y demás.
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";//dirección para la información dentro de las tablas en SystemaCatalog.

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog() //FALTA HACER COSAS AQUÍ
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }

        //-----------------------------Apartir de aquí son las operaciones-------------------------------------------
        public OperationStatus CreateDataBase(string databaseName)
        {
            if (string.IsNullOrWhiteSpace(databaseName)) //Verificación de errores(Que no sea una oración vacía)
            {
                return OperationStatus.Error;
            }
        
            string databasePath = Path.Combine(DataPath, databaseName); //Sirve para poder combinar toda la ruta
            //y no tener que escribirla por defecto.
            Directory.CreateDirectory(databasePath); //Creamos un directorio con el nombre que se pasó como parámetro.
            return OperationStatus.Success;
        }
        
        public OperationStatus Set(string DataBaseToSet) //Cambia la ruta donde crear tablas , es decir, en que base de datos crear las tablas.
        {
            string databasePath = Path.Combine(DataPath, DataBaseToSet);
            if (!Directory.Exists(databasePath))//Manejo de errores.
            {
                Console.WriteLine($"Error: La base de datos '{DataBaseToSet}' no existe.");
                return OperationStatus.Error;
            }
        
            RutaDeterminadaPorSet = databasePath;
            Console.WriteLine($"Base de datos seleccionada: {DataBaseToSet}");
            return OperationStatus.Success;
        }

        public OperationStatus InsertIntoTable(string tableName, string[] columnas, string[] valores) //Incertamos valores en una tabla dada.
        {
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table");//Para poder encontrar el  archivo binario de la tabla para abrirlo y escribir sobre él.

            Console.WriteLine($"Attempting to insert into table: {tableName}");//Debug porque daba errores:/
            Console.WriteLine($"Full path: {fullPath}");//Debug porque daba errores:/

            if (!File.Exists(fullPath)) //Comprobamos que el archivo exista.
            {
                Console.WriteLine($"Error: The table file '{fullPath}' does not exist.");
                return OperationStatus.Error;
            }

            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                using (BinaryWriter writer = new BinaryWriter(stream))//Accedemos al archivo, abriendolo y modificándolo.
                {
                    reader.ReadString(); // TINYSQLSTART
                    int columnCount = reader.ReadInt32();
                    Console.WriteLine($"Número de columna: {columnCount}");

                    List<ColumnDefinition> tableColumns = new List<ColumnDefinition>(); //Creamos una list para almacenar las columnas y luego poner los datos.
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
                        tableColumns.Add(column); //Añadimos el encabezado que tenga con su tipo.
                        Console.WriteLine($"Column {i}: Name={column.Name}, Type={column.DataType}, Nullable={column.IsNullable}, PrimaryKey={column.IsPrimaryKey}, VarcharLength={column.VarcharLength}"); //Para debug
                    }

                    // Buscar DATASTART, esto muerstra donde emepzar a insertar valores.
                    while (reader.ReadString() != "DATASTART") { }

                    // Mover al final del archivo para insertar
                    stream.Seek(0, SeekOrigin.End);

                    // Se comienzan a escribir todos los valores que tiene la lista temporal de columnas
                    for (int i = 0; i < tableColumns.Count; i++)
                    {
                        var column = tableColumns[i];
                        var value = valores[Array.IndexOf(columnas, column.Name)];

                        Console.WriteLine($"Insertando valor para la columna {column.Name}: {value}");

                        switch (column.DataType) //Y comenzamos a dividir los casos  que lee.
                        {//Los vamos parseando.
                            case "INTEGER":
                                writer.Write(int.Parse(value));
                                break;
                            case "DOUBLE":
                                writer.Write(double.Parse(value));
                                break;
                            case "DATETIME":
                                writer.Write(long.Parse(value));
                                break;
                            default: // VARCHAR para nombre y apellido.
                                writer.Write(value.Length);
                                writer.Write(value.ToCharArray());
                                break;
                        }
                    }
                }

                Console.WriteLine("¡Operación Insert aplicada correctamente!");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Se produjo este error mientras se hacía la operación Insert: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                return OperationStatus.Error;
            }
        }

        public OperationStatus CreateTable(string tableName, List<ColumnDefinition> columns) //Operación para poder crear tablas vacías pero con encabezados a los cuales agregarles datos.
        {
            if (string.IsNullOrWhiteSpace(RutaDeterminadaPorSet) || RutaDeterminadaPorSet == DataPath + "\\")
            {
                Console.WriteLine("Error: No se ha seleccionado una base de datos. Use el comando SET primero.");
                return OperationStatus.Error;
            }

            string tablePath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table"); //Ruta completa hasta la tabla.

            try
            {
                //Abrimos el archivo con el nombre ingresado.
                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream) )
                {
                    // Escribir una marca de inicio de archivo, esto se usa para que Insert más adeltante sepa donde empezar y terminar de agregar valores.
                    writer.Write("TINYSQLSTART");

                    // Escribir el número de columnas
                    writer.Write(columns.Count);

                    // Escribir las definiciones de columnas
                    foreach (var column in columns)
                    {
                        writer.Write(column.Name);
                        writer.Write(column.DataType);
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);
                        writer.Write(column.VarcharLength ?? 0);
                    }

                    // Escribir una marca de fin de estructura
                    writer.Write("ENDSTRUCTURE");

                    // Espacio para datos futuros
                    writer.Write("DATASTART");
                }

                //Esta lógica tiene que mejorarse pero realmente repite lo de este método.
                UpdateSystemCatalog(tableName, columns);

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en {RutaDeterminadaPorSet}");
                return OperationStatus.Success;
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al crear la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void UpdateSystemCatalog(string tableName, List<ColumnDefinition> columns) //Encargado de actualizar la carpeta que contiene toda la información de las base de datos.
        {
            // Actualizar SystemTables
            using (FileStream stream = File.Open(SystemTablesFile, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                writer.Write(RutaDeterminadaPorSet.PadRight(100)); // Ruta de la base de datos (fijo 100 caracteres)
                writer.Write(tableName.PadRight(30)); // Nombre de la tabla (fijo 30 caracteres)
            }

            // Actualizar SystemColumns
            string systemColumnsFile = Path.Combine(SystemCatalogPath, "SystemColumns.table");
            using (FileStream stream = File.Open(systemColumnsFile, FileMode.Append))
            using (BinaryWriter writer = new BinaryWriter(stream))
            {
                foreach (var column in columns)
                {
                    writer.Write(tableName.PadRight(30)); // Nombre de la tabla (fijo 30 caracteres)
                    writer.Write(column.Name.PadRight(30)); // Nombre de la columna (fijo 30 caracteres)
                    writer.Write(column.DataType.PadRight(20)); // Tipo de dato (fijo 20 caracteres)
                    writer.Write(column.IsNullable);
                    writer.Write(column.IsPrimaryKey);
                    writer.Write(column.VarcharLength ?? 0);
                }
            }
        }

        public (OperationStatus Status, string Data) Select(string NombreDeTableASeleccionar) //Permite leer todo el contenido de un archivo binario(Tablas)
        {
            // Prepara el nombre completo del archivo de la tabla
            string tableName = NombreDeTableASeleccionar + ".Table"; //Se preapara la tabla a leer.
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName);//Se combina toda la ruta

            // Log para depuración
            Console.WriteLine($"Attempting to select from table: {tableName}");
            Console.WriteLine($"Full path: {fullPath}");

            // Verifica si el archivo de la tabla existe.
            if (!File.Exists(fullPath))//Prevención de errores, la tabla no existe.
            {
                Console.WriteLine($"Error: The table file '{fullPath}' does not exist.");
                return (OperationStatus.Error, $"Error: La tabla '{NombreDeTableASeleccionar}' no existe.");
            }
        
            StringBuilder resultBuilder = new StringBuilder(); //Creamos una string mutable que pueda albergar toda la estructura que contiene el archivo binario.
            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Verificar la marca de inicio, por eso era tan importante añadirlo en la InsertOperation.
                    string startMarker = reader.ReadString();
                    if (startMarker != "TINYSQLSTART")
                    {
                        throw new InvalidDataException("Formato de archivo inválido.");
                    }
        
                    // Leer la estructura de la tabla
                    int columnCount = reader.ReadInt32();
                    List<ColumnDefinition> columns = new List<ColumnDefinition>();
        
                    for (int i = 0; i < columnCount; i++) //Se comienza a añadir como columnas los encabezados
                    {
                        var column = new ColumnDefinition//<-- Este archivo se encuentra en Entities, permite definir que tipos de datos se esperan y cuales son.
                        {
                            Name = reader.ReadString(),
                            DataType = reader.ReadString(),
                            IsNullable = reader.ReadBoolean(),
                            IsPrimaryKey = reader.ReadBoolean(),
                            VarcharLength = reader.ReadInt32()
                        };
                        columns.Add(column);
                        Console.WriteLine($"Column {i + 1}: Name={column.Name}, Type={column.DataType}, Nullable={column.IsNullable}, PrimaryKey={column.IsPrimaryKey}, VarcharLength={column.VarcharLength}");
                    }
        
                    // Verificar la marca de fin de estructura
                    string endStructureMarker = reader.ReadString();
                    if (endStructureMarker != "ENDSTRUCTURE")
                    {
                        throw new InvalidDataException("Invalid file structure");
                    }
        
                    // Construye el encabezado del resultado
                    resultBuilder.AppendLine(string.Join(",", columns.Select(c => c.Name)));

                    //-----------------------Apartir de aquí comienza a añadir los datos almacenados------------------------------------------------------
                    // Buscar el inicio de los datos
                    string dataStartMarker = reader.ReadString();
                    if (dataStartMarker != "DATASTART")
                    {
                        throw new InvalidDataException("Marca donde comienza la información no encontrada");
                    }

                    //Depuración
                    Console.WriteLine($"Longitud del archivo: {stream.Length}, Posición actual: {stream.Position}");
        
                    bool hasData = false; //<--- Se usa para poder devolver un mensaje indicando si se encontró o no información en la tabla actual.
                    // Lee los datos de cada fila
                    while (stream.Position < stream.Length)
                    {
                        hasData = true;
                        StringBuilder ConstructorFila = new StringBuilder(); //<--- Encargado de guardar todos los datos de las filas en las tablas.
                        foreach (var column in columns)
                        {
                            // Leer el valor según el tipo de dato, lo vamos agregando 
                            switch (column.DataType)
                            {
                                case "INTEGER":
                                    ConstructorFila.Append(reader.ReadInt32());
                                    break;
                                case "DOUBLE":
                                    ConstructorFila.Append(reader.ReadDouble());
                                    break;
                                case "DATETIME":
                                    long ticks = reader.ReadInt64();
                                    DateTime dateTime = new DateTime(ticks);
                                    ConstructorFila.Append(dateTime.ToString("yyyy-MM-dd HH:mm:ss"));
                                    break;
                                default: // VARCHAR para los Nombres y Apellidos en caso de...
                                    int length = reader.ReadInt32();
                                    ConstructorFila.Append(new string(reader.ReadChars(length)));
                                    break;
                            }
                            ConstructorFila.Append(",");
                        }
                        resultBuilder.AppendLine(ConstructorFila.ToString().TrimEnd(','));
                    }

                    // Verifica si se encontraron datos
                    if (!hasData)
                    {
                        Console.WriteLine("La tabla está vacía.");
                        return (OperationStatus.Success, "La tabla está vacía.");
                    }
        
                    Console.WriteLine("¡Operación SELECT ejecutada correctamente!");
                    return (OperationStatus.Success, resultBuilder.ToString());
                }
            }
            catch (Exception ex)
            {
                // Log de errores
                Console.WriteLine($"Error reading file: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
                return (OperationStatus.Error, $"Error: {ex.Message}");
            }
        }
    }
}