using Entities;
using System.Data;
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
        private int? currentDatabaseId; // Variable para almacenar el ID de la base de datos seleccionada

        public Store()
        {
            this.InitializeSystemCatalog();
        }

        private void InitializeSystemCatalog() {
            // Preparamos los archivos a crear dentro de SystemCatalog
            string[] catalogFiles = {
                Path.Combine(SystemCatalogPath, "SystemDatabases.databases"),
                Path.Combine(SystemCatalogPath, "SystemTables.tables"),
                Path.Combine(SystemCatalogPath, "SystemColumns.columns"),
                Path.Combine(SystemCatalogPath, "SystemIndexes.Indexes")
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
            if (string.IsNullOrWhiteSpace(databaseName)) //Verificación de errores(Que no sea una oración vacía)
            {
                return OperationStatus.Error;
            }

            if (DatabaseExists(databaseName)) {
                // Verificar si la base de datos ya existe por medio de recorrer todo el archivo binario para al final comparar por el nombre, si el nombre es igual, entonces existe, por lo tanto
                //reportará un Status de error.
                return OperationStatus.Error;
            }

            int newDatabaseId = GetNextDatabaseId(); /*Asignar un nuevo ID para la base de datos, es se logra 
            recorriendo todo el archivo binario que contiene todas las BDs y al llegar al final guarda el 
            último ID(también el nombre pero nos interesa el ID) y al retornar ese ID decimos que el ID a otorgar 
            a la nueva base de datos es el ID actual +1 y listo.*/

            // Guardar la base de datos en SystemDatabases.databases
            AddDatabaseToCatalog(newDatabaseId, databaseName); //Pasamos el ID generado y el nombre de la base de datos a crear(se extrajo el nombre desde SQLQueryProccesor.cs)
        
            string databasePath = Path.Combine(DataPath, databaseName); //Sirve para poder combinar toda la ruta para posterior crear el directorio.
            Directory.CreateDirectory(databasePath); //Creamos un directorio con el nombre que se pasó como parámetro.
            return OperationStatus.Success;
        }

        private bool DatabaseExists(string databaseName) 
        { /*Este metodo compara el nombre de la base de datos a crear con alguno de los que están en la lista
            creada por ReadFromSystemDatabases, si encuentra que es igual, manda un True, si no un False.
            Permitiendo así crear la BD.*/
            var databases = ReadFromSystemDatabases();
            return databases.Any(db => db.Name == databaseName);
        }

        private List<(int Id, string Name)> ReadFromSystemDatabases() 
        { //Se encarga de crear una lista con todas las bases de datos.
            var databaseList = new List<(int, string)>(); //Creamos una lista que contenga el ID y el Nombre de cada BD.
            string systemDatabasesFilePath = Path.Combine(SystemCatalogPath, "SystemDatabases.databases"); //Con esto llegamos al archivo dentro del SystemCatalog.

            using (var reader = new BinaryReader(File.Open(systemDatabasesFilePath, FileMode.Open))) { //Abrimos el archivo binario y comenzamos a leerlo.
                while (reader.BaseStream.Position != reader.BaseStream.Length) { //Vamos almacenando los datos de cada BD.
                    int id = reader.ReadInt32(); //Su ID
                    string name = reader.ReadString(); // Su nombre
                    databaseList.Add((id, name)); //Y los agreamos a la lista.
                }
            }
            return databaseList; //Cuando llegamos al final, devolvemos la lista para poder compararla.
        }

        private int GetNextDatabaseId() { //Permite crear IDs para las bases de datos nuevas.
            var databases = ReadFromSystemDatabases(); //Consultamos el último ID que tiene la última base de datos.
            if (databases.Count == 0) {//Si resulta que no hay bases de datos, entonces la primera tendrá un "1" como ID.
                return 1; // Primer ID
            }
            return databases.Max(db => db.Id) + 1; //Si existen bases de datos nos vamos a la última, obtenemos su ID y luego se le suma +1, y ese será el nuevo ID.
        }

        private int? GetDatabaseId(string databaseName) { //Igual que las tablas lo que hace es recorrer todo el archivo binario para luego compararlo.
            var databases = ReadFromSystemDatabases();
            var database = databases.FirstOrDefault(db => db.Name == databaseName); //Nos devuelve el primer elemento en esa lista, es decir el ID donde el nombre sea igual al nombre de la BD que se le pasó como parámetro.
            return database != default ? database.Id : (int?)null;
        }
        private void AddDatabaseToCatalog(int id, string databaseName) { //Este métdo se encarga de ingresar al archivo binario dentro de SystemCatalog encargado de llevar el recuento de cuales bases de datos existen.
            string systemDatabasesFilePath = Path.Combine(SystemCatalogPath, "SystemDatabases.databases"); //Preparamos la ruta hacía el archivo.

            using (var writer = new BinaryWriter(File.Open(systemDatabasesFilePath, FileMode.Append))) { //Lo abrimos y luego comenzamos a escribir sobre él.
                writer.Write(id);         // Escribimos el ID de la base de datos
                writer.Write(databaseName); // Escribimos el nombre de la base de datos
            }
        }

        public OperationStatus Set(string DataBaseToSet) { //Cambia la ruta donde crear tablas , es decir, en que base de datos crear las tablas.
            string databasePath = Path.Combine(DataPath, DataBaseToSet);

            // Verifica que la base de datos exista
            if (!Directory.Exists(databasePath)) {
                Console.WriteLine($"Error: La base de datos '{DataBaseToSet}' no existe.");
                return OperationStatus.Error;
            }

            // Obtiene el ID de la base de datos seleccionada y lo guarda de forma global.
            currentDatabaseId = GetDatabaseId(DataBaseToSet); //Bastante importante para poder 

            if (currentDatabaseId == null) { //En caso de que no encuentre un ID para la base actual significa que no ha sido creada una BD hasta el momento, es decir, no hay nada que SETear.
                Console.WriteLine($"Error: No se pudo encontrar el ID de la base de datos '{DataBaseToSet}'.");
                return OperationStatus.Error;
            }

            RutaDeterminadaPorSet = databasePath; //Guardamos de forma global la ruta hacía donde se encuentre la base actual, eso para poder crear Tablas e insertar datos en un futuro.
            Console.WriteLine($"Base de datos seleccionada: {DataBaseToSet}");
            return OperationStatus.Success;
        }

        public OperationStatus CreateTable(string tableName, List<ColumnDefinition> columns) //Operación para poder crear tablas vacías pero con encabezados a los cuales agregarles datos.
        {
            if (string.IsNullOrWhiteSpace(RutaDeterminadaPorSet) || RutaDeterminadaPorSet == DataPath + "\\" || currentDatabaseId == null)
            {
                Console.WriteLine("Error: No se ha seleccionado una base de datos. Use el comando SET primero.");
                return OperationStatus.Error;
            }

            // Verifica si la tabla ya existe
            if (TableExists(tableName)) {
                return OperationStatus.Error;
            }

            // Generar un ID único para la tabla
            int tableId = GetNextTableId();

            // Actualizar SystemTables con la nueva tabla, usando el ID de la base de datos seleccionada
            AddTableToCatalog(currentDatabaseId.Value, tableId, tableName);

            // Actualizar SystemColumns con la definición de las columnas
            AddColumnsToCatalog(tableId, columns); //Columns su definición no es más que la operación enviada por el socket pero todas sus columnas parseadas en el la clase CreateTable.cs, encargado de separar cada columna según su tipo
            //además crear una lista, es prácticamente eso, pasamos también esas columnas al método que actualiza el catalog.

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
                        // Validación adicional para prevenir escritura de valores nulos en column.Name y column.DataType
                        if (string.IsNullOrEmpty(column.Name)) 
                            throw new InvalidOperationException("El nombre de la columna no puede ser nulo o vacío.");
                        writer.Write(column.Name);

                        if (string.IsNullOrEmpty(column.DataType)) 
                            throw new InvalidOperationException("El tipo de dato de la columna no puede ser nulo o vacío.");
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

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en {RutaDeterminadaPorSet}");
                return OperationStatus.Success;
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al crear la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void AddColumnsToCatalog(int tableId, List<ColumnDefinition> columns) //Método encargado de añadir las columnas a un documento dentro de SystemCatalog.
        {
            string systemColumnsFilePath = Path.Combine(SystemCatalogPath, "SystemColumns.columns"); //Obtenemos la ruta hacía el documento que contendrá las columnas.

            //Anteriormente hubo un error aquí, y es que en lugar de agregar al archivo con Append, se sobre escribia, entonces solo se añadía una tanda de columnas.
            using (var writer = new BinaryWriter(File.Open(systemColumnsFilePath, FileMode.Append)))
            {
                foreach (var column in columns) //Se encarga de iterar sobre cada elemento en la lista de columnas que fué parseada en la clase CreateTables en Operations dentro de QueryProcessor
                { //Esto con el objetvo de actualizar el archivo de columnas en SystemCatalog.
                
                    if (string.IsNullOrWhiteSpace(column.Name) || string.IsNullOrWhiteSpace(column.DataType)) //Comprobación de errores.
                    {
                        throw new InvalidOperationException("El nombre de la columna o el tipo de dato no pueden ser nulos o vacíos.");
                    }

                    Console.WriteLine($"Guardando columna: {column.Name} con tipo {column.DataType} en SystemColumns.columns para la tabla con ID {tableId}"); //Debug
                    writer.Write(tableId);            // Escribimos el ID de la tabla
                    writer.Write(column.Name);        // Escribimos el nombre de la columna
                    writer.Write(column.DataType);    // Escribimos el tipo de dato de la columna
                    writer.Write(column.IsNullable);  // Escribimos si es nullable

                    // Mensaje de depuración para verificar si el atributo IsPrimaryKey se guarda correctamente
                    Console.WriteLine($"Guardando columna: {column.Name} con tipo {column.DataType} y PrimaryKey={column.IsPrimaryKey} en SystemColumns.columns para la tabla con ID {tableId}");

                    writer.Write(column.IsPrimaryKey);// Escribimos si es llave primaria
                    writer.Write(column.VarcharLength ?? 0); // Escribimos la longitud si es VARCHAR, por defecto 0
                }
            }
        }

        private bool TableExists(string tableName) { //Al igual que el método de las Bases crea una lista con todos las BDs existentes para luego comparar el nombre.
            var tables = ReadFromSystemTables(); //Aquí se almacena la lista generada por el método que crea las BDs
            return tables.Any(tbl => tbl.TableName == tableName); //Si la comparación encuentra una Tabla con el mismo nombre 
        }

        private int GetNextTableId() { //Igual que el método para las BDs se encarga de otorgar nuevos IDs.
            var tables = ReadFromSystemTables(); //Obtenemos todas las BDs existentes con sus IDs
            if (tables.Count == 0) { //si no hay, entonces a la nueva se le ortorga el ID de "1"
                return 1; // Primer ID
            }
            return tables.Max(t => t.TableId) + 1; //En caso de que hayan bases, entonces se obtiene el ID de la última BD y se le suma "+1", esto permite crear el nuevo ID.
        }

        public int GetTableId(string tableName) //Este método es super útil, ya que permite obtener el ID de la tabla de la cual se le pasa un nombre.
        { //Su uso es implementado en la clase Insert.cs, como es necesario extraer el esquema de las columnas para verificar antes de insertar, entonces este método se encarga de
            //obtener el ID de la tabla que sea actual, gracias a comparar su nombre con alguno de los existentes en la extración de ReadFromSystemTables en SystemCatalog.
            var tables = ReadFromSystemTables();
            var table = tables.FirstOrDefault(t => t.TableName == tableName);
            return table != default ? table.TableId : -1;
        }

        private List<(int DbId, int TableId, string TableName)> ReadFromSystemTables() { //Genera la lista de tablas para luego compararlas.
            string systemTablesFilePath = Path.Combine(SystemCatalogPath, "SystemTables.tables"); //Obtenemos la ruta del documento que contiene las tablas.
            
            if (!File.Exists(systemTablesFilePath)) {// Verificar si el archivo existe antes de intentar leer
                Console.WriteLine("Error: El archivo SystemTables.tables no existe.");
                return new List<(int, int, string)>();
            }
        
            var tableList = new List<(int, int, string)>(); //Estrucutra del almacén que contendrá todas las tablas, es una lista.
            using (var reader = new BinaryReader(File.Open(systemTablesFilePath, FileMode.Open))) {//Abrimos el archivo y comenzamos a agregar todas las tablas que estén ahí.
                while (reader.BaseStream.Position != reader.BaseStream.Length) {//Esto hasta que se llegue al máximo de la longitud)(largo) del archivo.
                    int dbId = reader.ReadInt32();
                    int tableId = reader.ReadInt32();
                    string tableName = reader.ReadString();
                    tableList.Add((dbId, tableId, tableName));
                }
            }
            return tableList; //Lista preparada para ser comparada.
        }

        private void AddTableToCatalog(int dbId, int tableId, string tableName) {
            string systemTablesFilePath = Path.Combine(SystemCatalogPath, "SystemTables.tables");

            using (var writer = new BinaryWriter(File.Open(systemTablesFilePath, FileMode.Append))) {
                writer.Write(dbId);        // ID de la base de datos
                writer.Write(tableId);     // ID de la tabla
                writer.Write(tableName);   // Nombre de la tabla
            }
        }

        //!!!!!!!!!!!!!!!!!!!!Este método tiene que ser reestructurado según como se pide en el documento.!!!!!!!!!!!!!!!!!!!!!
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
                        Console.WriteLine($"Columna agregada: Name={column.Name}, Type={column.DataType}, Nullable={column.IsNullable}, PrimaryKey={column.IsPrimaryKey}, VarcharLength={column.VarcharLength}");
                        columns.Add(column);
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
        
        public OperationStatus InsertIntoTable(string tableName, string[] columnas, string[] valores) //Permite insertar los datos en alguna tabla
        {//pero solo si se verificaron que dichos datos cumplen con la estructura esperada, esto se logra comparar en la clase dedicada para la operación
            // Insert.cs en Operations en QueryProcessor.

            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table"); //Obtenemos la ruta hacía el archivo.

            if (!File.Exists(fullPath))//Verificamos que exista.
            {
                Console.WriteLine($"Error: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open)) //Si existe, abrimos el archivo.
                using (BinaryReader reader = new BinaryReader(stream))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Lee la estructura de la tabla
                    reader.ReadString(); // TINYSQLSTART
                    int columnCount = reader.ReadInt32();
                    List<ColumnDefinition> tableColumns = new List<ColumnDefinition>(); //Creamos una lista donde almacenar la estructura actual.

                    for (int i = 0; i < columnCount; i++) //Vamos añadiendo.
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
                    while (reader.ReadString() != "DATASTART") { } //Cuando consiga la marca de inicio de donde comienza la Información almacenada
                    //entonces puede empieza a insertar.

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

        public List<ColumnDefinition> GetTableSchema(int tableId) //Permite obtener la estructura de una tabla para poder compararlo luego.
        {
            var columns = new List<ColumnDefinition>(); //Se crea una lista con la definición de las columnas previamente definidas en el ENUM.
            string systemColumnsFilePath = Path.Combine(SystemCatalogPath, "SystemColumns.columns"); //Definimos la ruta hacía el documento en SystemCatalog.
            
            using (var reader = new BinaryReader(File.Open(systemColumnsFilePath, FileMode.Open))) { //Abrimos el documento y para poder leer.
                while (reader.BaseStream.Position != reader.BaseStream.Length) { //Mientras no lleguemos al final vamos leyendo.

                    long currentFilePointer = reader.BaseStream.Position; // Depuración: imprimir el puntero actual del archivo
                    Console.WriteLine($"Puntero de archivo antes de leer: {currentFilePointer}");
            
                    int columnTableId = reader.ReadInt32();
                    Console.WriteLine($"Leyendo columna para la tabla con ID {columnTableId}");
            
                    if (columnTableId == tableId) {
                        var column = new ColumnDefinition();
                        column.Name = reader.ReadString();
                        column.DataType = reader.ReadString();
                        column.IsNullable = reader.ReadBoolean();
                        column.IsPrimaryKey = reader.ReadBoolean();
                        column.VarcharLength = reader.ReadInt32();
            
                        Console.WriteLine($"Columna encontrada: {column.Name}, Tipo: {column.DataType}");
                        columns.Add(column);
                    } else {
                        // Si no coincide, seguimos avanzando en el archivo.
                        // Leer los datos de la columna para avanzar el puntero correctamente
                        reader.ReadString(); // Name
                        reader.ReadString(); // DataType
                        reader.ReadBoolean(); // IsNullable
                        reader.ReadBoolean(); // IsPrimaryKey
                        reader.ReadInt32(); // VarcharLength
                    }
            
                    long newFilePointer = reader.BaseStream.Position; // Depuración: imprimir el puntero después de leer
                    Console.WriteLine($"Puntero de archivo después de leer: {newFilePointer}");
                }
            }
            
            if (columns.Count == 0) {
                Console.WriteLine($"Error: No se encontró ninguna columna para la tabla con ID {tableId}.");
            }
            
            return columns;
        }
        // Implementacion del DropTable 
        public OperationStatus DropTable(string tableName)
        {
            string tablePath = Path.Combine(RutaDeterminadaPorSet,tableName+".Table");
            if (!File.Exists(tablePath) ) 
            {
                Console.WriteLine($"tabla'{tableName}'no existe,");
                return OperationStatus.Error;
            }

                try 
                {   //Se almacena el resultado de buscar si hay información dentro de las tablas.
                    bool tableHasData = TableHasInfo(tableName);

                    if (tableHasData) //Si tiene información
                    {
                        Console.WriteLine($"La tabla '{tableName}' no puede ser eliminada porque contiene datos.");
                        return OperationStatus.Error;
                    }
            
                    // Si no tiene datos, procedemos a eliminar la tabla
                    File.Delete(tablePath);
                    RemoveTableFromSystemCatalog(tableName); //Eliminamos el registro del SystemCatalog
                    Console.WriteLine($"La tabla '{tableName}' se ha eliminado correctamente.");
                    return OperationStatus.Success;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al eliminar la tabla:{ex.Message}");
                    return OperationStatus.Error;
                }
        }

        public bool TableHasInfo(string tableName) //La idea está en aprovechar la palabra: "DATASTART" que se escribe al final de 
        { //las tablas cuando se hace CREATE TABLE, más precisamente al  final de escribir "ENDSTRUCTURE", con el fin de leer más hallá de 
            // esa marca y determinar si hay datos.

            //Ruta donde se almacena la tabla:
            string tablePath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table");

            //condición para verificar el inicio de la información:
            bool dataStarted = false;

            try
            {
                //Leer el archivo de la tabla:
                using (var reader = new StreamReader(tablePath))
                {
                    string? line; //Variable para indicar el lector

                    //Leer línea por línea
                    while ((line = reader.ReadLine()) != null)
                    {
                        //Buscamos el indicador que dice donde empieza la información insertada en la tabla.(Después de la estructura, es decir, columnas o encabezado.)
                        if (line.Contains("DATASTART"))
                        {
                            dataStarted = true;
                            continue; //Como ya encontramos donde debería empezar la información insertada, entonces comprobamos si después de dicha marca
                            //existe algo insertado, si se mantiene en blanco o null, entonces no hay información:
                        }

                        //Si ya hemos pasado la marca de inicio de datos, entonces comprobamos si hay información insertada más adelante de ella.
                        if (dataStarted && !string.IsNullOrWhiteSpace(line))
                        {
                            //Si ya estamos más allá de "DATASTART" y resulta que lo que leemos no es nullo o espacios en blanco, significa que hay información
                            //insertada, ya que una tabla vacía no debería de contener nada después de la marca de inicio.
                            Console.WriteLine($"La tabla '{tableName}' tiene datos y no puede ser eliminada.");
                            return true; // Se encontró información
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer la tabla {tableName}: {ex.Message}");
            }

            // Si no se encontraron datos después del marcador DATASTART, la tabla está vacía
            return false;
        }

        public bool RemoveTableFromSystemCatalog(string tableName) //Método especial para eliminar el registro del Documento del SystemCatalog(SystemTables)
        {
            string systemTablesFilePath = Path.Combine(SystemCatalogPath, "SystemTables.tables"); //Ruta hacía el archivo binario.

            // Lista que almacenará todos los datos de las tablas (dbId, tableId, tableName) excepto la que se va a eliminar
            var tableList = new List<(int DbId, int TableId, string TableName)>();

            try
            {
                // Leemos el archivo binario y extraemos toda su información
                using (var reader = new BinaryReader(File.Open(systemTablesFilePath, FileMode.Open)))
                {
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        int dbId = reader.ReadInt32();      // Leer el dbId
                        int tableId = reader.ReadInt32();   // Leer el tableId
                        string currentTableName = reader.ReadString(); // Leer el nombre de la tabla

                        // Si encontramos la tabla a eliminar, no la agregamos a la lista
                        if (currentTableName != tableName)
                        {
                            tableList.Add((dbId, tableId, currentTableName));
                        }
                    }
                }

                // Reescribimos todo el contenido menos la tabla deseada, la cual no fue agregada a la lista
                using (var writer = new BinaryWriter(File.Open(systemTablesFilePath, FileMode.Create)))
                {
                    foreach (var table in tableList)
                    {
                        writer.Write(table.DbId);     // Escribir el dbId
                        writer.Write(table.TableId);  // Escribir el tableId
                        writer.Write(table.TableName); // Escribir el nombre de la tabla
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
    }
}
