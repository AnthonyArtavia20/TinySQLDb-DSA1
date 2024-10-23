//Última actualización 18/10/2024
//11:20am

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

        public const string DatabaseBasePath = @"C:\TinySql\"; //Se crea la carpeta que contendrá todo lo relacionado a la data del programa.
        public const string DataPath = $@"{DatabaseBasePath}\Data"; //Además se crea una carpeta que contendrá la data, es decir las bases de datos y demás.
        //Pasó a ser global en el archivo de configuración de rutas "ConfigPaths" -- > private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";//Contendrá archivos binarios con la información total de todas las bases de datos, con sus tablas y demás.
        public string RutaDeterminadaPorSet = $@"{DataPath}\"; //Ruta que cambiará constantemente ya que la determina la operación SET
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
        public OperationStatus Update(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue, string operatorValue)
        {
            var updateOperation = new UpdateOperation(RutaDeterminadaPorSet);
            return updateOperation.Execute(tableName, columnToUpdate, newValue, whereColumn, whereValue, operatorValue);
        }
        //Ver esto de la clase String
        public (OperationStatus, String) DeleteWhere(string tableName, string columnName, string conditionValue, string operatorValue)
        {
            var DeleteOperation = new DeleteOperation(RutaDeterminadaPorSet);
            return DeleteOperation.Execute(tableName, columnName, conditionValue,operatorValue);
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
        public OperationStatus CreateIndexes(string indexName, string tableName, string columnName, string indexType)
        {
            var CreateIndexesStoreOperation = new CreateIndexesStoreOperation(DataPath, Entities.ConfigPaths.SystemCatalogPath, RutaDeterminadaPorSet);
            return CreateIndexesStoreOperation.Execute(indexName, tableName, columnName, indexType);
        }
        public (OperationStatus Status, string Data) Select(string tableName,string columnName, string conditionValue, string operatorValue, List<string> columnasSeleccionadas = null)
        {
            var selectOperation = new SelectOperation(RutaDeterminadaPorSet);
            return selectOperation.Select(tableName,columnasSeleccionadas,columnName,conditionValue,operatorValue);
        }
    }   
}