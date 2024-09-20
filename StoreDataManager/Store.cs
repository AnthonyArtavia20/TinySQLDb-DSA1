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

        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private string RutaDeterminadaPorSet = $@"{DataPath}\"; //Ruta que cambiará constantemente ya que la determina la operación SET
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";

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
        
        public OperationStatus Set(string DataBaseToSet)
        {
            string databasePath = Path.Combine(DataPath, DataBaseToSet);
            if (!Directory.Exists(databasePath))
            {
                Console.WriteLine($"Error: La base de datos '{DataBaseToSet}' no existe.");
                return OperationStatus.Error;
            }
        
            RutaDeterminadaPorSet = databasePath;
            Console.WriteLine($"Base de datos seleccionada: {DataBaseToSet}");
            return OperationStatus.Success;
        }

        public OperationStatus CreateTable(string tableName, List<ColumnDefinition> columns)
        {
            if (string.IsNullOrWhiteSpace(RutaDeterminadaPorSet) || RutaDeterminadaPorSet == DataPath + "\\")
            {
                Console.WriteLine("Error: No se ha seleccionado una base de datos. Use el comando SET primero.");
                return OperationStatus.Error;
            }

            string tablePath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table");

            try
            {
                using (FileStream stream = File.Open(tablePath, FileMode.CreateNew))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Escribir el número de columnas
                    writer.Write(columns.Count);

                    // Escribir las definiciones de columnas en orden por medio de un loop que pase por toda la lista que contiene los datos a escribir
                    foreach (var column in columns)
                    {
                        writer.Write(column.Name.PadRight(30));
                        writer.Write(column.DataType.PadRight(20));
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);
                        if (column.DataType.StartsWith("VARCHAR"))
                        {
                            writer.Write(column.VarcharLength ?? 0);
                        }
                    }
                }

                UpdateSystemCatalog(tableName, columns); //Se actualiza el catálogo con las nuevas tablas y su contenido.

                Console.WriteLine($"Tabla '{tableName}' creada exitosamente en {RutaDeterminadaPorSet}");
                return OperationStatus.Success;
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error al crear la tabla: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private void UpdateSystemCatalog(string tableName, List<ColumnDefinition> columns) //Encargado de actualizar la carpeta que conteien toda la información de las base de datos
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

        public (OperationStatus Status, string Data) Select(string NombreDeTableASeleccionar)
        {
            //La lógica principal acá es poder leer la información de una tabla, y empaquetarla
            //para luego retornarla como un resultado, esto camino al Socket para ser enviado al PowerShell
            //y mostrar el resultado en formato tabla.
            string tableName = NombreDeTableASeleccionar + ".Table";
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName);

            StringBuilder resultBuilder = new StringBuilder();
            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Se añade una cabecera donde ese indican los títulos.
                    resultBuilder.AppendLine("Id,Nombre,Apellido");

                    while (stream.Position < stream.Length)
                    {
                        int id = reader.ReadInt32();
                        string nombre = reader.ReadString().Trim();
                        string apellido = reader.ReadString().Trim();

                        // Se añaden filas con al información que haya.
                        resultBuilder.AppendLine($"{id},{nombre},{apellido}");
                    }
                }

                return (OperationStatus.Success, resultBuilder.ToString());
            }
            catch (Exception ex) //En caso de error al leer los archivos binarios
            {
                Console.WriteLine($"Error reading file: {ex.Message}");
                return (OperationStatus.Error, $"Error: {ex.Message}");
            }
        }
    }
}
