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
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";

        public Store()
        {
            this.InitializeSystemCatalog();
            
        }

        private void InitializeSystemCatalog()
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }

        public OperationStatus CreateTable()
        {
            // Creates a default DB called TESTDB
            Directory.CreateDirectory($@"{DataPath}\TESTDB");

            // Creates a default Table called ESTUDIANTES
            var tablePath = $@"{DataPath}\TESTDB\ESTUDIANTES.Table";

            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryWriter writer = new (stream))
            {
                int id = 1;
                string nombre = "NombreEjemplo".PadRight(30);
                string apellido = "Ramirez".PadRight(50);

                writer.Write(id);
                writer.Write(nombre);
                writer.Write(apellido);
            }
            return OperationStatus.Success;
        }

        public (OperationStatus Status, string Data) Select()
        {
            //La lógica principal acá es poder leer la información de una tabla, y empaquetarla
            //para luego retornarla como un resultado, esto camino al Socket para ser enviado al PowerShell
            //y mostrar el resultado en formato tabla.
            var tablePath = $@"{DataPath}\TESTDB\ESTUDIANTES.Table";
            StringBuilder resultBuilder = new StringBuilder();

            try
            {
                using (FileStream stream = File.Open(tablePath, FileMode.Open))
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
