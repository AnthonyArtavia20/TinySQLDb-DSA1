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
                // Create an object with a hardcoded.
                // First field is an int, second field is a string of size 30,
                // third is a string of 50
                int id = 1;
                string nombre = "Isaac".PadRight(30); // Pad to make the size of the string fixed
                string apellido = "Ramirez".PadRight(50);

                writer.Write(id);
                writer.Write(nombre);
                writer.Write(apellido);
            }
            return OperationStatus.Success;
        }

        public OperationStatus Select() //QUEDA PENDIENTE ---> Mejorar la lógica para poder enviar la respuesta por el socket,  ya que esto da la respuesta
        {       //por la consola del servidor, y no es la idea, debería de ser la del PowerShell, pero como no envia lo creado, nunca lo formatea en formato tabla.

            var tablePath = $@"{DataPath}\TESTDB\ESTUDIANTES.Table";
            DataTable table = new DataTable();

            // Definir las columnas de la tabla
            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Nombre", typeof(string));
            table.Columns.Add("Apellido", typeof(string));

            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                //Esto es para poder mostrar el resultado y entender como funciona esto.
                while (stream.Position < stream.Length)
                {
                    int id = reader.ReadInt32();
                    string nombre = reader.ReadString().Trim();
                    string apellido = reader.ReadString().Trim();

                    // Agregar fila a la tabla
                    table.Rows.Add(id, nombre, apellido);
                }
            }

            // Mostrar la tabla
            foreach (DataRow row in table.Rows)
            {
                Console.WriteLine($"{row["Id"],-5} {row["Nombre"],-30} {row["Apellido"],-50}");
            }

            return OperationStatus.Success; // Devolver el enum OperationStatus
        }
    }
}
