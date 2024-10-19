using Entities;
using StoreDataManager.StoreOperations; //Para poder acceder a la información de las demás clases, especialmente Store.

namespace StoreDataManager
{
    public class IndexManager
    {
        /*  
            Esta clase es la encarga de la reconstrucción de los indices guardados en el archivo: "SystemIndexes.Indexes", que anteriormente
            se crearon con las sentencias CREATEINDEX. Se reutiliza la clase de creación de indices pero se identifica si es creación de indices
            o simplemente reconstructor.
        */
        public static void RecreateIndicesOnStartup() //Método que extrae la info de cada indices y la pasa al creador de indices.
        {
            string systemIndexesFilePath = Path.Combine(ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes");
            
            if (!File.Exists(systemIndexesFilePath))
            {
                Console.WriteLine("No se encontró el archivo SystemIndexes.Indexes. No hay índices para recrear.");
                return;
            }

            if (new FileInfo(systemIndexesFilePath).Length == 0)
            {
                Console.WriteLine("El archivo SystemIndexes.Indexes está vacío. No hay índices para recrear.");
                return;
            }
    
            try
            {
                using (var reader = new BinaryReader(File.Open(systemIndexesFilePath, FileMode.Open, FileAccess.Read, FileShare.Read)))
                {
                    //Se abre el archivo que contiene los indices, esto lo realiza en modo compartido y en lectura, para no interrumpir otros procesos que
                    //pueden estarlo necesitando.
                    while (reader.BaseStream.Position < reader.BaseStream.Length)
                    {
                        try
                        {
                            string indexName = reader.ReadString();
                            string databaseName = reader.ReadString();
                            string tableName = reader.ReadString();
                            string columnName = reader.ReadString();
                            string indexType = reader.ReadString();

                            Console.WriteLine($"Recreando indice con nombre: {indexName} para la base de datos: {databaseName}, tabla: {tableName}, Key: {columnName}, tipo de estructura: {indexType}");

                            //Se llama al método encargado de enviar la información substraida al creador de indices.
                            RecreateIndex(indexName, databaseName, tableName, columnName, indexType);
                        }
                        catch (EndOfStreamException)
                        {
                            Console.WriteLine("Se alcanzó el final del archivo SystemIndexes.Indexes.");
                            break;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al leer el archivo SystemIndexes.Indexes: {ex.Message}");
            }
        }
    
        private static void RecreateIndex(string indexName, string databaseName, string tableName, string columnName, string indexType)
        {
            try
            {
                Console.WriteLine($"Lo que recibe el reconstructor es: Indice: {indexName}, databasename: {databaseName}, nombre de la tabla: {tableName}, nombre de la columna a buscar: {columnName} y el tipo de arbol: {indexType}");

                if (!Directory.Exists(databaseName))
                {
                    Console.WriteLine($"La base de datos '{databaseName}' no existe. No se puede recrear el índice '{indexName}'.");
                    return;
                }

                //Se crea un objeto de la clase CreateIndexStoreOperation, se le pasan los parámetros necesarios y se ejecuta el métooo
                //Se reutiliza la lógica de creación de indices.
                var createIndexesOperation = new CreateIndexesStoreOperation(Store.DataPath, ConfigPaths.SystemCatalogPath, databaseName);
                var result = createIndexesOperation.Execute(indexName, tableName, columnName, indexType, true);
    
                if (result == OperationStatus.Success)
                {
                    Console.WriteLine($"Índice '{indexName}' recreado exitosamente para la tabla '{tableName}' en la base de datos '{databaseName}'.");
                }
                else
                {
                    Console.WriteLine($"Error al recrear el índice '{indexName}' para la tabla '{tableName}' en la base de datos '{databaseName}'.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al recrear el índice '{indexName}': {ex.Message}");
            }
        }
    }
}