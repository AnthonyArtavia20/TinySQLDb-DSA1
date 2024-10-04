using Entities;
using StoreDataManager;
using StoreDataManager.StoreOperations; //Para poder acceder a TableExist en StoreOperations CreateTable.

namespace QueryProcessor.Operations
{
    public class CreateIndexes
    {
        public OperationStatus Execute(string indexName, string tableName, string columnNameKeyValue, string indexType)
        {
            //Verificaciones  para toda la información ingresante:
            
            //Verifica que la tabla exista:
            if (!CreateTableOperation.TableExists(tableName))
            {
                Console.WriteLine($"Error al verificar la tabla mientras se crea el indice: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            // Verificar que la columna exista en la tabla
            if (!ColumnExists(tableName, columnNameKeyValue))
            {
                Console.WriteLine($"Error: La columna '{columnNameKeyValue}' no existe en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            //Verificar en SystemIndexes que el indice a crear no se haya creado antes.
            // Verificar que el índice no exista ya.
            if (IndexExists(indexName, tableName))
            {
                Console.WriteLine($"Error: Ya existe un índice con el nombre '{indexName}' en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }

            // Verificar que el tipo de índice sea válido
            if (indexType != "BTREE" && indexType != "BST")
            {
                Console.WriteLine($"Error: Tipo de índice '{indexType}' no válido. Use 'BTREE' o 'BST'.");
                return OperationStatus.Error;
            }

            //Si se verificó que todo esté correcto, entonces se procede a enviar los datos relevantes para crear 
            //el indice.
            return Store.GetInstance().CreateIndexes(indexName, tableName, columnNameKeyValue, indexType);
        }

        private bool ColumnExists(string tableName, string columnName)
        {
            // Implementar lógica para verificar si la columna existe en la tabla
            // Retornar true si la columna existe, false en caso contrario

            int IDtable = Insert.GetTableId(tableName); //Se obtiene el ID de la tabla a consultar por medio del método creado en Insert.cs
            if (IDtable == -1)
            {
                Console.WriteLine($"Error al buscar la columa en CreateIndexes.cs: La tabla '{tableName}' no existe. Id: {IDtable}");
                return false;
            }

            string systemColumnsFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemColumns.columns"); //Se consigue la ruta hacía el archivo.

            if (!File.Exists(systemColumnsFilePath)) //Siempre que se inicia el programa se crea el SystemColumns pero por si aquello.
            {
                Console.WriteLine("Error: El archivo SystemColumns.columns no existe.");
                return false;
            }

            using (var reader = new BinaryReader(File.Open(systemColumnsFilePath, FileMode.Open)))
            {
                while (reader.BaseStream.Position != reader.BaseStream.Length)
                {
                    // Leer el ID de la tabla asociada a la columna
                    int columnTableId = reader.ReadInt32();
                    // Leer el nombre de la columna
                    string currentColumnName = reader.ReadString();

                    // Leer y omitir el resto de la información de la columna
                    string dataType = reader.ReadString();
                    bool isNullable = reader.ReadBoolean();
                    bool isPrimaryKey = reader.ReadBoolean();
                    int varcharLength = reader.ReadInt32();

                    // Comparar el ID de la tabla y el nombre de la columna
                    if (columnTableId == IDtable && currentColumnName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                    {
                        return true; // La columna existe
                    }
                }
            }

            // Si llegamos aquí, la columna no existe
            Console.WriteLine($"Error al comprobar columna antes de crear el Índice: La columna '{columnName}' no existe en la tabla '{tableName}'.");
            return false; // La columna no se encontró
        }

        private bool IndexExists(string indexName, string tableName)
        {
            string SystemIndexesFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemIndexes.Indexes"); //Se consigue la ruta hacía el archivo.

            using (var reader = new BinaryReader(File.Open(SystemIndexesFilePath, FileMode.Open)))
            {
                while (reader.BaseStream.Position != reader.BaseStream.Length)
                {
                    string currentIndexName = reader.ReadString();
                    string currentTableName = reader.ReadString();

                    if (currentIndexName == indexName && currentTableName == tableName)
                    {
                        Console.WriteLine($"El índice '{indexName}' ya existe en la tabla '{tableName}'.");
                        return true;
                    }
                }
            }

            Console.WriteLine($"Anterior mente es la tabla '{tableName}' no se había creado '{indexName}', por lo tanto se permite crear este.");
            return false; // La columna no se encontró
        }
    }
}