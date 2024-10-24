using Entities;
using StoreDataManager;
using StoreDataManager.StoreOperations; //Para poder acceder a TableExist en StoreOperations CreateTable.

namespace QueryProcessor.Operations
{
    public class Update
    {
        public OperationStatus Execute(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue, string operatorValue)
        {
            // Verificar que la tabla exista
            if (!CreateTableOperation.TableExists(tableName))
            {
                Console.WriteLine($"Error: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            // Llamar a la operación de actualización con los parámetros adecuados
            return Store.GetInstance().Update(tableName, columnToUpdate, newValue, whereColumn, whereValue, operatorValue);
        }
    }
}
