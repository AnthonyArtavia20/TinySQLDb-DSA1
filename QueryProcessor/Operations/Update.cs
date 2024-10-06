using Entities;
using StoreDataManager;
using StoreDataManager.StoreOperations; //Para poder acceder a TableExist en StoreOperations CreateTable.

namespace QueryProcessor.Operations
{
    public class Update
    {
        public OperationStatus Execute(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue)
        {
            //Verifica que la tabla exista:
            if (!CreateTableOperation.TableExists(tableName))
            {
                Console.WriteLine($"Error al verificar la tabla mientras se crea el indice: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            return Store.GetInstance().Update(tableName, columnToUpdate, newValue, whereColumn, whereValue);
        }
    }
}
