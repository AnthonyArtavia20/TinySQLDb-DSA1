using Entities;
using StoreDataManager;
using StoreDataManager.StoreOperations; //Para poder acceder a TableExist en StoreOperations CreateTable.

namespace QueryProcessor.Operations
{
    public class Delete
    {
        //
        public OperationStatus Execute(string tableName, string whereColumn)
        {
            //Verifica que la tabla exista:
            if (!CreateTableOperation.TableExists(tableName))
            {
                Console.WriteLine($"Error al verificar la tabla mientras se crea el indice: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            return Store.GetInstance().Delete(tableName, whereColumn);
        }
    }
}


