using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class Delete
    {
        public (OperationStatus Status, string Data) Execute(string tableName, string columnName, string conditionValue, string operatorValue)
        {
            
            return Store.GetInstance().DeleteWhere(tableName, columnName, conditionValue, operatorValue);
        }
    }
}