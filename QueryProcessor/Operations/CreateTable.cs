using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string TableName)
        {
            return Store.GetInstance().CreateTable(TableName);
        }
    }
}
