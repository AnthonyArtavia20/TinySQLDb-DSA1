using Entities;
using StoreDataManager;
namespace QueryProcessor.Operations
{
    public class DropTable
    {
        public OperationStatus Execute(string tableName)
        {
            return Store.GetInstance().DropTable(tableName);
        }
    }
}