using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class Set
    {
        public OperationStatus Execute(string DataBaseToSet)
        {
            return Store.GetInstance().Set(DataBaseToSet);
        }
    }
}
