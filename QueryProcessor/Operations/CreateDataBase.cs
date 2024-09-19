using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    public class CreateDataBase
    {
        public OperationStatus Execute(string NombreDeArchivo)
        {
            return Store.GetInstance().CreateDataBase(NombreDeArchivo);
        }
    }
}
