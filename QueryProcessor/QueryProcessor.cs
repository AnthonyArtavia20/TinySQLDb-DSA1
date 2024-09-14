/*
    Se importa la solución del StoredDataManager para realizar las operaciones sobre los datos almacenados
*/

using StoredDataManager;

namespace QueryProcessor
{
    public class QueryProcessorHandler
    {
        private StoredDataManagerHandler? _dataManager; //Variable del tipo StoredDataManegerHandler se usa
        //para poder acceder a los datos.

        public void Initialize(StoredDataManagerHandler dataManager)
        {
            _dataManager = dataManager;
        }

        public string ProcessQuery(string query)
        {
            // Lógica para procesar la consulta
            // Usar _dataManager para acceder a los datos cuando sea necesario
            return "Ejem: Resultado de la consulta";
        }
    }
}