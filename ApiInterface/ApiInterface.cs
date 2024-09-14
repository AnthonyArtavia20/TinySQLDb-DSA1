/*
Importamos las soluciones a las cuales ApiInterface debería tener acceso:/únicamente la del QueryProccesor 
ya que ApiInterface se encarga de las conexiones entrantes y las dirige al QueryProcessor, además de utilizar
la solución StoredDataManajer para acceder a los datos cuando sea necesario.
*/

using QueryProcessor;
using StoredDataManager;

namespace ApiInterface
{
    public class ApiInterfaceHandler
    {
        //Atributos necesarios, encargados de procesar las solicitudes.
        private QueryProcessorHandler? _queryProcessor;
        private StoredDataManagerHandler? _dataManager;

        public void StartServer(QueryProcessorHandler queryProcessor, StoredDataManagerHandler dataManager)
        {
            _queryProcessor = queryProcessor;
            _dataManager = dataManager;
            // Lógica para iniciar el servidor y manejar conexiones
            // Aquí --> X <--
        }

        public void HandleRequest(string request)
        {
            var queryResult = _queryProcessor.ProcessQuery(request);
            // Procesar el resultado y enviarlo de vuelta al cliente
        }
    }
}