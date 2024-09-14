//Importamos cada solución a Program.cs ya que es el responsable de iniciar el programa.
using ApiInterface;
using QueryProcessor;
using StoredDataManager;

namespace TinySQLDb
{
    class Program
    {
        static void Main(string[] args)
        {
            /*Extraemos instancias de cada clase correspondiente a cada solución
            Propósito: Poeder acceder a cada método y lograr "pasar" cada solicitud del cliente siguiendo
            el flujo de trabajo establecido.*/
            
            var apiInterface = new ApiInterfaceHandler();
            var queryProcessor = new QueryProcessorHandler();
            var dataManager = new StoredDataManagerHandler();

            // Configurar y iniciar el servidor
            apiInterface.StartServer(queryProcessor, dataManager);
        }
    }
}