using System.Net.Sockets;
using System.Net;
using ApiInterface.InternalModels;
using System.Text.Json;
using ApiInterface.Exceptions;
using ApiInterface.Processors;
using ApiInterface.Models;

namespace ApiInterface
{
    public class Server //Lógica para poder iniciar el Servidor cuando se ejecuta "dotnet run" conecta los Sockets.
    {
        private static IPEndPoint serverEndPoint = new(IPAddress.Any, 11000); //Puerto estático, pero puede ser Dinámico.
        private static int supportedParallelConnections = 1; //Solo una conexión a la vez.

        public static async Task Start()
        {
            using Socket listener = new(serverEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listener.Bind(serverEndPoint);
            listener.Listen(supportedParallelConnections);
            Console.WriteLine($"Server ready at {serverEndPoint.ToString()}");

            while (true)
            {
                var handler = await listener.AcceptAsync();
                try
                {
                    //Aquí se llaman uno a uno los métodos para poder procesar las consultas
                    var rawMessage = GetMessage(handler); //Se obtiene el mensaje enviado por el socket
                    var requestObject = ConvertToRequestObject(rawMessage); //Se dessealiza (PARSEA)
                    var response = ProcessRequest(requestObject); //Y luego se procesa y se crea lo solicitado
                    SendResponse(response, handler); //La respuesta serealizada es enviada por el socket por medio de este método.
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    await SendErrorResponse("Unknown exception while trying to connect", handler);
                }
                finally
                {
                    handler.Close();
                }
            }
        }

        private static string GetMessage(Socket handler) //Permite extraer el mensaje enviado desde el cliente.
        {
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamReader reader = new StreamReader(stream))
            {
                return reader.ReadLine() ?? String.Empty;
            }
        }

        private static Request ConvertToRequestObject(string rawMessage) //Luego se deserializa el mensaje para poder ser procesado.
        {
            return JsonSerializer.Deserialize<Request>(rawMessage) ?? throw new InvalidRequestException();
        }

        private static Response ProcessRequest(Request requestObject) //Se ingresa la solicitud para poder compararla en el ProcessorFactory
        {//esto para luego determinar que clase de operación es.

            var processor = ProcessorFactory.Create(requestObject);
            return processor.Process(); //Se recibe la información serealizada y empaqueda, la cual contiene la
            //información tanto del estado de respuesta como la información(Data) generada por la consulta
        }

        private static void SendResponse(Response response, Socket handler) //Una vez que se procesó la solicitud, la serealizamos para poder enviarla
        {// por el socket.
            using (NetworkStream stream = new NetworkStream(handler))
            using (StreamWriter writer = new StreamWriter(stream))
            {
                writer.WriteLine(JsonSerializer.Serialize(response));
            }
        }

        private static Task SendErrorResponse(string reason, Socket handler) //En caso de error, se utiliza este método para poder avisar.
        {
            throw new NotImplementedException();
        }
    }
}
