using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;
using System.Text.Json;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor : IProcessor //Permite dirigir los tipos de consultas a diferentes procesadores.
    {//en este caso permite dirigir las solicitudes SQLSentence al SQLQueryProcessor.
        public Request Request { get; }

        public SQLSentenceProcessor(Request request) //Realiza automáticamente el proceso para ejecutar las operaciones tipo SQL
        {
            Request = request;
        }

        public Response Process()
        {
            var sentence = this.Request.RequestBody.Trim(); //Se añade ".Trim" para evitar que la respuesta lleve espacios.
            var (status, data) = SQLQueryProcessor.Execute(sentence); //Se recibe la información enviada desde 
            // el StoredDataManager
            return this.ConvertToResponse(status, data); //Finalmente se retorna Serealizado como tipo Data,
            //para que pueda ser enviado por el Socket
        }

        private Response ConvertToResponse(OperationStatus status, string data) //Cuando ya se procesó la solicitud, la procesamos 
        {//para poder pasarla como respuesta atravez del socket.
            var response = new Response
            {
                Status = status,
                Request = this.Request,
                ResponseBody = JsonSerializer.Serialize(new { Data = data })
            };
            Console.WriteLine($"Response created: {JsonSerializer.Serialize(response)}"); // Debug line
            return response;
        }
    }
}