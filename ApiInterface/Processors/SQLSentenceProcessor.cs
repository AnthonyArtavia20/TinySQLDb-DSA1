using ApiInterface.InternalModels;
using ApiInterface.Models;
using Entities;
using QueryProcessor;
using System.Text.Json;

namespace ApiInterface.Processors
{
    internal class SQLSentenceProcessor : IProcessor 
    {
        public Request Request { get; }

        public SQLSentenceProcessor(Request request)
        {
            Request = request;
        }

        public Response Process()
        {
            var sentence = this.Request.RequestBody;
            var (status, data) = SQLQueryProcessor.Execute(sentence); //Se recibe la información enviada desde 
            // el StoredDataManager
            return this.ConvertToResponse(status, data); //Finalmente se retorna Serealizado como tipo Data,
            //para que pueda ser enviado por el Socket
        }

        private Response ConvertToResponse(OperationStatus status, string data)
        {
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