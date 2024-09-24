namespace ApiInterface.InternalModels
{
    internal enum RequestType 
    { 
        SQLSentence = 0 //Con esto indicamos que puede recibir solicitudes tipo SQL, en un futuro se puede añadir más tipos.
    }

    internal class Request
    {
        public required RequestType RequestType { get; set; } 

        public required string RequestBody { get; set; }
    }
}
