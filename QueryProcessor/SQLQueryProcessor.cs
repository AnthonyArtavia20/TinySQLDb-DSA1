using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static (OperationStatus Status, string Data) Execute(string sentence)
        {
            if (sentence.StartsWith("CREATE TABLE"))
            {
                var status = new CreateTable().Execute();
                return (status, string.Empty);
            }   
            if (sentence.StartsWith("SELECT"))
            {
                var result = new Select().Execute(); //Se extrae el resultado/Data de la instancia del Select
                return result;//Y luego se retorna poco a poco camino al ApiInterface.
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}