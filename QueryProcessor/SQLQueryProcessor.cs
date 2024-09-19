using Entities;
using QueryProcessor.Exceptions;
using QueryProcessor.Operations;

namespace QueryProcessor
{
    public class SQLQueryProcessor
    {
        public static (OperationStatus Status, string Data) Execute(string sentence) //Recibe la operación como una oración completa.
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
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                const string createDatabaseKeyword = "CREATE DATABASE"; //Creamos una constante con la parte de la oración que se quiere quitar, es constante, pues nunca cambia.
                var databaseName = sentence.Substring(createDatabaseKeyword.Length).Trim(); //Le quitamos los espacios en blanco y además le quitamos la constante.
                if (string.IsNullOrWhiteSpace(databaseName)) //Manejo de errores.
                {
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }
                var result = new CreateDataBase().Execute(databaseName); //Llamamos la clase y método adecuado para crear la base de datos y le pasamos el nombre a poner en el directorio a crear.
                return (result, string.Empty);
            } 
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}