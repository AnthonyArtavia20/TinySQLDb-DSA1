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
                const string CreateTableKeyWord = "CREATE TABLE";
                var TableName = sentence.Substring(CreateTableKeyWord.Length).Trim();

                if (string.IsNullOrWhiteSpace(TableName)) {throw new InvalidOperationException("Debe ingresar un nombre para la tabla");}

                var status = new CreateTable().Execute(TableName);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("SET"))
            {
                const string SetKeyWord = "SET";
                var DataBaseToSet = sentence.Substring(SetKeyWord.Length).Trim();

                if (string.IsNullOrWhiteSpace(DataBaseToSet)) {throw new InvalidOperationException("Debe ingresar un nombre de un a BD para settear y realizar consultas");}

                var result = new Set().Execute(DataBaseToSet);
                return (result, string.Empty);
            }     
            if (sentence.StartsWith("SELECT"))
            {
                const string selectDataBaseKeyWord = "SELECT * FROM";
                var DataBaseToSelect = sentence.Substring(selectDataBaseKeyWord.Length).Trim();

                if (string.IsNullOrWhiteSpace(DataBaseToSelect)) {throw new InvalidOperationException("Debe ingresar un nombre de un a BD para seleccionar");}

                var result = new Select().Execute(DataBaseToSelect); //Se extrae el resultado/Data de la instancia del Select
                return result;//Y luego se retorna poco a poco camino al ApiInterface.
            }
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                const string createDatabaseKeyword = "CREATE DATABASE"; //Creamos una constante con la parte de la oración que se quiere quitar, es constante, pues nunca cambia.
                var databaseName = sentence.Substring(createDatabaseKeyword.Length).Trim(); //Le quitamos los espacios en blanco y además le quitamos la constante.

                //Manejo de errores
                if (string.IsNullOrWhiteSpace(databaseName)) { throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");}

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