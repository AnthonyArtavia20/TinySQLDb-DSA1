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
                var createTable = new CreateTable();
                var status = createTable.Execute(sentence);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("SET"))
            {
                const string SetKeyWord = "SET";
                var DataBaseToSet = sentence.Substring(SetKeyWord.Length).Trim();

                if (string.IsNullOrWhiteSpace(DataBaseToSet)) 
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una BD para settear y realizar consultas");
                }

                var result = new Set().Execute(DataBaseToSet);
                return (result, string.Empty);
            }     
            if (sentence.StartsWith("SELECT"))
            {
                const string selectDataBaseKeyWord = "SELECT * FROM";
                var DataBaseToSelect = sentence.Substring(selectDataBaseKeyWord.Length).Trim();

                if (string.IsNullOrWhiteSpace(DataBaseToSelect)) 
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una BD para seleccionar");
                }

                var result = new Select().Execute(DataBaseToSelect);
                return result;
            }
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                const string createDatabaseKeyword = "CREATE DATABASE";
                var databaseName = sentence.Substring(createDatabaseKeyword.Length).Trim();

                if (string.IsNullOrWhiteSpace(databaseName)) 
                { 
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }

                var result = new CreateDataBase().Execute(databaseName);
                return (result, string.Empty);
            } 
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}