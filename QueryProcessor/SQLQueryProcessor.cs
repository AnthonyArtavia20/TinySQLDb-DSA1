using System.Text.RegularExpressions;
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
                var createTable = new CreateTable(); //Para todas las operaciones se crea una instancia de la operación a realizar.
                var status = createTable.Execute(sentence);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("INSERT INTO"))
            {
                var insert = new Insert();
                var status = insert.Execute(sentence);
                return (status, string.Empty);
            }
            if (sentence.StartsWith("SET"))
            {
                const string SetKeyWord = "SET";
                var DataBaseToSet = sentence.Substring(SetKeyWord.Length).Trim(); //De esta manera se puede obtener el nombre de la base de datos a settear.

                if (string.IsNullOrWhiteSpace(DataBaseToSet)) //En caso de que sea nula o solo tiene espacios
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una BD para settear y realizar consultas");
                }

                var result = new Set().Execute(DataBaseToSet);//Pasamos el nombre de la base de datos a settear como ruta para crear tablas
                return (result, string.Empty);//Devolvemos éxito.
            }     
            if (sentence.StartsWith("SELECT"))
            {
                const string selectDataBaseKeyWord = "SELECT * FROM";
                var DataBaseToSelect = sentence.Substring(selectDataBaseKeyWord.Length).Trim(); //Igual, eliminamos la pabra clave.

                if (string.IsNullOrWhiteSpace(DataBaseToSelect))  //En caso de que se ingrese mal.
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una BD para seleccionar");
                }

                var result = new Select().Execute(DataBaseToSelect); //Pasamos el nombre de la pase de datos a seleccionar.
                return result;
            }
            if (sentence.StartsWith("CREATE DATABASE"))
            {
                const string createDatabaseKeyword = "CREATE DATABASE";
                var databaseName = sentence.Substring(createDatabaseKeyword.Length).Trim(); //De igual forma substraemos el nombre de la base de datos a crear.

                if (string.IsNullOrWhiteSpace(databaseName)) 
                { 
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }

                var result = new CreateDataBase().Execute(databaseName); //Pasamos dicho nombre.
                return (result, string.Empty);
            }
            // Implementacion del DROP TABLE 
            if (sentence.StartsWith("DROP TABLE"))
            {
                const string dropDatabaseKeyword = "DROP TABLE";
                var tableName = sentence.Substring(dropDatabaseKeyword.Length).Trim(); //De igual forma substraemos el nombre de la base de datos a crear.
                if (string.IsNullOrWhiteSpace(tableName)) 
                { 
                    throw new InvalidOperationException("Debe ingresar un nombre para la base de datos, especifíquelo en el archivo de texto");
                }
                var result = new DropTable().Execute(tableName); //Pasamos dicho nombre.
                return (result, string.Empty);
            }
            if (sentence.StartsWith("CREATE INDEX"))
            {
                // Se parsea la instrucción completa con el objetivo de obtener la información deseada para crear el índice.
                // Ajuste de Split para evitar problemas con los delimitadores

                // Se agregó esto para evitar dobles espacios en blanco en la entrada, daba errores para captar el tipo de árbol.
                sentence = Regex.Replace(sentence, @"\s+", " ");
                
                // Realizar el split asegurando que los delimitadores se mantienen correctos
                var parts = sentence.Split(new[] { "CREATE INDEX ", " ON ", "(", ")", " OF TYPE ", ";" }, StringSplitOptions.RemoveEmptyEntries);
            
                if (parts.Length != 4)
                {
                    throw new Exception("Error al parsear la instrucción CREATE INDEX. La sintaxis es incorrecta.");
                }
            
                string indexName = parts[0].Trim(); // Aquí se obtiene el nombre del índice
                string tableName = parts[1].Trim(); // Aquí se obtiene el nombre de la tabla
                string columnNameKeyValue = parts[2].Trim(); // Aquí se obtiene la columna clave (debería ser 'ID o primaryKey')
                string indexType = parts[3].Trim(); // Aquí se obtiene el tipo de índice (BTREE o BST)
            
                //Se pasan todos los datos para verificar si la tabla, columnas etc... existen.
                var result = new CreateIndexes().Execute(indexName, tableName, columnNameKeyValue, indexType);
                return (result, string.Empty); //Se devuelve el resultado de la operación.
            }
            if (sentence.StartsWith("UPDATE"))
            {
                // Dividir la sentencia en las partes relevantes
                var parts = sentence.Split(new[] { "UPDATE ", " SET ", " WHERE " }, StringSplitOptions.RemoveEmptyEntries);

                // Asegurarse de que SET esté presente
                if (parts.Length < 2 || parts.Length > 3)
                {
                    throw new Exception("Error al parsear la sentencia UPDATE. La sintaxis es incorrecta.");
                }

                // Extraer el nombre de la tabla
                string tableName = parts[0].Trim();

                // Dividir la cláusula SET en columna y valor
                var setClause = parts[1].Split(new[] { " = " }, StringSplitOptions.RemoveEmptyEntries);
                if (setClause.Length != 2)
                {
                    throw new Exception("Error al parsear la cláusula SET.");
                }
                string columnToUpdate = setClause[0].Trim();
                string newValue = setClause[1].Trim().Trim('"');  // Eliminar comillas alrededor del valor

                // Verificar si hay cláusula WHERE
                string? whereColumn = null;
                string? whereValue = null;
                string? operatorValue = null;

                if (parts.Length == 3)  // Hay WHERE, si hay 3 "operandos" quiere decir que el WHERE va incluido.
                {
                    // Detectar el operador en la cláusula WHERE
                    string[] operators = new[] { "==", "!=", ">", "<", ">=", "<=" };
                    operatorValue = operators.FirstOrDefault(op => parts[2].Contains(op));

                    if (operatorValue == null)
                    {
                        throw new Exception("Operador no soportado en la cláusula WHERE.");
                    }

                    // Dividir la cláusula WHERE usando el operador detectado
                    var whereClause = parts[2].Split(new[] { operatorValue }, StringSplitOptions.RemoveEmptyEntries);
                    if (whereClause.Length != 2)
                    {
                        throw new Exception("Error al parsear la cláusula WHERE.");
                    }

                    whereColumn = whereClause[0].Trim();
                    whereValue = whereClause[1].Trim();
                }

                Console.WriteLine($"Tabla: {tableName}, Columna a actualizar: {columnToUpdate}, Nuevo valor: {newValue}");
                if (whereColumn != null)
                {
                    Console.WriteLine($"Columna WHERE: {whereColumn}, Valor WHERE: {whereValue}, Operador: {operatorValue}");
                }
                else
                {
                    Console.WriteLine("Actualizando toda la columna, no se especificó WHERE.");
                }

                // Llamar a la operación Update con el operador incluido
                var result = new Update().Execute(tableName, columnToUpdate, newValue, whereColumn, whereValue, operatorValue);
                return (result, string.Empty);
            }
            // Delete implementacion...
            if (sentence.StartsWith("DELETE"))
            {
                const string deleteKeyword = "DELETE FROM";
                string? whereClause = null;
                string? columnName = null;
                string? conditionValue = null;

                var tableToDeleteFrom = sentence.Substring(deleteKeyword.Length).Trim();

                // Comprobar si hay una cláusula WHERE
                if (tableToDeleteFrom.Contains("WHERE"))
                {
                    
                    var parts = tableToDeleteFrom.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 2)
                    {
                        throw new InvalidOperationException("La consulta DELETE no tiene una cláusula WHERE válida.");
                    }
                    
                    tableToDeleteFrom = parts[0].Trim();  // Nombre de la tabla
                    whereClause = parts[1].Trim();  // La cláusula WHERE

                    // Procesar la cláusula WHERE
                    var whereParts = whereClause.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    if (whereParts.Length == 2)
                    {
                        columnName = whereParts[0].Trim();  // Nombre de la columna
                        conditionValue = whereParts[1].Trim();  // Valor de la condición
                    }
                    else
                    {
                        throw new InvalidOperationException("Formato inválido en la cláusula WHERE.");
                    }
                }

                if (string.IsNullOrWhiteSpace(tableToDeleteFrom))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una tabla para eliminar.");
                }

                // Ejecutamos la operación DELETE
                var result = new Delete().Execute(tableToDeleteFrom, columnName, conditionValue);
                return result;
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}