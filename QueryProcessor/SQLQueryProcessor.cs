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
            if (sentence.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
            {
                // Separar la parte de SELECT y el resto de la consulta
                int fromIndex = sentence.IndexOf("FROM", StringComparison.OrdinalIgnoreCase);
                if (fromIndex == -1)
                {
                    throw new InvalidOperationException("Consulta SQL no válida. Falta la cláusula 'FROM'.");
                }

                // Extraer las columnas seleccionadas (lo que está entre SELECT y FROM)
                string columnsPart = sentence.Substring(6, fromIndex - 6).Trim();
                
                // Si se selecciona todo, significa que debemos seleccionar todas las columnas
                List<string>? columnasSeleccionadas = columnsPart == "*" ? null : columnsPart.Split(',').Select(c => c.Trim()).ToList();

                // Extraer el nombre de la tabla (lo que está después de FROM y antes de WHERE si existe)
                string afterFrom = sentence.Substring(fromIndex + 4).Trim();
                string? whereClause = null;
                string? columnName = null;
                string? conditionValue = null;
                string operatorValue = "==";  // Operador por defecto

                // Comprobar si hay una cláusula WHERE
                if (afterFrom.Contains("WHERE", StringComparison.OrdinalIgnoreCase))
                {
                    // Dividir la sentencia para obtener la tabla y la cláusula WHERE
                    var parts = afterFrom.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
                    afterFrom = parts[0].Trim(); // Nombre de la tabla
                    whereClause = parts[1].Trim(); // La cláusula WHERE

                    // Procesar la cláusula WHERE (asumimos que el formato es `columna operador valor`)
                    var whereParts = whereClause.Split(new[] { " ", "=", "<", ">", "!=", "<=", ">=" }, StringSplitOptions.RemoveEmptyEntries);


                    if (whereParts.Length >= 2)
                    {
                        columnName = whereParts[0].Trim();  // Nombre de la columna
                        conditionValue = whereParts[1].Trim();  // Valor de la condición

                        // Si hay un operador (e.g., <, >, <=, >=, !=)
                        if (whereClause.Contains("<=")) operatorValue = "<=";
                        else if (whereClause.Contains(">=")) operatorValue = ">=";
                        else if (whereClause.Contains("<")) operatorValue = "<";
                        else if (whereClause.Contains(">")) operatorValue = ">";
                        else if (whereClause.Contains("!=")) operatorValue = "!=";
                        else operatorValue = "=="; // Por defecto "igual"
                    }
                    else
                    {
                        throw new InvalidOperationException("Formato inválido en la cláusula WHERE.");
                    }
                }

                if (string.IsNullOrWhiteSpace(afterFrom))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de tabla para seleccionar.");
                }

                // Ejecutar la operación de selección con las columnas y la cláusula WHERE opcional
                var result = new Select().Execute(afterFrom,columnasSeleccionadas, columnName, conditionValue, operatorValue);
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
            if (sentence.StartsWith("DELETE FROM ", StringComparison.OrdinalIgnoreCase))
            {
                string tableToDeleteFrom = sentence.Substring("DELETE FROM ".Length).Trim();
                string columnName = string.Empty;
                string conditionValue = string.Empty;
                string operatorValue = string.Empty;
                string whereClause = string.Empty;

                // Comprobar si hay una cláusula WHERE
                if (tableToDeleteFrom.Contains("WHERE"))
                {
                    var parts = tableToDeleteFrom.Split(new[] { "WHERE" }, StringSplitOptions.RemoveEmptyEntries);
                    if (parts.Length < 2)
                    {
                        throw new InvalidOperationException("La consulta DELETE no tiene una cláusula WHERE válida.");
                    }

                    tableToDeleteFrom = parts[0].Trim(); // Nombre de la tabla
                    whereClause = parts[1].Trim(); // La cláusula WHERE

                    // Identificar el operador en la cláusula WHERE
                    string[] operators = { "==", "!=", "<=", ">=", "<", ">" };
                    string selectedOperator = operators.FirstOrDefault(op => whereClause.Contains(op));

                    if (!string.IsNullOrEmpty(selectedOperator))
                    {
                        var whereParts = whereClause.Split(new[] { selectedOperator }, StringSplitOptions.RemoveEmptyEntries);
                        if (whereParts.Length == 2)
                        {
                            columnName = whereParts[0].Trim(); // Nombre de la columna
                            conditionValue = whereParts[1].Trim(); // Valor de la condición

                            // Verificar si el valor de la condición es numérico o de texto
                            if (int.TryParse(conditionValue, out _))
                            {
                                // Es un valor numérico, no hacer más cambios
                            }
                            else
                            {
                                // Es un valor de texto, eliminar comillas
                                conditionValue = conditionValue.Replace("'", "").Trim();
                            }

                            operatorValue = selectedOperator; // Operador encontrado
                        }
                        else
                        {
                            throw new InvalidOperationException("Formato inválido en la cláusula WHERE.");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("Operador no válido en la cláusula WHERE.");
                    }
                }

                if (string.IsNullOrWhiteSpace(tableToDeleteFrom))
                {
                    throw new InvalidOperationException("Debe ingresar un nombre de una tabla para eliminar.");
                }

                // Ejecutamos la operación DELETE con los parámetros obtenidos
                var result = new Delete().Execute(tableToDeleteFrom, columnName, conditionValue, operatorValue);
                return result;
            }
            else
            {
                throw new UnknownSQLSentenceException();
            }
        }
    }
}