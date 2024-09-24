//Clase encargada de insertar datos ingresados por el usario en las tablas que están creadas.
using System.Globalization;
using Entities;
using StoreDataManager;
using System.Text.RegularExpressions;

namespace QueryProcessor.Operations
{
    internal class Insert
    {
        public OperationStatus Execute(string sentence)
        {
            var estructuraEsperada = Regex.Match(sentence, @"INSERT INTO (\w+) \((.*?)\) VALUES \((.*?)\)");// Estructura esperada de la sentencia INSERT INTO
            if (!estructuraEsperada.Success) //En caso de que no se haya escrito bien el nombre del comando.
            {
                throw new InvalidOperationException("Formato de INSERT inválido, escriba bien INSERT INTO");
            }
        
            string tableName = estructuraEsperada.Groups[1].Value; //Almacenamos el nombre de la tabla.
            string[] columnas = estructuraEsperada.Groups[2].Value.Split(',').Select(c => c.Trim()).ToArray(); //Almacenamos las columnas
            string[] valores = estructuraEsperada.Groups[3].Value.Split(',').Select(v => v.Trim()).ToArray(); //Almacenamos los valores de dichas columnas.
        
            if (columnas.Length != valores.Length) //Si se metió un valor de más.
            {
                throw new InvalidOperationException("El número de columnas no coincide con el número de valores");
            }
        
            // Obtiene el ID de la tabla desde SystemTables, esto para poder saber en que Tabla insertar.
            int tableId = Store.GetInstance().GetTableId(tableName);

            Console.WriteLine($"Obtenido tableId: {tableId} para la tabla {tableName}"); //Debug
            if (tableId == -1)
            {
                throw new InvalidOperationException($"La tabla '{tableName}' no existe.");
            }
        
            // Obtiene el esquema de la tabla desde SystemColumns usando el ID de la tabla
            var tableSchema = Store.GetInstance().GetTableSchema(tableId); //Esto para poder validar los datos ingresantes y el esquema guardado en SystemCatalog al momento de crear la tabla.
        
            //Debug: Ver el esquema guardado:
            Console.WriteLine("Esquema de la tabla obtenido:");
            foreach (var column in tableSchema)
            {
                Console.WriteLine($"Nombre: {column.Name}, Tipo: {column.DataType}");
            }
        
            // Validación de tipos de datos y parseo
            for (int i = 0; i < columnas.Length; i++)
            {
                var columna = tableSchema.FirstOrDefault(col => col.Name == columnas[i]);
                
                if (columna == null)
                {
                    Console.WriteLine($"Error: La columna '{columnas[i]}' no existe en la tabla '{tableName}'");
                    throw new InvalidOperationException($"La columna '{columnas[i]}' no existe en la tabla '{tableName}'");
                }
        
                valores[i] = ParseAndValidateValue(columna, valores[i]).ToString();
            }
        
            // Llama a Store para insertar los datos
            return Store.GetInstance().InsertIntoTable(tableName, columnas, valores);
        }

        private object ParseAndValidateValue(ColumnDefinition columna, string value) //Permite validar y parsear las filas a insertar.
        {
            //Checkear para tipo VARCHAR con una longtid en específico.
            value = value.Trim();
            if (columna.DataType.StartsWith("VARCHAR", StringComparison.OrdinalIgnoreCase))
            {
                // Extract the length from VARCHAR(X)
                var match = Regex.Match(columna.DataType, @"VARCHAR\((\d+)\)");
                if (match.Success && int.TryParse(match.Groups[1].Value, out int maxLength))
                {
                    //Acá se verifica que el valor no sea más grande que el largo de las columnas.
                    if (value.Length > maxLength)
                    {
                        throw new InvalidOperationException($"El valor para la columna '{columna.Name}' excede la longitud máxima de {maxLength} caracteres.");
                    }
        
                    //Retornamos el valor ya que si es válido para VARCHAR
                    return value;
                }
                else
                {
                    throw new InvalidOperationException($"Tipo de dato no soportado: {columna.DataType}");
                }
            }
        
            // Revisamos el resto de tipos (INTEGER, DOUBLE, DATETIME)
            switch (columna.DataType.ToUpper())
            {
                case "INTEGER":
                    if (int.TryParse(value, out int intValue))
                        return intValue;
                    throw new InvalidOperationException($"El valor '{value}' para la columna '{columna.Name}' no es un entero válido.");
        
                case "DOUBLE":
                    if (double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out double doubleValue))
                        return doubleValue;
                    throw new InvalidOperationException($"El valor '{value}' para la columna '{columna.Name}' no es un número decimal válido.");
        
                case "DATETIME":
                {
                    string format = "yyyy-MM-dd HH:mm:ss";

                    // Con esto se eliminas las comillas simples que llevan los datos al momento de la instrucción.
                    value = value.Trim('\'');

                    // Parseamos la fecha.
                    if (DateTime.TryParseExact(value, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
                    {
                        return date.Ticks; //Se retorna la fecha como ticks (long)
                    }
                    throw new InvalidOperationException($"Formato de fecha inválido: {value}");
                }

                default:
                    throw new InvalidOperationException($"Tipo de dato no soportado: {columna.DataType}");
            }
        }

    }
}