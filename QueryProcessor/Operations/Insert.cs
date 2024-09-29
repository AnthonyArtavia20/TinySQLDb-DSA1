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
            int tableId = GetTableId(tableName);

            Console.WriteLine($"Obtenido tableId: {tableId} para la tabla {tableName}"); //Debug
            if (tableId == -1)
            {
                throw new InvalidOperationException($"La tabla '{tableName}' no existe.");
            }
        
            // Obtiene el esquema de la tabla desde SystemColumns usando el ID de la tabla
            var tableSchema = GetTableSchema(tableId); //Esto para poder validar los datos ingresantes y el esquema guardado en SystemCatalog al momento de crear la tabla.
        
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

        public List<ColumnDefinition> GetTableSchema(int tableId) //Permite obtener la estructura de una tabla para poder compararlo luego.
        {
            var columns = new List<ColumnDefinition>(); //Se crea una lista con la definición de las columnas previamente definidas en el ENUM.
            string systemColumnsFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemColumns.columns"); //Definimos la ruta hacía el documento en SystemCatalog.
            
            using (var reader = new BinaryReader(File.Open(systemColumnsFilePath, FileMode.Open))) { //Abrimos el documento y para poder leer.
                while (reader.BaseStream.Position != reader.BaseStream.Length) { //Mientras no lleguemos al final vamos leyendo.

                    long currentFilePointer = reader.BaseStream.Position; // Depuración: imprimir el puntero actual del archivo
                    Console.WriteLine($"Puntero de archivo antes de leer: {currentFilePointer}");
            
                    int columnTableId = reader.ReadInt32();
                    Console.WriteLine($"Leyendo columna para la tabla con ID {columnTableId}");
            
                    if (columnTableId == tableId) {
                        var column = new ColumnDefinition();
                        column.Name = reader.ReadString();
                        column.DataType = reader.ReadString();
                        column.IsNullable = reader.ReadBoolean();
                        column.IsPrimaryKey = reader.ReadBoolean();
                        column.VarcharLength = reader.ReadInt32();
            
                        Console.WriteLine($"Columna encontrada: {column.Name}, Tipo: {column.DataType}");
                        columns.Add(column);
                    } else {
                        // Si no coincide, seguimos avanzando en el archivo.
                        // Leer los datos de la columna para avanzar el puntero correctamente
                        reader.ReadString(); // Name
                        reader.ReadString(); // DataType
                        reader.ReadBoolean(); // IsNullable
                        reader.ReadBoolean(); // IsPrimaryKey
                        reader.ReadInt32(); // VarcharLength
                    }
            
                    long newFilePointer = reader.BaseStream.Position; // Depuración: imprimir el puntero después de leer
                    Console.WriteLine($"Puntero de archivo después de leer: {newFilePointer}");
                }
            }
            
            if (columns.Count == 0) {
                Console.WriteLine($"Error: No se encontró ninguna columna para la tabla con ID {tableId}.");
            }
            
            return columns;
        }

        public static int GetTableId(string tableName) //Este método es super útil, ya que permite obtener el ID de la tabla de la cual se le pasa un nombre.
        { //Su uso es implementado en la clase Insert.cs, como es necesario extraer el esquema de las columnas para verificar antes de insertar, entonces este método se encarga de
            //obtener el ID de la tabla que sea actual, gracias a comparar su nombre con alguno de los existentes en la extración de ReadFromSystemTables en SystemCatalog.
            var tables = ReadFromSystemTables();
            var table = tables.FirstOrDefault(t => t.TableName == tableName);
            return table != default ? table.TableId : -1;
        }

        private static List<(int DbId, int TableId, string TableName)> ReadFromSystemTables() { //Genera la lista de tablas para luego compararlas.
            string systemTablesFilePath = Path.Combine(Entities.ConfigPaths.SystemCatalogPath, "SystemTables.tables"); //Obtenemos la ruta del documento que contiene las tablas.
            
            if (!File.Exists(systemTablesFilePath)) {// Verificar si el archivo existe antes de intentar leer
                Console.WriteLine("Error: El archivo SystemTables.tables no existe.");
                return new List<(int, int, string)>();
            }
        
            var tableList = new List<(int, int, string)>(); //Estructura del almacén que contendrá todas las tablas, es una lista.
            using (var reader = new BinaryReader(File.Open(systemTablesFilePath, FileMode.Open))) {//Abrimos el archivo y comenzamos a agregar todas las tablas que estén ahí.
                while (reader.BaseStream.Position != reader.BaseStream.Length) {//Esto hasta que se llegue al máximo de la longitud)(largo) del archivo.
                    int dbId = reader.ReadInt32();
                    int tableId = reader.ReadInt32();
                    string tableName = reader.ReadString();
                    tableList.Add((dbId, tableId, tableName));
                }
            }
            return tableList; //Lista preparada para ser comparada.
        }
    }
}