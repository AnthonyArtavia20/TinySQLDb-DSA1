using Entities; //Con la finalidad de usar los archivos de configuración

namespace StoreDataManager.StoreOperations
{
    public class UpdateOperation
    {
        private readonly string RutaDeterminadaPorSet;
    
        public UpdateOperation(string RutaDeterminadaPorSet) //Constructor de la clase
        {
            this.RutaDeterminadaPorSet = RutaDeterminadaPorSet;
        }

        private bool CompareValues<T>(T columnValue, T conditionValue, string operatorValue) where T : IComparable 
        {//Método genérico para poder comparar los distintos comparadores
            switch (operatorValue)
            {
                case "==": return columnValue.CompareTo(conditionValue) == 0;
                case "!=": return columnValue.CompareTo(conditionValue) != 0;
                case "<": return columnValue.CompareTo(conditionValue) < 0;
                case ">": return columnValue.CompareTo(conditionValue) > 0;
                case "<=": return columnValue.CompareTo(conditionValue) <= 0;
                case ">=": return columnValue.CompareTo(conditionValue) >= 0;
                default: throw new InvalidOperationException($"Operador no soportado: {operatorValue}");
            }
        }

        public OperationStatus Execute(string tableName, string columnToUpdate, string newValue, string whereColumn, string whereValue, string operatorValue)
        {
            string tableFilePath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table");

            if (!File.Exists(tableFilePath)) //Se comprueba que la tabla sobre la que se quiere actualizar, exista.
            {
                Console.WriteLine($"Error: La tabla '{tableName}' no existe.");
                return OperationStatus.Error;
            }

            try
            {
                List<ColumnDefinition> columns = new List<ColumnDefinition>();
                List<List<object>> rows = new List<List<object>>();
                int columnToUpdateIndex = -1;
                int whereColumnIndex = -1;

                // Leer la estructura de la tabla y los datos
                using (FileStream stream = File.Open(tableFilePath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    // Verificar la marca de inicio
                    string startMarker = reader.ReadString();
                    if (startMarker != "TINYSQLSTART")
                    {
                        throw new InvalidDataException("Formato de archivo inválido.");
                    }

                    // Leer la estructura de la tabla
                    int columnCount = reader.ReadInt32();
                    for (int i = 0; i < columnCount; i++)
                    {
                        var column = new ColumnDefinition
                        {
                            Name = reader.ReadString(),
                            DataType = reader.ReadString(),
                            IsNullable = reader.ReadBoolean(),
                            IsPrimaryKey = reader.ReadBoolean(),
                            VarcharLength = reader.ReadInt32()
                        };
                        columns.Add(column);

                        if (column.Name == columnToUpdate)
                            columnToUpdateIndex = i;
                        if (column.Name == whereColumn)
                            whereColumnIndex = i;
                    }

                    // Verificar la marca de fin de estructura
                    string endStructureMarker = reader.ReadString();
                    if (endStructureMarker != "ENDSTRUCTURE")
                    {
                        throw new InvalidDataException("Invalid file structure"); //En caso de que la tabla tenga un formato de encabezado inválido
                    }

                    // Buscar el inicio de los datos
                    string dataStartMarker = reader.ReadString();
                    if (dataStartMarker != "DATASTART")
                    {
                        throw new InvalidDataException("Marca donde comienza la información no encontrada");
                    }

                    // Leer los datos
                    while (stream.Position < stream.Length)
                    {
                        List<object> row = new List<object>();
                        foreach (var column in columns)
                        {
                            object value = ReadValue(reader, column.DataType, column.VarcharLength ?? 0);
                            row.Add(value);
                        }
                        rows.Add(row);
                    }
                }

                // Actualizar los datos
                int updatedCount = 0;
                for (int i = 0; i < rows.Count; i++)
                {
                    bool HayWhere = true;  // Si no hay WHERE, se actualizan todas las filas

                    if (operatorValue != null)  // Hay una condición WHERE, si es diferente de null.
                    {
                        //Se le otorga un valor diferente 
                        HayWhere = CompareValues((IComparable)rows[i][whereColumnIndex], (IComparable)ConvertToType(whereValue, columns[whereColumnIndex].DataType), operatorValue);
                    }

                    if (HayWhere)
                    {
                        rows[i][columnToUpdateIndex] = ConvertToType(newValue, columns[columnToUpdateIndex].DataType);
                        updatedCount++;
                    }
                }

                // Escribir los datos actualizados de vuelta al archivo
                using (FileStream stream = File.Open(tableFilePath, FileMode.Create))
                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    // Escribir la marca de inicio
                    writer.Write("TINYSQLSTART");

                    // Escribir la estructura de la tabla
                    writer.Write(columns.Count);
                    foreach (var column in columns)
                    {
                        writer.Write(column.Name);
                        writer.Write(column.DataType);
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);
                        writer.Write(column.VarcharLength ?? 0);
                    }

                    // Escribir la marca de fin de estructura
                    writer.Write("ENDSTRUCTURE");

                    // Escribir la marca de inicio de datos
                    writer.Write("DATASTART");

                    // Escribir los datos actualizados
                    foreach (var row in rows)
                    {
                        for (int i = 0; i < columns.Count; i++)
                        {
                            WriteValue(writer, row[i], columns[i].DataType, columns[i].VarcharLength ?? 0);
                        }
                    }
                }

                Console.WriteLine($"Actualización completada. {updatedCount} filas actualizadas en la tabla '{tableName}'");
                return OperationStatus.Success;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error durante la actualización: {ex.Message}");
                return OperationStatus.Error;
            }
        }

        private object ConvertToType(string value, string dataType)
        {
            switch (dataType)
            {
                case "INTEGER":
                    return int.Parse(value);
                case "DOUBLE":
                    return double.Parse(value);
                case "DATETIME":
                    return DateTime.Parse(value);
                default: // VARCHAR
                    return value;
            }
        }

        //Objetivo: Leer los valores actuales del documento/archivo.
        private object ReadValue(BinaryReader reader, string dataType, int varcharLength)
        {
            switch (dataType)
            {
                case "INTEGER":
                    return reader.ReadInt32();
                case "DOUBLE":
                    return reader.ReadDouble();
                case "DATETIME":
                    return new DateTime(reader.ReadInt64());
                default: // VARCHAR
                    int length = reader.ReadInt32();
                    return new string(reader.ReadChars(length)).TrimEnd();
            }
        }

        //Permite escribir los valores en el archivo.
        private void WriteValue(BinaryWriter writer, object value, string dataType, int varcharLength)
        {
            switch (dataType)
            {
                case "INTEGER":
                    writer.Write(Convert.ToInt32(value));
                    break;
                case "DOUBLE":
                    writer.Write(Convert.ToDouble(value));
                    break;
                case "DATETIME":
                    writer.Write(((DateTime)value).Ticks);
                    break;
                default: // VARCHAR
                    string strValue = value.ToString();
                    if (strValue.Length > varcharLength)
                    {
                        strValue = strValue.Substring(0, varcharLength);
                    }
                    writer.Write(strValue.Length);
                    writer.Write(strValue.ToCharArray());
                    break;
            }
        }

        public OperationStatus ExecuteAtPosition(string tableName, string columnToUpdate, string newValue, long position)
        {
            string filePath = Path.Combine(RutaDeterminadaPorSet, $"{tableName}.Table");
        
            if (!File.Exists(filePath))
            {
                return OperationStatus.Error;
            }
        
            using (var stream = File.Open(filePath, FileMode.Open, FileAccess.ReadWrite))
            using (var writer = new BinaryWriter(stream))
            {
                stream.Seek(position, SeekOrigin.Begin);
        
                // Escribir el nuevo valor en la posición especificada
                writer.Write(newValue); // Ajustar según el tipo de dato
        
                return OperationStatus.Success;
            }
        }

    }
}