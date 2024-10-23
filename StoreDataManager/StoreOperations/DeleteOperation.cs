using Entities;

namespace StoreDataManager.StoreOperations
{
    public class DeleteOperation
    {
        private readonly string RutaDeterminadaPorSet;

        public DeleteOperation(string dutaDeterminadaPorSet)
        {
            this.RutaDeterminadaPorSet = dutaDeterminadaPorSet;
        }

        // Función genérica para comparar valores con el operador dado
        private bool CompareValues(IComparable columnValue, IComparable conditionValue, string operatorValue)
        {
            switch (operatorValue)
            {
                case "==":
                    return columnValue.CompareTo(conditionValue) == 0;
                case "!=":
                    return columnValue.CompareTo(conditionValue) != 0;
                case "<":
                    return columnValue.CompareTo(conditionValue) < 0;
                case "<=":
                    return columnValue.CompareTo(conditionValue) <= 0;
                case ">":
                    return columnValue.CompareTo(conditionValue) > 0;
                case ">=":
                    return columnValue.CompareTo(conditionValue) >= 0;
                default:
                    throw new InvalidOperationException("Operador desconocido.");
            }
        }

        public (OperationStatus Status, string Data) Execute(string tableName, string columnName, string conditionValue, string operatorValue)
        {
            // Genera la ruta de la tabla donde ejecutar los cambios
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName + ".Table");

            Console.WriteLine("La ruta completa que le llega a DeleteOperation es!!!!!!!: " + fullPath);
            if (!File.Exists(fullPath))
            {
                return (OperationStatus.Error, $"Error: La tabla '{tableName}' no existe.");
            }

            // Crea una tabla temporal para almacenar los datos (si es necesario)
            string tempFilePath = Path.Combine(RutaDeterminadaPorSet, tableName + "_temp.Table");

            try
            {
                // Abrir el archivo original y el archivo temporal
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                using (FileStream tempStream = File.Open(tempFilePath, FileMode.Create))
                using (BinaryWriter writer = new BinaryWriter(tempStream))
                {
                    // Leer encabezado del archivo original
                    string startMarker = reader.ReadString();
                    if (startMarker != "TINYSQLSTART")
                    {
                        throw new InvalidDataException("Formato de archivo inválido.");
                    }

                    int columnCount = reader.ReadInt32();
                    // Leer la estructura de la tabla
                    List<ColumnDefinition> columns = new List<ColumnDefinition>();

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
                    }

                    // Verificar la marca de fin de estructura
                    string endStructureMarker = reader.ReadString();
                    if (endStructureMarker != "ENDSTRUCTURE")
                    {
                        throw new InvalidDataException("Invalid file structure");
                    }

                    // Escribir encabezado al archivo temporal
                    writer.Write("TINYSQLSTART");
                    writer.Write(columnCount);
                    // Escribir la estructura de la tabla
                    foreach (var column in columns)
                    {
                        writer.Write(column.Name);
                        writer.Write(column.DataType);
                        writer.Write(column.IsNullable);
                        writer.Write(column.IsPrimaryKey);

                        if (column.VarcharLength.HasValue)
                        {
                            writer.Write(column.VarcharLength.Value);  // Escribir el valor si no es null
                        }
                        else
                        {
                            writer.Write(0);
                        }
                    }
                    writer.Write("ENDSTRUCTURE");

                    // Buscar el inicio de los datos
                    string dataStartMarker = reader.ReadString();
                    if (dataStartMarker != "DATASTART")
                    {
                        throw new InvalidDataException("Marca donde comienza la información no encontrada");
                    }

                    writer.Write("DATASTART");

                    // Si no se especifica una columna ni una condición, eliminar todos los registros
                    if (string.IsNullOrEmpty(columnName) && string.IsNullOrEmpty(conditionValue))
                    {
                        stream.Close();  // Cerrar los archivos para evitar errores al reemplazar
                        tempStream.Close();

                        // Reemplazamos el archivo original por el temporal (sin datos)
                        File.Delete(fullPath);
                        File.Move(tempFilePath, fullPath);

                        return (OperationStatus.Success, "Todos los registros fueron eliminados correctamente.");
                    }

                    // En caso de que exista una condición, procedemos a eliminar basados en esa condición
                    int columnIndex = columns.FindIndex(c => c.Name == columnName);
                    if (columnIndex == -1)
                    {
                        return (OperationStatus.Error, $"Error: La columna '{columnName}' no existe en la tabla '{tableName}'.");
                    }

                    // Leer los datos y omitir las filas que coincidan con la condición
                    while (stream.Position < stream.Length)
                    {
                        bool matchCondition = false;
                        long rowStartPosition = stream.Position;  // Guardamos la posición al inicio de la fila
                        string[] rowData = new string[columnCount];

                        for (int i = 0; i < columnCount; i++)
                        {
                            switch (columns[i].DataType)
                            {
                                case "INTEGER":
                                    rowData[i] = reader.ReadInt32().ToString();
                                    break;
                                case "DOUBLE":
                                    rowData[i] = reader.ReadDouble().ToString();
                                    break;
                                case "DATETIME":
                                    long ticks = reader.ReadInt64();
                                    DateTime dateTime = new DateTime(ticks);
                                    rowData[i] = dateTime.ToString("yyyy-MM-dd HH:mm:ss");
                                    break;
                                default:
                                    int length = reader.ReadInt32();
                                    rowData[i] = new string(reader.ReadChars(length));
                                    break;
                            }
                        }

                        // Convertir el valor de la columna y la condición al tipo adecuado
                        object columnValue = rowData[columnIndex];
                        object conditionParsedValue = conditionValue;
                        switch (columns[columnIndex].DataType)
                        {
                            case "INTEGER":
                                columnValue = int.Parse(rowData[columnIndex]);
                                conditionParsedValue = int.Parse(conditionValue);
                                break;
                            case "DOUBLE":
                                columnValue = double.Parse(rowData[columnIndex]);
                                conditionParsedValue = double.Parse(conditionValue);
                                break;
                            case "DATETIME":
                                columnValue = DateTime.Parse(rowData[columnIndex]);
                                conditionParsedValue = DateTime.Parse(conditionValue);
                                break;
                        }

                        // Verificamos si la fila cumple con la condición para eliminar
                        matchCondition = CompareValues((IComparable)columnValue, (IComparable)conditionParsedValue, operatorValue);

                        // Escribimos la fila al archivo temporal solo si no coincide con la condición
                        if (!matchCondition)
                        {
                            for (int i = 0; i < columnCount; i++)
                            {
                                switch (columns[i].DataType)
                                {
                                    case "INTEGER":
                                        writer.Write(int.Parse(rowData[i]));
                                        break;
                                    case "DOUBLE":
                                        writer.Write(double.Parse(rowData[i]));
                                        break;
                                    case "DATETIME":
                                        writer.Write(DateTime.Parse(rowData[i]).Ticks);
                                        break;
                                    default:
                                        writer.Write(rowData[i].Length);
                                        writer.Write(rowData[i].ToCharArray());
                                        break;
                                }
                            }
                        }
                    }
                }
                // Reemplazamos el archivo original por el temporal
                File.Delete(fullPath);
                File.Move(tempFilePath, fullPath);
                return (OperationStatus.Success, "Filas eliminadas correctamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during delete: {ex.Message}");
                return (OperationStatus.Error, ex.Message);
            }
        }
    }
}