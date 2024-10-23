using Entities;
using System.Text;

namespace StoreDataManager.StoreOperations
{
    public class SelectOperation
    {
        private readonly string RutaDeterminadaPorSet;

        public SelectOperation(string RutaDeterminadaPorSet)
        {
            this.RutaDeterminadaPorSet = RutaDeterminadaPorSet;
        }

        public (OperationStatus Status, string Data) Select(string? NombreDeTableASeleccionar, 
                                                            List<string>? columnasSeleccionadas = null, 
                                                            string? columnName = null, 
                                                            string? conditionValue = null, 
                                                            string? operatorValue = "==")
        {
            string tableName = NombreDeTableASeleccionar + ".Table";
            string fullPath = Path.Combine(RutaDeterminadaPorSet, tableName);

            if (!File.Exists(fullPath))
            {
                Console.WriteLine($"Error: The table file '{fullPath}' does not exist.");
                return (OperationStatus.Error, $"Error: La tabla '{NombreDeTableASeleccionar}' no existe.");
            }

            StringBuilder resultBuilder = new StringBuilder();

            try
            {
                using (FileStream stream = File.Open(fullPath, FileMode.Open))
                using (BinaryReader reader = new BinaryReader(stream))
                {
                    string startMarker = reader.ReadString();
                    if (startMarker != "TINYSQLSTART")
                    {
                        throw new InvalidDataException("Formato de archivo inválido.");
                    }

                    int columnCount = reader.ReadInt32();
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

                    string endStructureMarker = reader.ReadString();
                    if (endStructureMarker != "ENDSTRUCTURE")
                    {
                        throw new InvalidDataException("Estructura del archivo inválida");
                    }

                    List<ColumnDefinition> columnasASeleccionar;
                    if (columnasSeleccionadas == null || columnasSeleccionadas.Count == 0)
                    {
                        columnasASeleccionar = columns;
                    }
                    else
                    {
                        columnasASeleccionar = columns.Where(c => columnasSeleccionadas.Contains(c.Name)).ToList();

                        if (columnasASeleccionar.Count == 0)
                        {
                            return (OperationStatus.Error, "Error: Ninguna de las columnas seleccionadas existe en la tabla.");
                        }
                    }

                    resultBuilder.AppendLine(string.Join(",", columnasASeleccionar.Select(c => c.Name)));

                    string dataStartMarker = reader.ReadString();
                    if (dataStartMarker != "DATASTART")
                    {
                        throw new InvalidDataException("Marca donde comienza la información, no encontrada");
                    }
                    int columnIndex = -1;
                    if (!string.IsNullOrEmpty(columnName))
                    {
                        columnIndex = columns.FindIndex(c => c.Name == columnName);
                        if (columnIndex == -1)
                        {
                            return (OperationStatus.Error, $"Error: La columna '{columnName}' no existe en la tabla '{NombreDeTableASeleccionar}'.");
                        }
                    }
                    bool hasData = false;

                    while (stream.Position < stream.Length)
                    {
                        StringBuilder ConstructorFila = new StringBuilder();
                        string[] rowData = new string[columnCount];

                        for (int i = 0; i < columnCount; i++)
                        {
                            if (columnasASeleccionar.Any(c => c.Name == columns[i].Name))
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
                            else
                            {
                                SkipValue(reader, columns[i]);
                            }
                        }

                        bool conditionMet = true;
                        if (!string.IsNullOrEmpty(columnName) && !string.IsNullOrEmpty(conditionValue))
                        {
                            conditionMet = EvaluateCondition(columns[columnIndex].DataType, rowData[columnIndex], conditionValue, operatorValue.ToLower());
                        }

                        if (conditionMet)
                        {
                            resultBuilder.AppendLine(string.Join(",", rowData));
                            hasData = true;
                        }
                    }

                    if (!hasData)
                    {
                        return (OperationStatus.Success, string.IsNullOrEmpty(columnName) ? "La tabla está vacía." : "No se encontraron datos que coincidan con la condición.");
                    }
                    return (OperationStatus.Success, resultBuilder.ToString());
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading file: {ex.Message}");
                return (OperationStatus.Error, $"Error: {ex.Message}");
            }
        }

        private bool EvaluateCondition(string dataType, string columnValue, string conditionValue, string operatorValue)
        {
            switch (dataType)
            {
                case "INTEGER":
                    int intColumnValue = int.Parse(columnValue);
                    int intConditionValue = int.Parse(conditionValue);
                    return CompareValues(intColumnValue, intConditionValue, operatorValue);

                case "DOUBLE":
                    double doubleColumnValue = double.Parse(columnValue);
                    double doubleConditionValue = double.Parse(conditionValue);
                    return CompareValues(doubleColumnValue, doubleConditionValue, operatorValue);

                case "DATETIME":
                    DateTime dateTimeColumnValue = DateTime.Parse(columnValue);
                    DateTime dateTimeConditionValue = DateTime.Parse(conditionValue);
                    return CompareValues(dateTimeColumnValue, dateTimeConditionValue, operatorValue);
                default:
                    if (operatorValue == "LIKE")
                    {
                        return columnValue.Contains(conditionValue.Replace("*", ""), StringComparison.OrdinalIgnoreCase);
                    }
                    return CompareValues(columnValue, conditionValue, operatorValue);
            }
        }

        private bool CompareValues<T>(T columnValue, T conditionValue, string operatorValue) where T : IComparable
        {
            switch (operatorValue)
            {
                case "==": return columnValue.CompareTo(conditionValue) == 0;
                case "not": return columnValue.CompareTo(conditionValue) != 0;
                case "<": return columnValue.CompareTo(conditionValue) < 0;
                case ">": return columnValue.CompareTo(conditionValue) > 0;
                case "<=": return columnValue.CompareTo(conditionValue) <= 0;
                case ">=": return columnValue.CompareTo(conditionValue) >= 0;
                default: throw new InvalidOperationException($"Operador no soportado: {operatorValue}");
            }
        }

        private void SkipValue(BinaryReader reader, ColumnDefinition column)
        {
            switch (column.DataType)
            {
                case "INTEGER":
                    reader.ReadInt32();
                    break;
                case "DOUBLE":
                    reader.ReadDouble();
                    break;
                case "DATETIME":
                    reader.ReadInt64();
                    break;
                default:
                    int length = reader.ReadInt32();
                    reader.ReadChars(length);
                    break;
            }
        }
    }
}
