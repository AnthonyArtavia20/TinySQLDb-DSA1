using Entities;
using StoreDataManager;
using System.Text.RegularExpressions;
using System.Collections.Generic;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string createTableStatement)
        {
            //Ahora también podemos recibir las propiedades a solicitadas para crear las tablas
            var (tableName, columns) = ParseCreateTableStatement(createTableStatement);
            return Store.GetInstance().CreateTable(tableName, columns);
        }

        private (string tableName, List<ColumnDefinition> columns) ParseCreateTableStatement(string statement)
        {
            var tableNameMatch = Regex.Match(statement, @"CREATE TABLE (\w+)");
            if (!tableNameMatch.Success)
            {
                throw new ArgumentException("Mal formato CREATE TABLE, ingresa corectamente los datos: Usar(Integer, Varchar(especificar el tamaño), DATETIME, y DOUBLE)");
            }
            string tableName = tableNameMatch.Groups[1].Value;

            var columnDefinitions = new List<ColumnDefinition>();
            var columnMatches = Regex.Matches(statement, @"(\w+)\s+(INTEGER|DOUBLE|VARCHAR\(\d+\)|DATETIME)(?:\s+(NOT NULL))?(?:\s+(PRIMARY KEY))?"); //Como debería de ser la entrada.
            
            foreach (Match match in columnMatches)
            {
                //Comenzamos a guardar los valores ingresados.
                string columnName = match.Groups[1].Value;
                string dataType = match.Groups[2].Value;
                bool isNullable = !match.Groups[3].Success;
                bool isPrimaryKey = match.Groups[4].Success;

                int? varcharLength = null;
                if (dataType.StartsWith("VARCHAR")) //En caso de que se ocupe VARCHAR necesitamos extraer su longitud
                {
                    varcharLength = int.Parse(Regex.Match(dataType, @"\d+").Value);
                }

                columnDefinitions.Add(new ColumnDefinition
                {
                    Name = columnName,
                    DataType = dataType,
                    IsNullable = isNullable,
                    IsPrimaryKey = isPrimaryKey,
                    VarcharLength = varcharLength
                });
            }

            return (tableName, columnDefinitions);
        }
    }
}