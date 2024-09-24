using Entities;
using StoreDataManager;
using System.Text.RegularExpressions;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string createTableStatement)
        {
            var (tableName, columns) = ParseCreateTableStatement(createTableStatement); //Pasamos la operación completa y la Parseamos para 
            //poder obtener las columnas que se enviaron con todo y mensaje.
            return Store.GetInstance().CreateTable(tableName, columns); //Luego enviamos el nombre de la tabla y las columnas incluidas
            //a Store.cs para poder procesarlas.
        }

        private (string tableName, List<ColumnDefinition> columns) ParseCreateTableStatement(string statement) //Ayuda a separar de toda la 
        {//operación solo las columnas y el nombre de la tabla a crear.

            var tableNameMatch = Regex.Match(statement, @"CREATE TABLE (\w+)"); //Como empieza la operación.
            if (!tableNameMatch.Success) //En el caso de que no se escriba correctamente el comando
            {
                throw new ArgumentException("Mal formato CREATE TABLE, escriba bien CREATE TABLE y/o, ingresa corectamente los datos: Usar(Integer, Varchar(especificar el tamaño), DATETIME, y DOUBLE)");
            }
            string tableName = tableNameMatch.Groups[1].Value; //Si calzó el comando, obtenemos el nombre de la tabla.

            var columnDefinitions = new List<ColumnDefinition>(); //Creamos una lista donde almacenar las columnas.
            var columnMatches = Regex.Matches(statement, @"(\w+)\s+(INTEGER|DOUBLE|VARCHAR\(\d+\)|DATETIME)(?:\s+(NOT NULL))?(?:\s+(PRIMARY KEY))?"); //Como debería de ser la entrada.
            
            foreach (Match match in columnMatches) //Vamos buscando cuales calzan con los valores esperados.
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

                columnDefinitions.Add(new ColumnDefinition //Guardamos la estructura a crear de la tabla.
                {
                    Name = columnName,
                    DataType = dataType,
                    IsNullable = isNullable,
                    IsPrimaryKey = isPrimaryKey,
                    VarcharLength = varcharLength
                });
            }

            return (tableName, columnDefinitions); //Retornamos tanto el nombre de la tabla como sus columnas separadas y parseadas.
        }
    }
}