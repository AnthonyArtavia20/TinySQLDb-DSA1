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
            var estructuraEsperada = Regex.Match(sentence, @"INSERT INTO (\w+) \((.*?)\) VALUES \((.*?)\)"); //Forma esperada de la operación
            if (!estructuraEsperada.Success) //Si la forma no es la esperada, entonces se rechaza.
            {
                throw new InvalidOperationException("Formato de INSERT inválido");
            }

            //Definimos que serán los datos ingresantes, tales serán como nombre de la tabla, columnas y valores.
            string tableName = estructuraEsperada.Groups[1].Value;
            string[] columnas = estructuraEsperada.Groups[2].Value.Split(',').Select(c => c.Trim()).ToArray(); //Para ambas operaciones les quitamos las comas
            string[] valores = estructuraEsperada.Groups[3].Value.Split(',').Select(v => v.Trim()).ToArray();//y los espacios en blanco, además los convertimos en array para poder almacenar varios datos.

            if (columnas.Length != valores.Length)
            {
                throw new InvalidOperationException("El número de columnas no coincide con el número de valores");
            }

            /*En el requerimiento 010 se pide lo siguiente: "Las columnas DateTime se especifican en String 
            pero internamente se parsean a DateTime"
            Por lo tanto lo siguiente convierte las strings de tipo DateTime a ticks:*/
            for (int i = 0; i < columnas.Length; i++)
            {
                valores[i] = valores[i].Trim('\'', '"'); // Le quitamos las comillas a los datos para que no estorben a la hora de procesarlos.

                //Dividimos los datos entrantes en tipo "Fecha/date" y los procesamos aqi+i
                if (columnas[i].ToLower().Contains("fecha") || columnas[i].ToLower().Contains("date"))
                {
                    string format = "yyyy-MM-dd HH:mm:ss"; //Formato esperado determinado por las instrucciones.
                    Console.WriteLine($"Intentando convertir: {valores[i]} en el formato {format}"); //Esto es para debug, daba problemas y no sabía que era :/
                    if (DateTime.TryParseExact(valores[i], format, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime date))
                    {
                        valores[i] = date.Ticks.ToString(); //Incertamos en la columna esperada que es en la posición que se indicó en la operación pues es la longitud de la lista de columnas.
                    }
                    else
                    {
                        throw new InvalidOperationException($"Formato de fecha inválido: {valores[i]}");
                    }
                }//En el caso que el tipo de dato sea "salario" lo procesamos aquí:
                else if (columnas[i].ToLower().Contains("salario") || columnas[i].ToLower() == "double")
                {
                    if (double.TryParse(valores[i], NumberStyles.Any, CultureInfo.InvariantCulture, out double result))
                    {
                        valores[i] = result.ToString(CultureInfo.InvariantCulture); //Igual, se añade como string a la columna de valores en el lugar provisto por su posición en la lista.
                    }
                    else
                    {
                        throw new InvalidOperationException($"Formato de número inválido: {valores[i]}");
                    }
                }
            }

            return Store.GetInstance().InsertIntoTable(tableName, columnas, valores); //Se retorna la tabla con los valores ordenados y parseados.
        }
    }
}