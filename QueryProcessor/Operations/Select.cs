using Entities;
using StoreDataManager;
using System.Text.Json;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public (OperationStatus Status, string Data) Execute() //Se modificó este método para que sea capáz
        {//de poder recibir no solo la respuesta de la creación de la solcitiud,si no tambien la Data
            //como en el caso del Selectt, que necesita enviar el contenido del archivo binario.
            var result = Store.GetInstance().Select();
            return result;
        }
    }
}