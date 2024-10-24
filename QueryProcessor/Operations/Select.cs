using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public (OperationStatus Status, string Data) Execute(string TableNameToSelect,List<string> columnasSeleccionadas, string columnName = null!, string conditionValue = null!, string operatorValue = "==")
        {   //Se modificó este método para que sea capáz de poder recibir no solo la respuesta de la creación de la solicitud, si no tambien la
            //Data como en el caso del Select, que necesita enviar el contenido del archivo binario. -No eliminar este comentario-

            return Store.GetInstance().Select(TableNameToSelect, columnName, conditionValue, operatorValue,columnasSeleccionadas);
        }
    }
}
    
