using ApiInterface.Exceptions;
using ApiInterface.InternalModels;

namespace ApiInterface.Processors
{
    internal class ProcessorFactory //Aquí se procesan los tipos de solictudes que pueden entrar
    {
        internal static IProcessor Create(Request request)
        {
            if (request.RequestType is RequestType.SQLSentence) //Por el momento solo hay de tipo SQLSentnce.
            {
                return new SQLSentenceProcessor(request); //Se obtiene la información serealizada para ser procesada
            }
            throw new UnknowRequestTypeException();
        }
    }
}
