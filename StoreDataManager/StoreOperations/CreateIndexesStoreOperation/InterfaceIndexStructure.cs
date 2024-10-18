namespace StoreDataManager.StoreOperations
{

    //Se creó esta interfaz con la finalidad de reutilizar el método "ActualizarIndice" de la clase CreateIndexesStoreOperation
    //de esta forma se puede utilizar el mismo método para ambas estructuras(o más), 
    public interface InterfaceIndexStructure
    {
        void Insert(int key, long position); //Método que inserta los datos en las estructuras de datos correspondientes.
        void InOrderTraversal(); //Método para poder recorrer los árboles y observar si los datos en dichas estructuras están correctamente
        //insertados, o simplemente para poder observar cambios.
    }
}