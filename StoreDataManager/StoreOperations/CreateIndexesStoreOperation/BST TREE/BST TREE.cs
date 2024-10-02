namespace StoreDataManager.StoreOperations
{
    public class BinarySearchTree //Clase especializada para poder crear el índice de tipo BST
    {
        BSTNode? actual; //Es el punto de entrada o el primer nodo a partir del cual se realizan todas las 
        //operaciones de inserción, búsqueda y eliminación.

        public BinarySearchTree() //Constructor que inicializa la raíz en null.
        {
            actual = null;
        }

        public BSTNode? Search(int key)  //Permite realizar búsquedas solo por el valor a buscar
        {//dicho valor a buscar será la PrimaryKey de las tablas.
            return SearchRecursive(actual, key);
        }

        private BSTNode? SearchRecursive(BSTNode? node, int key) //Método recursivo que busca dependiendo
        { //en si el valor a buscar es menor o igual a la raíz actual.
            if (node == null || node.Key == key)
            {
                return node;  // Retorna el nodo que contiene la posición.
            }
        
            if (key < node.Key)
            {
                return SearchRecursive(node.Left, key); //Llamamos recursivamente por el lado izquierdo
            }
            else
            {
                return SearchRecursive(node.Right, key); //Llamamos recursivamente por el lado derecho
            }
        }

        public void Insert(int key, long position) //Operación que permite agregar valores al árbol binario.
        {
            Console.WriteLine($"Insertando clave: {key}, posición: {position}");
            actual = InsertRecursive(actual, key, position); //Actualizamos el valor de la raíz en base al
            //valor insertado.
        }
    
        private BSTNode InsertRecursive(BSTNode? node, int key, long position) //Operación de manera recursiva.
        {
            if (node == null) 
            {
                Console.WriteLine($"Nodo creado: clave: {key}, posición: {position}");
                return new BSTNode(key, position);
            }

            if (key < node.Key)
            {
                Console.WriteLine($"Valores a insertar menores a la raiz, navegando a la izquierda (clave actual: {node.Key})");
                node.Left = InsertRecursive(node.Left, key, position); //Inserta por el lado izquierdo
            }
            else if (key > node.Key)
            {
                Console.WriteLine($"Valores a insertar mayores a la raiz, navegando a la derecha (clave actual: {node.Key})");
                node.Right = InsertRecursive(node.Right, key, position);//Inserta por el lado derecho
            }
            
            return node; //Devuelve el nodo actual (ya sea la raíz o un subárbol) después de haber hecho la inserción
        }

            public void Delete(int key) //Este método permite eliminar los registros del árbol binario.
            {
                actual = DeleteRecursive(actual, key);
            }

            private BSTNode? DeleteRecursive(BSTNode? actual, int key)//Se cubren los 3 casos primordiales
            {   /*El nodo a eliminar es una hoja. Elimina la hoja nada más(caso easy)
                El nodo a eliminar tiene un solo hijo. Elimina el nodo y conecta el hijo(caso medium)
                El nodo a eliminar tiene dos hijos Encuentra el valor mínimo en el subárbol 
                derecho(sucesor en orden) para reemplazar el nodo que estás eliminando(caso Hard.)*/
                
                if (actual == null)
                    return actual;

                if (key < actual.Key)
                    actual.Left = DeleteRecursive(actual.Left, key);
                else if (key > actual.Key)
                    actual.Right = DeleteRecursive(actual.Right, key);
                else {
                    if (actual.Left == null)
                        return actual.Right;
                    else if (actual.Right == null)
                        return actual.Left;

                    actual.Key = MinValue(actual.Right);
                    actual.Right = DeleteRecursive(actual.Right, actual.Key);
                }
                return actual;
            }

            private int MinValue(BSTNode? actual) 
            {//Este método se utiliza en el proceso de eliminación para encontrar el nodo con el valor más pequeño 
            //en un subárbol (sucesor en orden) cuando el nodo a eliminar tiene dos hijos.
            
                if (actual == null)
                {
                    throw new ArgumentNullException(nameof(actual), "El nodo no puede ser null.");
                }

                int minValue = actual.Key;
                while (actual.Left != null) {
                    minValue = actual.Left.Key;
                    actual = actual.Left;
                }
                return minValue;
            }
    }
}