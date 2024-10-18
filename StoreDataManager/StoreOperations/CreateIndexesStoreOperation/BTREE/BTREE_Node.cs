namespace StoreDataManager.StoreOperations
{
    public class BTreeNode
    {
        public int[] Keys; // Arreglo de claves del nodo
        public long[] Positions; // Arreglo de posiciones de los registros
        public BTreeNode[] Children; // Arreglo de hijos del nodo
        public int NumKeys; // Número de claves actuales en el nodo
        public bool IsLeaf; // Indica si el nodo es una hoja (no tiene hijos)

        // Constructor para inicializar un nodo B
        public BTreeNode(int t, bool isLeaf)
        {
            this.IsLeaf = isLeaf; // Se asigna si es hoja
            this.Keys = new int[2 * t - 1]; // Tamaño del arreglo de claves
            this.Positions = new long[2 * t - 1]; // Tamaño del arreglo de posiciones
            this.Children = new BTreeNode[2 * t]; // Tamaño del arreglo de hijos
            this.NumKeys = 0; // Inicialmente, no hay claves
        }

        // Método para insertar una clave en un nodo que no está lleno
        public void InsertNonFull(int key, long position)
        {
            int i = NumKeys - 1; // Posición para insertar

            // Si el nodo es hoja, se insertan claves directamente
            if (IsLeaf)
            {
                // Movemos las claves para hacer espacio
                while (i >= 0 && Keys[i] > key)
                {
                    Keys[i + 1] = Keys[i]; // Desplazamos las claves hacia la derecha
                    Positions[i + 1] = Positions[i]; // Desplazamos las posiciones
                    i--; // Decrementamos el índice
                }
                Keys[i + 1] = key; // Insertamos la nueva clave
                Positions[i + 1] = position; // Insertamos la posición
                NumKeys++; // Incrementamos el contador de claves
            }
            else
            {
                // Si no es hoja, encontramos el hijo adecuado donde insertar
                while (i >= 0 && Keys[i] > key)
                {
                    i--; // Buscamos el hijo adecuado
                }
                i++; // Avanzamos al hijo correspondiente

                // Si el hijo está lleno, lo dividimos
                if (Children[i].NumKeys == 2 * (Keys.Length + 1) - 1)
                {
                    SplitChild(i); // Dividimos el hijo
                    // Si la nueva clave es mayor que la clave promovida, avanzamos
                    if (Keys[i] < key)
                    {
                        i++;
                    }
                }
                Children[i].InsertNonFull(key, position); // Insertamos en el hijo correspondiente
            }
        }

        // Método para dividir un hijo que está lleno
        public void SplitChild(int i)
        {
            BTreeNode z = new BTreeNode(Keys.Length / 2, Children[i].IsLeaf); // Nuevo nodo
            BTreeNode y = Children[i]; // Nodo hijo a dividir
            z.NumKeys = (Keys.Length / 2) - 1; // Se calcula el número de claves en el nuevo nodo

            // Movemos las claves y posiciones del nodo a dividir al nuevo nodo
            for (int j = 0; j < z.NumKeys; j++)
            {
                z.Keys[j] = y.Keys[j + (Keys.Length / 2)];
                z.Positions[j] = y.Positions[j + (Keys.Length / 2)];
            }

            // Si no es hoja, movemos los hijos al nuevo nodo
            if (!y.IsLeaf)
            {
                for (int j = 0; j < z.NumKeys + 1; j++)
                {
                    z.Children[j] = y.Children[j + (Keys.Length / 2)];
                }
            }

            y.NumKeys = (Keys.Length / 2) - 1; // Ajustamos el número de claves del nodo original

            // Desplazamos los hijos y claves del nodo padre
            for (int j = NumKeys; j >= i + 1; j--)
            {
                Children[j + 1] = Children[j];
            }
            Children[i + 1] = z; // Insertamos el nuevo nodo como hijo

            // Desplazamos las claves en el nodo padre
            for (int j = NumKeys - 1; j >= i; j--)
            {
                Keys[j + 1] = Keys[j];
                Positions[j + 1] = Positions[j];
            }
            Keys[i] = y.Keys[(Keys.Length / 2) - 1]; // Promovemos la clave al nodo padre
            Positions[i] = y.Positions[(Keys.Length / 2) - 1]; // Promovemos la posición
            NumKeys++; // Incrementamos el número de claves en el nodo padre
        }

        // Método para buscar una clave en el nodo
        public BTreeNode? Search(int key)
        {
            int i = 0;
            // Buscamos la posición de la clave
            while (i < NumKeys && key > Keys[i])
            {
                i++; // Avanzamos hasta encontrar la clave o llegar al final
            }

            // Si encontramos la clave, la retornamos
            if (i < NumKeys && Keys[i] == key)
            {
                return this; // Retorna el nodo actual
            }

            // Si es hoja, la clave no está presente
            if (IsLeaf)
            {
                return null; // Retorna null si no se encontró
            }

            // Buscamos recursivamente en el hijo correspondiente
            return Children[i]?.Search(key);
        }
    }
}
