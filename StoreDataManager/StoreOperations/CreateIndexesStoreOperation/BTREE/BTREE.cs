namespace StoreDataManager.StoreOperations
{
    public class BTree : InterfaceIndexStructure
    {
        private readonly int _t;  // Grado mínimo del árbol B
        private BTreeNode? actual;  // Nodo raíz

        public BTree(int t)  // Constructor para inicializar el BTree con un grado t
        {
            this._t = t;
            actual = null;
        }

        // Método para buscar una clave en el árbol B
        public BTreeNode? Search(int key)
        {
            if (actual == null) return null; // Si el árbol está vacío
            return SearchRecursive(actual, key); // Llamamos al método de búsqueda recursivo
        }

        // Método recursivo para buscar una clave
        private BTreeNode? SearchRecursive(BTreeNode node, int key)
        {
            int i = 0;
            // Buscamos la posición correcta dentro de las claves
            while (i < node.NumKeys && key > node.Keys[i])
            {
                i++;
            }

            // Si encontramos la clave, la devolvemos
            if (i < node.NumKeys && node.Keys[i] == key)
            {
                return node;
            }

            // Si el nodo es una hoja, la clave no está en el árbol
            if (node.IsLeaf)
            {
                return null;
            }

            // Si no es una hoja, seguimos buscando en el hijo correspondiente
            return SearchRecursive(node.Children[i], key);
        }

        // Método para insertar una clave en el árbol B
        public void Insert(int key, long position)
        {
            if (actual == null)
            {
                // Si el árbol está vacío, crea un nodo nuevo
                actual = new BTreeNode(_t, true);
                actual.Keys[0] = key;  // Inserta la primera clave
                actual.Positions[0] = position;  // Inserta la posición asociada
                actual.NumKeys = 1;  // Actualiza el número de claves
            }
            else
            {
                // Si la raíz está llena, dividimos
                if (actual.NumKeys == 2 * _t - 1)
                {
                    BTreeNode newRoot = new BTreeNode(_t, false);  // Nueva raíz
                    newRoot.Children[0] = actual;  // El nodo actual pasa a ser el primer hijo

                    SplitChild(newRoot, 0);  // Divide el hijo 0
                    int i = 0;
                    if (newRoot.Keys[0] < key)
                    {
                        i++;
                    }
                    InsertNonFull(newRoot.Children[i], key, position);  // Inserta en el nodo adecuado
                    actual = newRoot;  // Actualiza la raíz
                }
                else
                {
                    InsertNonFull(actual, key, position);  // Inserta directamente si no está lleno
                }
            }
        }

        // Método para insertar en un nodo que no está lleno
        private void InsertNonFull(BTreeNode node, int key, long position)
        {
            int i = node.NumKeys - 1;

            if (node.IsLeaf)
            {
                // Inserta en un nodo hoja
                while (i >= 0 && node.Keys[i] > key)
                {
                    node.Keys[i + 1] = node.Keys[i];
                    node.Positions[i + 1] = node.Positions[i];
                    i--;
                }
                node.Keys[i + 1] = key;
                node.Positions[i + 1] = position;
                node.NumKeys++;
            }
            else
            {
                // Encuentra el hijo adecuado donde insertar
                while (i >= 0 && node.Keys[i] > key)
                {
                    i--;
                }
                i++;
                if (node.Children[i].NumKeys == 2 * _t - 1)
                {
                    SplitChild(node, i);
                    if (node.Keys[i] < key)
                    {
                        i++;
                    }
                }
                InsertNonFull(node.Children[i], key, position);
            }
        }

        // Método para dividir un hijo lleno
        private void SplitChild(BTreeNode parent, int i)
        {
            BTreeNode y = parent.Children[i];
            BTreeNode z = new BTreeNode(_t, y.IsLeaf);
            z.NumKeys = _t - 1;

            // Mover las claves y posiciones a z
            for (int j = 0; j < _t - 1; j++)
            {
                z.Keys[j] = y.Keys[j + _t];
                z.Positions[j] = y.Positions[j + _t];
            }

            // Mover los hijos si no es hoja
            if (!y.IsLeaf)
            {
                for (int j = 0; j < _t; j++)
                {
                    z.Children[j] = y.Children[j + _t];
                }
            }

            y.NumKeys = _t - 1;

            // Mover los hijos de parent para hacer espacio
            for (int j = parent.NumKeys; j >= i + 1; j--)
            {
                parent.Children[j + 1] = parent.Children[j];
            }
            parent.Children[i + 1] = z;

            // Mover las claves de parent para hacer espacio
            for (int j = parent.NumKeys - 1; j >= i; j--)
            {
                parent.Keys[j + 1] = parent.Keys[j];
                parent.Positions[j + 1] = parent.Positions[j];
            }

            parent.Keys[i] = y.Keys[_t - 1];
            parent.Positions[i] = y.Positions[_t - 1];
            parent.NumKeys++;
        }

        // Método para eliminar una clave del árbol B
        public void Delete(int key)
        {
            if (actual == null)
            {
                Console.WriteLine("El árbol está vacío.");
                return;
            }

            DeleteRecursive(actual, key);

            // Si la raíz pierde todas sus claves, actualizamos la raíz
            if (actual.NumKeys == 0)
            {
                if (!actual.IsLeaf)
                {
                    actual = actual.Children[0];  // La raíz se convierte en el primer hijo
                }
                else
                {
                    actual = null;  // Si era una hoja, el árbol queda vacío
                }
            }
        }

        // Método recursivo para eliminar una clave
        private void DeleteRecursive(BTreeNode node, int key)
        {
            // Implementar el algoritmo de eliminación para BTree
            // ...
        }

        // Método de recorrido en orden (in-order) para verificar la estructura del árbol
        public void InOrderTraversal()
        {
            if (actual != null)
            {
                InOrderRecursive(actual);
            }
        }

        // Recursivamente recorre el árbol en orden
        private void InOrderRecursive(BTreeNode node)
        {
            int i;
            for (i = 0; i < node.NumKeys; i++)
            {
                if (!node.IsLeaf)
                {
                    InOrderRecursive(node.Children[i]);
                }
                Console.WriteLine($"Clave: {node.Keys[i]}, Posición: {node.Positions[i]}");
            }

            if (!node.IsLeaf)
            {
                InOrderRecursive(node.Children[i]);
            }
        }
    }
}

