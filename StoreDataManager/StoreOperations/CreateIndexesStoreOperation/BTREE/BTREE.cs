namespace StoreDataManager.StoreOperations
{
    public class BTree : InterfaceIndexStructure
    {
        private BTreeNode? root; // Nodo raíz del árbol B
        private int t; // Grado mínimo (número mínimo de hijos)

        // Constructor del árbol B que inicializa el grado mínimo y establece la raíz como null
        public BTree(int t)
        {
            this.t = t;
            root = null;
        }

        // Método para insertar una clave y su posición
        public void Insert(int key, long position)
        {
            // Si el árbol está vacío, se crea un nuevo nodo raíz
            if (root == null)
            {
                root = new BTreeNode(t, true);
                root.Keys[0] = key; // Asignamos la clave al nodo raíz
                root.Positions[0] = position; // Asignamos la posición al nodo raíz
                root.NumKeys = 1; // Incrementamos el número de claves en el nodo raíz
            }
            else
            {
                // Si el nodo raíz está lleno, se necesita dividirlo
                if (root.NumKeys == 2 * t - 1)
                {
                    BTreeNode newNode = new BTreeNode(t, false); // Creamos un nuevo nodo
                    newNode.Children[0] = root; // Hacemos que el nuevo nodo sea hijo del antiguo nodo raíz
                    newNode.SplitChild(0); // Dividimos el antiguo nodo raíz
                    int i = 0; // Variable para encontrar la posición adecuada

                    // Determinamos en qué hijo seguir la inserción
                    if (newNode.Keys[0] < key)
                    {
                        i++;
                    }
                    newNode.Children[i].InsertNonFull(key, position); // Insertamos en el hijo adecuado
                    root = newNode; // Actualizamos la raíz del árbol B
                }
                else
                {
                    // Si la raíz no está llena, insertamos directamente en ella
                    root.InsertNonFull(key, position);
                }
            }
        }

        // Método para buscar una clave en el árbol
        public BTreeNode? Search(int key)
        {
            return root?.Search(key); // Llamamos al método de búsqueda del nodo raíz
        }
        
        public void InOrderTraversal()
        {
            if (root != null)
            {
                InOrderTraversalRecursive(root);
            }
        }

        private void InOrderTraversalRecursive(BTreeNode node)
        {
            int i;
            for (i = 0; i < node.NumKeys; i++)
            {
                if (!node.IsLeaf)
                {
                    InOrderTraversalRecursive(node.Children[i]);
                }
                Console.WriteLine($"Clave: {node.Keys[i]}, Posición: {node.Positions[i]}");
            }
            if (!node.IsLeaf)
            {
                InOrderTraversalRecursive(node.Children[i]);
            }
        }
    }
}
