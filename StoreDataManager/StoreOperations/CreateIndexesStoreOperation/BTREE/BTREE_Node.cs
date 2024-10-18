namespace StoreDataManager.StoreOperations
{
    public class BTreeNode
    {
        public int[] Keys;  // Arreglo de claves del nodo
        public long[] Positions;  // Arreglo de posiciones de los registros
        public BTreeNode[] Children;  // Arreglo de hijos del nodo
        public int NumKeys;  // Número de claves actuales en el nodo
        public bool IsLeaf;  // Indica si el nodo es una hoja

        // Constructor para inicializar un nodo B
        public BTreeNode(int t, bool isLeaf)
        {
            this.IsLeaf = isLeaf;
            this.Keys = new int[2 * t - 1];  // Espacio para 2*t-1 claves
            this.Positions = new long[2 * t - 1];  // Espacio para las posiciones correspondientes
            this.Children = new BTreeNode[2 * t];  // Espacio para 2*t hijos
            this.NumKeys = 0;  // Inicialmente no hay claves
        }
    }
}
