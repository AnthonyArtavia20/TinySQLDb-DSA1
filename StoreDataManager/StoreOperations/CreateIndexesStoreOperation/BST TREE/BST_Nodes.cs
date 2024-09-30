namespace StoreDataManager.StoreOperations
{
    public class BSTNode
    {
        public int Key { get; set; }  // Primary Key (ID)
        public long Position { get; set; }  // Posición del registro en el archivo
        public BSTNode? Left { get; set; }
        public BSTNode? Right { get; set; }

        public BSTNode(int key, long position)
        {
            Key = key;
            Position = position;
            Left = null;
            Right = null;;
        }
    }
}