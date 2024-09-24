namespace Entities
{
    //Se agregó este ENUM para poder especificar las propiedades que van a tener las tablas
    //además se creó aquí para aprovechar que el namespace Entities se usa en los lugares donde ColumDefinition se usa.
    public class ColumnDefinition
    {
        //Tipos de datos esperados al momento de recibir una tabla.
        public string? Name { get; set; }
        public string? DataType { get; set; }
        public bool IsNullable { get; set; }
        public bool IsPrimaryKey { get; set; }
        public int? VarcharLength { get; set; }
    }
}
