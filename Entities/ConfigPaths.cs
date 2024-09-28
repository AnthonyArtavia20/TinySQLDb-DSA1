// ConfigPaths.cs, encargado de administrar las rutas estáticas en el proyecto
namespace Entities
{
    public static class ConfigPaths
    {
        // AAquí se definen rutas de manera global, útil para que todas las clases necesarias tengan acceso a ellas
        public static string SystemCatalogPath { get; } = @"C:\TinySql\Data\SystemCatalog";

        // Aquí se pueden seguir añadiendo rutas que pueden ser usadas de manera global
    }
}
