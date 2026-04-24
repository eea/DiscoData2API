using System.Text.Json.Serialization;

namespace DiscoData2API.Model
{
    public class DremioSchema
    {
        public string Schema { get; set; } = null!;
        public string SchemaName { get; set; } = null!;
    }

    public class DremioRawSchema
    {
        public string TABLE_SCHEMA { get; set; } = null!;
        public string SchemaName { get; set; } = null!;
    }
}
