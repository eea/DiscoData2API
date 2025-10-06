namespace DiscoData2API_Priv.Model
{
    public class MongoBaseDocument
    {
        public required string Name { get; set; }
        public string? Catalog { get; set; }
        public required string Description { get; set; }
        public required string Query { get; set; }
        public string? UserAdded { get; set; }
        public string? Version { get; set; }
        public List<ViewParameter>? Parameters { get; set; }
    }
}
