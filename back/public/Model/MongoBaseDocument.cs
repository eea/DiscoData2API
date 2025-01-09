namespace DiscoData2API.Model
{
    public class MongoBaseDocument
    {
       public string Name { get; set; }
        public string Description { get; set; }
        public string Query { get; set; } 
        public string? UserAdded { get; set; }
        public string? Version { get; set; }
    }
 }