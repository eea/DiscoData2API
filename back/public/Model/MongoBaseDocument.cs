namespace DiscoData2API.Model
{
    public interface MongoBaseDocument
    {
        string _id { get; set; }
        string Name { get; set; }
        Boolean IsActive { get; set; }
        string Query { get; set; } 
    }

 }
