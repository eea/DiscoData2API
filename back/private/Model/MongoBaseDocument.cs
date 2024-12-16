namespace DiscoData2API_Priv.Model
{
    public interface MongoBaseDocument
    {
        string _id { get; set; }
        string Name { get; set; }
        Boolean IsActive { get; set; }
        string Query { get; set; }
    }
}