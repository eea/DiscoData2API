namespace DiscoData2API.Model;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

public class MongoPublicDocument
{
      public string Id { get; set; } = null!;
      public string Name { get; set; } = null!;
      public string Description { get; set; } = null!;
      public Boolean IsActive { get; set; }
      public string? Version { get; set; }
      public string? UserAdded { get; set; } = null!;
      public DateTime? Date { get; set; }
      private string Query { get; set; } = null!;
      public List<Field>? Fields { get; set; } = null!;

      public MongoPublicDocument(MongoDocument doc)
      {
            Id = doc.Id;
            Name = doc.Name;
            UserAdded = doc.UserAdded;
            Description = doc.Description;
            IsActive = doc.IsActive;
            Version = doc.Version;
            Date = doc.Date;
            Query = doc.Query;
            Fields = doc.Fields;
      }


}