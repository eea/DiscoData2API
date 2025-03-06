using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace DiscoData2API_Priv.Model
{
    [BsonIgnoreExtraElements]
    public class DatashareUrl
    {
        [BsonElement("url")]
        public string Url { get; set; }

        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("is_eionet")]
        public bool IsEionet { get; set; } = false;

    }

    [BsonIgnoreExtraElements]
    public class DatasetStatus
    {
        /*
        [BsonElement("date")]
        public DateTime? date { get; set; }
        */

        [BsonElement("datetime")]
        public Object? DateTime { get; set; }
        
        [BsonElement("value")]
        public string? value { get; set; }
    }


    [BsonIgnoreExtraElements]
    public class DatasetFilter
    {
        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("table_name")]
        public string Table_Name { get; set; }

        [BsonElement("field")]
        public string Field { get; set; }

        [BsonElement("order")]
        public int Order { get; set; }
    }



    [BsonIgnoreExtraElements]
    public class DatasetTableField
    {
        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("type")]
        public string Type { get; set; }
    }



    [BsonIgnoreExtraElements]
    public class DatasetTable
    {
        [BsonElement("name")]
        public string Name { get; set; }

        [BsonElement("num_rows")]
        public int NumRows { get; set; }

        
        [BsonElement("dremio_route")]
        public string DremioRoute { get; set; }

        
        [BsonElement("s3_path")]
        public string S3Path { get; set; }

        
        [BsonElement("fields")]
        public List<DatasetTableField>? Fields { get; set; } = null!;
        
        [BsonElement("crs")]
        public string? crs { get; set; }


        [BsonElement("filter")]
        public string Filter { get; set; }
        

    }
    


    [BsonIgnoreExtraElements]
    public class MongoDatasetDocument
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string _id { get; set; } = null!;

        [BsonElement("id")]
        public string ID { get; set; }


        [BsonElement("name")]
        public string Name { get; set; } = null!;

        [BsonElement("format")]
        public List<string>? format { get; set; } = null!;

        [BsonElement("description")]
        public string Description { get; set; } = null!;

        [BsonElement("datashare_url")]
        public DatashareUrl Datashare_url { get; set; } = null!;


        [BsonElement("s3_path")]
        public string S3_Path { get; set; } = null!;


        [BsonElement("dremio_route")]
        public string Dremio_Route { get; set; } = null!;


        [BsonElement("parent")]
        public string Parent { get; set; } = null!;


        [BsonElement("tables")]
        public List<DatasetTable>? Tables { get; set; } = null;


        [BsonElement("serie_id")]
        public string Serie_id { get; set; } = null!;

        [BsonElement("status")]
        public List<DatasetStatus>? Status { get; set; } = null!;

        [BsonElement("filters")]
        public List<DatasetFilter>? Filters { get; set; } = null!;



    }
}
