using System.Xml.Linq;

namespace DiscoData2API.Class
{
    public class ConnectionSettingsMongo
    {
        private string  _connstring= string.Empty;
        public string ConnectionString
        {

            get
            {
                // custom logic when reading
                return System.Environment.GetEnvironmentVariable("MONGODB_CONNSTRING") ?? _connstring;
            }
            set
            {
                // custom logic when setting
                Console.WriteLine($"Setting Name to {value}");
                if (!string.IsNullOrWhiteSpace(value))
                {
                    _connstring = value;
                }
                else
                {
                    throw new ArgumentException("MongoDB ConnectionString cannot be empty");
                }
            }
        }


        public string DatabaseName { get; set; } = null!;
        public string CollectionName { get; set; } = null!;
    }
}
