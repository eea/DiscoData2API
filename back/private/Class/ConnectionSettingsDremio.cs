namespace DiscoData2API_Priv.Class
{
    public class ConnectionSettingsDremio
    {
        public string? Username { get; set; }
        public string? Password { get; set; }
        public string? DremioServer { get; set; }
        public string? DremioServerAuth { get; set; }
        public int Limit { get; set; }
        public int Timeout { get; set; }
    }

}
