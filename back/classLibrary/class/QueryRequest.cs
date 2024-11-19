namespace DiscoData2API_Library.Class;

using System.ComponentModel;

public class QueryRequest
{
    [DefaultValue(null)]
    public string[]? Fields { get; set; }
    [DefaultValue(null)]
    public string[]? Filters { get; set; }
    [DefaultValue(150)]
    public int? Limit { get; set; }

}
