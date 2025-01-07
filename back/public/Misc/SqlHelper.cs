namespace DiscoData2API.Misc
{
    public static class SqlHelper
    {
        public static bool IsSafeSql(string sql)
        {
            var blacklist = new[] { ";", "--", "/*", "*/", "xp_", "sp_", "EXEC", "DROP", "INSERT", "DELETE", "ALTER", "CREATE" };
            return !blacklist.Any(keyword => sql.IndexOf(keyword, StringComparison.OrdinalIgnoreCase) >= 0);
        }
    }
}