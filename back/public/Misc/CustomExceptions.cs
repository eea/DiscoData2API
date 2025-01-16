using System;

namespace DiscoData2API.Misc
{
    [Serializable]
    public class SQLFormattingException : Exception
    {
        public SQLFormattingException() { }

        public SQLFormattingException(string message) : base(message) { }
    }
}