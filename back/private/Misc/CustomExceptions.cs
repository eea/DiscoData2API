using System;

namespace DiscoData2API_Priv.Misc
{
	[Serializable]
	public class SQLFormattingException : Exception
	{
		public SQLFormattingException()	{}

		public SQLFormattingException(string message): base(message) {}
	}


    [Serializable]
    public class ViewNotFoundException : Exception
    {
        public ViewNotFoundException() { }

        public ViewNotFoundException(string message) : base(message) { }
    }

}