using System;

namespace brqueue
{
    public class ResponseException : Exception
    {
        public ResponseException(string message) : base(message)
        {
        }

        public ResponseException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}