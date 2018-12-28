using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;

namespace brqueue
{
    internal class ConnectionPool : IDisposable
    {
        private readonly Connection[] _connections;
        private int _nextIndex;

        public ConnectionPool(int count, Func<TcpClient> factory)
        {
            Console.WriteLine("Creation new connection pool");
            _connections = new Connection[count];
            var tasks = new List<Thread>(count);
            for (var i = 0; i < count; i++)
            {
                var index = i;
                var thread = new Thread(() =>
                {
                    Console.WriteLine("Creating TCP connection");
                    var client = factory();
                    _connections[index] = new Connection(client, client.GetStream());
                });
                thread.Start();
                tasks.Add(thread);
            }

            foreach (var task in tasks)
            {
                Console.WriteLine("Waiting for thread to join");
                task.Join();
            }
        }

        /// <summary>
        /// Gets an available connection
        /// </summary>
        /// <returns></returns>
        public Connection Get()
        {
            var index = Interlocked.Increment(ref _nextIndex) % _connections.Length;
            return _connections[index];
        }

        public void Dispose()
        {
            foreach (var connection in _connections)
            {
                connection.Dispose();
            }
        }
    }
}