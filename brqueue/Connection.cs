using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace brqueue
{
    internal class Connection : IDisposable
    {
        private readonly TcpClient client;
        private readonly NetworkStream stream;
        private readonly SemaphoreSlim mutex = new SemaphoreSlim(1);
        
        public Connection(TcpClient client, NetworkStream stream)
        {
            this.client = client;
            this.stream = stream;
        }

        public void Dispose()
        {
            client?.Dispose();
            stream?.Dispose();
        }

        public async Task<int> ReadAsync(byte[] bytes)
        {
            return await stream.ReadAsync(bytes, 0, bytes.Length);
        }

        public async Task WriteAsync(byte[] bytes)
        {
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }
        
        
        public async Task Lock()
        {
            await mutex.WaitAsync();
        }

        public void Unlock()
        {
            mutex.Release();
        }
    }
}