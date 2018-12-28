using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Protobuf;

namespace brqueue
{
    /// <summary>
    ///     A raw wrapper around the TCP connection to the brqueue server.
    ///     You probably want to add another layer, so you aren't dealing with raw byte arrays.
    ///     This client is safe to concurrent usage.
    /// </summary>
    public class Client : IDisposable
    {
        /// <summary>
        ///     A set of callbacks to be invoked whenever responses are available
        /// </summary>
        private readonly IDictionary<int, Channel<ResponseWrapper>> _callbacks =
            new ConcurrentDictionary<int, Channel<ResponseWrapper>>();

        private readonly ConnectionPool _pool;


        /// <summary>
        ///     Autoincremented id for referring between requests when multiplexing
        ///     the same client
        /// </summary>
        private int _nextRefId = 1;

        /// <summary>
        ///     Creates a new BRQueue client and connects to the specified server.
        ///     If the constructor returns successfully, the connection has been created
        ///     and is ready for use.
        /// </summary>
        /// <param name="hostname">The hostname of the server to connect to, e.g. "localhost"</param>
        /// <param name="port">The port of the brqueue server</param>
        public Client(string hostname, int port = 6431)
        {
            _pool = new ConnectionPool(12, () => new TcpClient(hostname, port));
        }


        public void Dispose()
        {
            _pool.Dispose();
        }

        /// <summary>
        ///     Generates a new ref id for calling the server
        /// </summary>
        /// <returns></returns>
        private int GetNextRefId()
        {
            return Interlocked.Increment(ref _nextRefId);
        }

        /// <summary>
        ///     Converts a byte array to the int representation
        ///     The byte array has to be in LittleEndian format
        /// </summary>
        private static int ByteArrayToInt(byte[] bytes)
        {
            if (!BitConverter.IsLittleEndian)
                Array.Reverse(bytes);

            return BitConverter.ToInt32(bytes, 0);
        }

        /// <summary>
        ///     Converts an int to a byte array in LittleEndian format
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        private static byte[] IntToByteArray(int i)
        {
            var bytes = BitConverter.GetBytes(i);

            if (!BitConverter.IsLittleEndian)
                Array.Reverse(bytes);

            return bytes;
        }

        /// <summary>
        ///     Waits for the next message on the underlying socket and returns the raw bytes of it
        /// </summary>
        private async Task<ResponseWrapper> ReadNextMessage(Connection connection)
        {
            // Get the size of the next message
            var bytes = new byte[4];
            await connection.Lock();
            try
            {
                await connection.ReadAsync(bytes);
                var size = ByteArrayToInt(bytes);

                // Actually read the message
                bytes = new byte[size];
                await connection.ReadAsync(bytes);
            }
            finally
            {
                connection.Unlock();
            }

            return ParseResponseMessage(bytes);
        }

        /// <summary>
        ///     Sends a protobuf message over the underlying stream
        /// </summary>
        /// <param name="message">The message to send</param>
        /// <returns></returns>
        /// <exception cref="Exception">Throws an exception if the byte conversion didn't work as expected</exception>
        private async Task<int> SendMessage(RequestWrapper message, Connection connection)
        {
            // Generate a reference id for the message, so we can catch the response 
            // later on
            var refId = GetNextRefId();
            message.RefId = refId;

            // Convert the message into bytes we can send across the wire
            var bytes = message.ToByteArray();
            var size = bytes.Length;
            var sizeBytes = IntToByteArray(size);
            if (sizeBytes.Length != 4)
                throw new Exception("Expected an int to be converted into 4 bytes. That was not the case.");

            // Gather the message into one big message, so the message can be send in one go, without something disturbing us
            var all = new byte[size + 4];
            Buffer.BlockCopy(sizeBytes, 0, all, 0, sizeBytes.Length);
            Buffer.BlockCopy(bytes, 0, all, sizeBytes.Length, bytes.Length);

            // Actually send the message
            await connection.WriteAsync(all);

            return refId;
        }

        /// <summary>
        ///     Parses the raw bytes into a protobuf message
        /// </summary>
        private static ResponseWrapper ParseResponseMessage(byte[] bytes)
        {
            return ResponseWrapper.Parser.ParseFrom(bytes);
        }

        /// <summary>
        ///     Takes care of possible response error
        ///     Will throw an exception if the response is an error, so we can follow normal c#
        ///     error handling.
        /// </summary>
        private static void HandlePossibleError(ResponseWrapper response)
        {
            if (response.MessageCase != ResponseWrapper.MessageOneofCase.Error) return;
            var error = response.Error;
            throw new ResponseException("Request error", new Exception(error.Message));
        }

        /// <summary>
        ///     Ensures the response is of a certain type, just in case something goes haywire on the wire
        /// </summary>
        private static void RequireResponseType(ResponseWrapper response, ResponseWrapper.MessageOneofCase type)
        {
            if (response.MessageCase != type)
                throw new ResponseException($"Invalid response type, expected {type}, got {response.MessageCase}");
        }

        private Channel<ResponseWrapper> CreateResponseChannel()
        {
            return Channel.CreateBounded<ResponseWrapper>(new BoundedChannelOptions(1)
                {FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true});
        }

        private async Task<ResponseWrapper> WaitForResponse(int refId, Connection connection)
        {
            Channel<ResponseWrapper> res;
            lock (_callbacks)
                if (!_callbacks.TryGetValue(refId, out res))
                {
                    res = CreateResponseChannel();
                    _callbacks.Add(refId, res);
                }

            var message = await ReadNextMessage(connection);

            if (message.RefId == refId)
            {
                lock (_callbacks) _callbacks.Remove(refId);
                return message;
            }

            Channel<ResponseWrapper> cha;
            lock (_callbacks)
                if (!_callbacks.TryGetValue(message.RefId, out cha))
                {
                    cha = CreateResponseChannel();
                    _callbacks.Add(message.RefId, cha);
                }

            await cha.Writer.WriteAsync(message);

            var actualMessage = await res.Reader.ReadAsync();
            lock (_callbacks) _callbacks.Remove(refId);
            return actualMessage;
        }

        /// <summary>
        ///     Utility method for executing a request and awaiting the response
        /// </summary>
        /// <param name="wrapper">The request to execute</param>
        /// <param name="requiredResponseType">
        ///     The expected response type. If the response is not of this type,
        ///     an exception will be thrown.
        /// </param>
        /// <returns>The valid response</returns>
        private async Task<ResponseWrapper> ExecuteRequest(RequestWrapper wrapper,
            ResponseWrapper.MessageOneofCase requiredResponseType)
        {
            var connection = _pool.Get();

            var refId = await SendMessage(wrapper, connection);

            // Wait for a response
            var responseWrapper = await WaitForResponse(refId, connection);
            
            // Make sure the response is valid
            HandlePossibleError(responseWrapper);
            RequireResponseType(responseWrapper, requiredResponseType);

            return responseWrapper;
        }

        /// <summary>
        ///     Wraps an async execution, so it can be used in a purely sync context
        /// </summary>
        /// <param name="task">The task to execute async</param>
        /// <returns>The result of the task</returns>
        private T ExecuteAsyncSync<T>(Task<T> task)
        {
            task.Start();
            task.Wait();
            return task.Result;
        }

        /// <summary>
        ///     Enqueues a new task to be handled
        /// </summary>
        /// <param name="message">
        ///     The message to enqueue, this is probable some sort of serialized blob of
        ///     whatever task format you prefer
        /// </param>
        /// <param name="priority">The priority of the task</param>
        /// <param name="requiredCapabilities">The capabilities required to complete the task</param>
        /// <returns>A guid of the created task</returns>
        public async Task<Guid> EnqueueRequestAsync(byte[] message, Priority priority,
            IEnumerable<string> requiredCapabilities)
        {
            var request = new EnqueueRequest {Message = ByteString.CopyFrom(message), Priority = priority};
            request.RequiredCapabilities.AddRange(requiredCapabilities);
            var wrapper = new RequestWrapper {Enqueue = request};

            var responseWrapper = await ExecuteRequest(wrapper, ResponseWrapper.MessageOneofCase.Enqueue);

            var response = responseWrapper.Enqueue;
            return Guid.Parse(response.Id);
        }

        /// <summary>
        ///     Enqueues a new task to be handled
        /// </summary>
        /// <param name="message">
        ///     The message to enqueue, this is probable some sort of serialized blob of
        ///     whatever task format you prefer
        /// </param>
        /// <param name="priority">The priority of the task</param>
        /// <param name="requiredCapabilities">The capabilities required to complete the task</param>
        /// <returns>A guid of the created task</returns>
        public Guid EnqueueRequest(byte[] message, Priority priority, IEnumerable<string> requiredCapabilities)
        {
            return ExecuteAsyncSync(EnqueueRequestAsync(message, priority, requiredCapabilities));
        }

        /// <summary>
        ///     Pops a single message of the queue.
        ///     When the message has been handled successfully, Acknowledge should be called
        /// </summary>
        /// <param name="availableCapabilities">The capabilities this server has available</param>
        /// <param name="waitForMessages">If the method should wait for a response to be available before returning</param>
        /// <returns>A task that should be worked on</returns>
        public async Task<WorkTask> PopAsync(List<string> availableCapabilities, bool waitForMessages)
        {
            var request = new PopRequest {WaitForMessage = waitForMessages};
            request.AvailableCapabilities.AddRange(availableCapabilities);
            var wrapper = new RequestWrapper {Pop = request};

            var responseWrapper = await ExecuteRequest(wrapper, ResponseWrapper.MessageOneofCase.Pop);

            var response = responseWrapper.Pop;
            var workTask = new WorkTask(Guid.Parse(response.Id), response.Message.ToByteArray(), this);

            return workTask;
        }

        /// <summary>
        ///     Pops a single message of the queue.
        ///     When the message has been handled successfully, Acknowledge should be called
        /// </summary>
        /// <param name="availableCapabilities">The capabilities this server has available</param>
        /// <param name="waitForMessage">If the method should wait for a response to be available before returning</param>
        /// <returns>A task that should be worked on</returns>
        public WorkTask Pop(List<string> availableCapabilities, bool waitForMessage)
        {
            return ExecuteAsyncSync(PopAsync(availableCapabilities, waitForMessage));
        }

        /// <summary>
        ///     Marks a task as finished
        /// </summary>
        /// <returns>The task to mark completed</returns>
        public async Task<Guid> AcknowledgeAsync(WorkTask task)
        {
            var request = new AcknowledgeRequest {Id = task.Id.ToString()};
            var wrapper = new RequestWrapper {Acknowledge = request};

            await ExecuteRequest(wrapper, ResponseWrapper.MessageOneofCase.Acknowledge);

            return task.Id;
        }

        /// <summary>
        ///     Marks a task as finished
        /// </summary>
        /// <returns>The task to mark completed</returns>
        public Guid Acknowledge(WorkTask task)
        {
            return ExecuteAsyncSync(AcknowledgeAsync(task));
        }
    }
}