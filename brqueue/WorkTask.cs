using System;
using System.Threading.Tasks;

namespace brqueue
{
    /// <summary>
    /// A task that can be worked on
    /// </summary>
    public class WorkTask
    {
        /// <summary>
        /// An internal reference to the connected client this message was created from.
        /// Used for convenience methods
        /// </summary>
        private readonly Client _client;

        /// <summary>
        /// The id of the task
        /// </summary>
        public Guid Id { get; }
        /// <summary>
        /// The raw message that was passed along from the consumer
        /// </summary>
        public byte[] Message { get; }

        internal WorkTask(Guid id, byte[] message, Client client)
        {
            _client = client;
            Id = id;
            Message = message;
        }


        /// <summary>
        /// Marks this task as finished.
        /// Convenience wrapper around calling client.Acknowledge(task)
        /// </summary>
        /// <returns>The guid of the task</returns>
        public async Task<Guid> AcknowledgeAsync()
        {
            return await _client.AcknowledgeAsync(this);
        }

        /// <summary>
        /// Marks this task as finished.
        /// Convenience wrapper around calling client.Acknowledge(task)
        /// </summary>
        /// <returns>The guid of the task</returns>
        public Guid Acknowledge()
        {
            return _client.Acknowledge(this);
        }
    }
}