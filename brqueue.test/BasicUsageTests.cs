using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using NUnit.Framework;

namespace brqueue.test
{
    [TestFixture]
    public class BasicUsageTests
    {
        [Test]
        public async Task EnqueuePopProcessUsingClient()
        {
            // Connect to the server
            using (var client = new Client("localhost"))
            {
                // Create some sort of message to send across
                var messageText = "Hello world!";
                var message = System.Text.Encoding.UTF8.GetBytes(messageText);

                // Put the task in the queue
                var id = await client.EnqueueRequestAsync(message, Priority.High, new List<string>());

                // Pop the task of the queue again (Normally in another process)
                var task = await client.PopAsync(new List<string>(), false);

                Assert.AreEqual(id, task.Id);
                Assert.AreEqual(messageText, System.Text.Encoding.UTF8.GetString(task.Message));

                await client.AcknowledgeAsync(task);
            }
        }

        [Test]
        public async Task OnFailTaskWillBeEnqueuedAgain()
        {
            
            // Create some sort of message to send across
            var messageText = "Hello world!";

            Guid taskId;
            
            // Connect to the server
            using (var client = new Client("localhost"))
            {
                var message = System.Text.Encoding.UTF8.GetBytes(messageText);

                // Put the task in the queue
                taskId = await client.EnqueueRequestAsync(message, Priority.High, new List<string>());

                // Pop the task of the queue again (Normally in another process)
                var task = await client.PopAsync(new List<string>(), false);

                Assert.AreEqual(taskId, task.Id);
                
                // Disconnect!
            }
            
            
            // Connect to the server again
            using (var client = new Client("localhost"))
            {
                // Pop the task of the queue again (Normally in another process)
                var task = await client.PopAsync(new List<string>(), false);

                Assert.AreEqual(taskId, task.Id);
                Assert.AreEqual(messageText, System.Text.Encoding.UTF8.GetString(task.Message));

                await client.AcknowledgeAsync(task);
            }
        }
    }
}