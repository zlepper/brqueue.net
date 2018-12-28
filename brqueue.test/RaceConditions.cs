using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Timers;
using NUnit.Framework;

namespace brqueue.test
{
    [TestFixture]
    public class RaceConditions
    {
        [Test]
        public async Task Push()
        {
            const int threadCount = 100; 
            const int messageCount = 10000;

            var sw = new Stopwatch();
            sw.Start();
            var client = new Client("localhost");

            sw.Stop();
            Console.WriteLine($"Client creation took {sw.Elapsed}");
            
            var message = Encoding.UTF8.GetBytes("Hello world1!");
            
            sw.Restart();
            
            var tasks = new List<Task>(threadCount);
            for (var i = 0; i < threadCount; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    for (var j = 0; j < messageCount; j++)
                    {
                        await client.EnqueueRequestAsync(message, Priority.High, new List<string>());
                    }
                }));
            }

            await Task.WhenAll(tasks);
            
            sw.Stop();
            
            Console.WriteLine($"Execution took {sw.Elapsed}");
        }

        [Test]
        public void Pop()
        {
            
        }

        [Test]
        public void PushPop()
        {
            
        }
    }
}