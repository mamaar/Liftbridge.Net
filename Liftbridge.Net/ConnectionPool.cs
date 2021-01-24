using System;
using System.Timers;
using System.Collections.Immutable;
using System.Threading.Tasks;
using Grpc.Core;

namespace Liftbridge.Net
{
    public class ConnectionPool {
        private uint MaxConns;
        private TimeSpan? KeepAliveTime;
        private ImmutableList<Channel> Connections;
        private ImmutableDictionary<Channel, Timer> Timers;

        public ConnectionPool(uint maxConnections, TimeSpan? keepAliveTime = null)
        {
            MaxConns = maxConnections;
            KeepAliveTime = keepAliveTime;
            Connections = ImmutableList<Channel>.Empty;
            Timers = ImmutableDictionary<Channel, Timer>.Empty;
        }

        public Channel Get(Func<Channel> factory)
        {
            lock (Connections)
            {
                if (Connections.IsEmpty)
                {
                    var ch = factory();
                    Connections = Connections.Add(ch);
                }

                var conn = Connections[0];
                Connections.Remove(conn);
                Timer keepAliveTimer = Timers[conn];
                keepAliveTimer.Stop();
                Timers.Remove(conn);
                return conn;
            }
        }

        public async void Put(Channel conn)
        {
            // Close connection if the pool is full;
            if (Connections.Count == MaxConns)
            {
                await conn.ShutdownAsync();
                return;
            }
            lock (Connections)
            {
                Connections = Connections.Add(conn);
                if (KeepAliveTime != null)
                {
                    var timer = new Timer(KeepAliveTime.Value.TotalMilliseconds);
                    Timers = Timers.Add(conn, timer);
                    timer.Elapsed += async (object sender, ElapsedEventArgs e) =>
                    {
                        Connections = Connections.Remove(conn);
                        await conn.ShutdownAsync();
                        Timers = Timers.Remove(conn);
                        timer.Dispose();
                    };
                    timer.Start();
                }
            }
        }

        public async void Close()
        {
            ImmutableList<Task> closing = ImmutableList<Task>.Empty;
            foreach(var conn in Connections)
            {
                closing = closing.Add(conn.ShutdownAsync());
            }

            foreach(var timer in Timers.Values)
            {
                timer.Stop();
                timer.Dispose();
            }

            Connections = Connections.Clear();
            Timers = Timers.Clear();
            await Task.WhenAll(closing);
        }
    }
}
