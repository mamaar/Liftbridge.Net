using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Liftbridge.Net
{
    public record SubscriptionOptions
    {
        public Proto.StartPosition StartPosition { get; init; }
        public long StartOffset { get; init; }
        public DateTimeOffset StartTimestamp { get; init; }
        public Proto.StopPosition StopPosition { get; init; }
        public long StopOffset { get; init; }
        public DateTimeOffset StopTimestamp { get; init; }
        public int Partition { get; init; }
        public bool ReadIsrReplica { get; init; }
        public bool Resume { get; init; }
    }

    /// <summary>
    /// Subscription is an async enumerable for consuming messages from a stream.
    /// </summary>
    public class Subscription : IAsyncEnumerable<Message>
    {
        private Grpc.Core.AsyncServerStreamingCall<Proto.Message> RpcSubscription { get; set; }
        private CancellationToken CancellationToken { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <example>
        /// <param name="rpcSub"></param>
        /// <param name="cancellationToken"></param>
        public Subscription(Grpc.Core.AsyncServerStreamingCall<Proto.Message> rpcSub, CancellationToken cancellationToken)
        {
            RpcSubscription = rpcSub;
            CancellationToken = cancellationToken;
        }
        public IAsyncEnumerator<Message> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new SubscriptionEnumerator(RpcSubscription, CancellationToken);
        }
    }

    public class SubscriptionEnumerator : IAsyncEnumerator<Message>
    {
        private Grpc.Core.AsyncServerStreamingCall<Proto.Message> RpcSubscription { get; set; }
        private CancellationToken CancellationToken { get; set; }

        public SubscriptionEnumerator(Grpc.Core.AsyncServerStreamingCall<Proto.Message> rpcSub, CancellationToken cancellationToken)
        {
            RpcSubscription = rpcSub;
            CancellationToken = cancellationToken;
        }

        public Message Current => Message.FromProto(RpcSubscription.ResponseStream.Current);

        public ValueTask DisposeAsync()
        {
            RpcSubscription.Dispose();
            return new ValueTask();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns>Whether a message has been consumed from the stream.</returns>
        /// <exception cref="StreamNotExistsException">Thrown when the stream has been deleted after subscribing.</exception>
        /// <exception cref="StreamPausedException">Thrown when the stream has been paused.</exception>
        /// <exception cref="EndOfReadonlyException">Thrown when the end of a readonly partition has been reached.</exception>
        /// <exception cref="BrokerNotFoundException">Thrown when the Liftbridge broker could not be reached.</exception>
        public ValueTask<bool> MoveNextAsync()
        {
            try
            {
                return new ValueTask<bool>(RpcSubscription.ResponseStream.MoveNext(CancellationToken));
            }
            catch (Grpc.Core.RpcException ex)
            {
                switch (ex.StatusCode)
                {
                    case Grpc.Core.StatusCode.Cancelled:
                        return ValueTask.FromResult(false);
                    case Grpc.Core.StatusCode.NotFound:
                        throw new StreamNotExistsException();
                    case Grpc.Core.StatusCode.FailedPrecondition:
                        throw new StreamPausedException();
                    case Grpc.Core.StatusCode.ResourceExhausted:
                        throw new EndOfReadonlyException();
                    case Grpc.Core.StatusCode.Unavailable:
                        throw new BrokerNotFoundException();
                }
                throw;
            }
        }
    }
}
