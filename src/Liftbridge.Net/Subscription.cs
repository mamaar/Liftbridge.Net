using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Liftbridge.Net
{
    public class Subscription : IAsyncEnumerable<Message>
    {
        private Grpc.Core.AsyncServerStreamingCall<Proto.Message> RpcSubscription { get; set; }
        private CancellationToken CancellationToken { get; set; }

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
                        throw new StreamDeletedException();
                    case Grpc.Core.StatusCode.FailedPrecondition:
                        throw new StreamPausedException();
                    case Grpc.Core.StatusCode.ResourceExhausted:
                        // Indicates the end of a readonly partition has been reached.
                        throw new EndOfReadonlyException();
                    case Grpc.Core.StatusCode.Unavailable:
                        throw new BrokerNotFoundException();
                }
                throw;
            }
        }
    }
}
