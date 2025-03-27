using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using System.Collections.Concurrent;
using System.Text;

namespace MessageQueue3.RabbitMq
{
    public class RabbitMqClient : IDisposable
    {
        private RabbitMqConfig rabbitMqConfig;
        private IConnection? _connection;
        private IChannel? _channel;
        private ConcurrentDictionary<ulong, string> _outstandingConfirms = new();
        const ushort MAX_OUTSTANDING_CONFIRMS = 256;

        public RabbitMqClient(RabbitMqConfig rabbitMqConfig)
        {
            this.rabbitMqConfig = rabbitMqConfig;
        }

        public bool ServiceIsFail => _connection is null || !_connection.IsOpen || _channel is null || _channel.IsClosed;

        public static Task<RabbitMqClient> CreateAsync(RabbitMqConfig rabbitMqConfig)
        {
            var client = new RabbitMqClient(rabbitMqConfig);

            return client.InitializeAsync();
        }

        public async Task<RabbitMqClient> InitializeAsync()
        {
            Log.Debug("{Context} {Method} - Start {@rabbitMqConfig}",
                nameof(RabbitMqClient), nameof(InitializeAsync), rabbitMqConfig);

            var factory = new ConnectionFactory
            {
                HostName = rabbitMqConfig.HostName,
                Port = rabbitMqConfig.Port,
                UserName = rabbitMqConfig.UserName,
                Password = rabbitMqConfig.Password
            };

            _connection = await factory.CreateConnectionAsync();

            _connection.ConnectionShutdownAsync += OnConnectionShutdownAsync;

            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true,
                outstandingPublisherConfirmationsRateLimiter: new ThrottlingRateLimiter(MAX_OUTSTANDING_CONFIRMS));

            _channel = await _connection.CreateChannelAsync(channelOptions);

            _channel.BasicAcksAsync += OnBasicAcksAsync;
            _channel.BasicNacksAsync += OnBasicNacksAsync;
            _channel.ChannelShutdownAsync += OnChannelShutdownAsync;

            Log.Debug("{Context} {Method} - Ok {@rabbitMqConfig}",
                nameof(RabbitMqClient), nameof(InitializeAsync), rabbitMqConfig);

            await DeclareAsync();

            return this;
        }

        public async Task DeclareAsync()
        {
            Log.Debug("{Context} {Method} - Start {@rabbitMqConfig}",
                    nameof(RabbitMqClient), nameof(DeclareAsync), rabbitMqConfig);

            if (_channel is null || _channel.IsClosed)
            {
                Log.Error("{Context} {Method} - Channel Is Null or Closed {RabbitMqConfig}",
                    nameof(RabbitMqClient), nameof(DeclareAsync), rabbitMqConfig);
                return;
            }

            foreach (var bind in rabbitMqConfig.QueueBinds)
            {
                await _channel.ExchangeDeclareAsync(
                    exchange: bind.Exchange,
                    type: ExchangeType.Direct,
                    durable: true,
                    autoDelete: false);

                Dictionary<string, object?> arguments = new() { ["x-queue-type"] = "quorum" };
                QueueDeclareOk queueDeclareOk = await _channel.QueueDeclareAsync(
                   queue: bind.Queue,
                   durable: true,
                   exclusive: false,
                   autoDelete: false,
                   arguments);

                await _channel.QueueBindAsync(
                    queue: bind.Queue,
                    exchange: bind.Exchange,
                    routingKey: bind.RoutingKey);
            }

            Log.Debug("{Context} {Method} - Ok {@rabbitMqConfig}",
                    nameof(RabbitMqClient), nameof(DeclareAsync), rabbitMqConfig);
        }

        public async Task BasicPublishAsync(string message, string exchange, string routingKey)
        {
            Log.Debug("{Context} {Method} - Start {message} {exchange} {routingKey}",
                    nameof(RabbitMqClient), nameof(BasicPublishAsync), message, exchange, routingKey);

            if (_channel is null)
            {
                Log.Error("{Context} {Method} - Channel Is Null {Message} {Exchange} {RoutingKey}",
                    nameof(RabbitMqClient), nameof(BasicPublishAsync), message, exchange, routingKey);
                return;
            }

            var nextPublishSequenceNumber = await _channel.GetNextPublishSequenceNumberAsync();

            _outstandingConfirms.TryAdd(nextPublishSequenceNumber, message);

            await _channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: routingKey,
                mandatory: true,
                basicProperties: new BasicProperties { Persistent = true },
                body: Encoding.UTF8.GetBytes(message));

            Log.Debug("{Context} {Method} - Ok {message} {exchange} {routingKey}",
                nameof(RabbitMqClient), nameof(BasicPublishAsync), message, exchange, routingKey);
        }

        private Task OnBasicAcksAsync(object sender, BasicAckEventArgs e)
        {
            _outstandingConfirms.TryGetValue(e.DeliveryTag, out string? message);

            CleanOutstandingConfirms(e.DeliveryTag, e.Multiple);

            Log.Debug("{Context} {Method} {@BasicAckEventArgs} {Message}", nameof(RabbitMqClient), nameof(OnBasicAcksAsync), e, message);

            return Task.CompletedTask;
        }

        private Task OnBasicNacksAsync(object sender, BasicNackEventArgs e)
        {
            _outstandingConfirms.TryGetValue(e.DeliveryTag, out string? message);

            CleanOutstandingConfirms(e.DeliveryTag, e.Multiple);

            Log.Error("{Context} {Method} {@BasicNackEventArgs} {Message}", nameof(RabbitMqClient), nameof(OnBasicNacksAsync), e, message);

            return Task.CompletedTask;
        }

        private void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
        {
            if (multiple)
            {
                foreach (var entry in _outstandingConfirms.Where(k => k.Key <= sequenceNumber))
                {
                    _outstandingConfirms.TryRemove(entry.Key, out _);
                }
            }
            else
            {
                _outstandingConfirms.TryRemove(sequenceNumber, out _);
            }
        }

        private Task OnChannelShutdownAsync(object sender, ShutdownEventArgs e)
        {
            Log.Error("{Context} {Method} {@ShutdownEventArgs}", nameof(RabbitMqClient), nameof(OnChannelShutdownAsync), e);

            Dispose();

            return Task.CompletedTask;
        }

        private Task OnConnectionShutdownAsync(object sender, ShutdownEventArgs e)
        {
            Log.Error("{Context} {Method} {@ShutdownEventArgs}", nameof(RabbitMqClient), nameof(OnConnectionShutdownAsync), e);

            Dispose();

            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _channel?.Dispose();
            _connection?.Dispose();
        }
    }
}
