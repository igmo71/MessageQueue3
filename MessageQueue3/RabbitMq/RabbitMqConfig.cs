namespace MessageQueue3.RabbitMq
{
    public class RabbitMqConfig
    {
        public const string Section = "RabbitMq";

        public required string HostName { get; set; }
        public required int Port { get; set; }
        public required string UserName { get; set; }
        public required string Password { get; set; }
        public List<QueueBind> QueueBinds { get; set; } = [];
    }

    public class QueueBind
    {
        public required string Exchange { get; set; }
        public required string Queue { get; set; }
        public required string RoutingKey { get; set; }

    }
}
