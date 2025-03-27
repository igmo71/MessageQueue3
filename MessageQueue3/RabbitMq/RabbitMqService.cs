using Serilog;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Unicode;

namespace MessageQueue3.RabbitMq
{
    public interface IRabbitMqService
    {
        //Task BasicPublishAsync(JsonObject jsonMessage, string exchange, string routingKey);
        Task BasicPublishAsync(string message, string exchange, string routingKey);
    }

    public class RabbitMqService : IRabbitMqService, IDisposable
    {

        private RabbitMqClient _rabbitMqClient;
        private readonly JsonSerializerOptions _jsonSerializerOptions;

        public RabbitMqService(RabbitMqClient rabbitMqClient)
        {
            _rabbitMqClient = rabbitMqClient;
            _jsonSerializerOptions = new JsonSerializerOptions
            {
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.BasicLatin, UnicodeRanges.Cyrillic),
                WriteIndented = true
            };
        }

        //public async Task BasicPublishAsync(JsonObject jsonMessage, string exchange, string routingKey)
        public async Task BasicPublishAsync(string message, string exchange, string routingKey)
        {
            //var message = jsonMessage.ToJsonString(_jsonSerializerOptions);


            Log.Debug("{Context} {Method} - Start {exchange} {routingKey} {message}",
                nameof(RabbitMqService), nameof(BasicPublishAsync), exchange, routingKey, message);

            if (_rabbitMqClient.ServiceIsFail)
                await _rabbitMqClient.InitializeAsync();

            await _rabbitMqClient.BasicPublishAsync(message, exchange, routingKey);

            Log.Debug("{Context} {Method} - Ok {exchange} {routingKey} {message}", 
                nameof(RabbitMqService), nameof(BasicPublishAsync), exchange, routingKey, message);
        }     

        public void Dispose()
        {
            _rabbitMqClient.Dispose();
        }
    }
}
