using Microsoft.AspNetCore.Mvc;
using Serilog;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace MessageQueue3.RabbitMq
{
    public static class RabbitMqWebApi
    {
        public static void MapRabbitMqEndpoints(this IEndpointRouteBuilder routes)
        {
            var prGroup = routes.MapGroup("/api/primrose").WithTags("Primrose");

            prGroup.MapPost("/catalog_warehouses", CatalogWarehouses).WithName("CatalogWarehouses");
            prGroup.MapPost("/catalog_prices", CatalogPrices).WithName("CatalogPrices");
            prGroup.MapPost("/catalog_products", CatalogProducts).WithName("CatalogProducts");
            prGroup.MapPost("/catalog_offers", CatalogOffers).WithName("CatalogOffers");
        }

        private static async Task CatalogWarehouses([FromBody] JsonElement body, [FromServices] IRabbitMqService rabbitMqService)
        {
            var message = body.ToString();
            Log.Debug("{Context} {Endpoint} {@body}", nameof(RabbitMqWebApi), nameof(CatalogWarehouses), body);
            //await rabbitMqService.BasicPublishAsync(body, exchange: "primrose", routingKey: "catalog_warehouses");
            await rabbitMqService.BasicPublishAsync(message, exchange: "primrose", routingKey: "catalog_warehouses");
        }

        private static async Task CatalogPrices([FromBody] JsonElement body, [FromServices] IRabbitMqService rabbitMqService)
        {
            var message = body.ToString();
            Log.Debug("{Context} {Endpoint} {@body}", nameof(RabbitMqWebApi), nameof(CatalogPrices), body);
            await rabbitMqService.BasicPublishAsync(message, exchange: "primrose", routingKey: "catalog_prices");
        }

        private static async Task CatalogProducts([FromBody] JsonElement body, [FromServices] IRabbitMqService rabbitMqService)
        {
            var message = body.ToString();
            Log.Debug("{Context} {Endpoint} {@body}", nameof(RabbitMqWebApi), nameof(CatalogProducts), body);
            await rabbitMqService.BasicPublishAsync(message, exchange: "primrose", routingKey: "catalog_products");
        }

        private static async Task CatalogOffers([FromBody] JsonElement body, [FromServices] IRabbitMqService rabbitMqService)
        {
            var message = body.ToString();
            Log.Debug("{Context} {Endpoint} {@body}", nameof(RabbitMqWebApi), nameof(CatalogOffers), body);
            await rabbitMqService.BasicPublishAsync(message, exchange: "primrose", routingKey: "catalog_offers");
        }
    }
}
