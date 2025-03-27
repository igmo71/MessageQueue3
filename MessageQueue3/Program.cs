
using MessageQueue3.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Serilog;

namespace MessageQueue3
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            //var logger = new LoggerConfiguration()
            //    .ReadFrom.Configuration(builder.Configuration)
            //    .CreateLogger();
            //builder.Host.UseSerilog(logger);

            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(builder.Configuration)
                .CreateLogger();

            // Add services to the container.
            builder.Services.AddAuthorization();

            // Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
            builder.Services.AddOpenApi();

            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            var rabbitMqConfig = builder.Configuration.GetSection(RabbitMqConfig.Section).Get<RabbitMqConfig>()
                ?? throw new ApplicationException("RabbitMq Configuration Not Found");
            var rabbitMqClient = await RabbitMqClient.CreateAsync(rabbitMqConfig);

            builder.Services.AddSingleton<IRabbitMqService, RabbitMqService>(factory => new RabbitMqService(rabbitMqClient));

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            //if (app.Environment.IsDevelopment())
            {
                app.MapOpenApi();
                //app.UseSwaggerUI(options =>
                //{
                //    options.SwaggerEndpoint("/openapi/v1.json", "OpenAPI V1");
                //});

                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();

            app.MapRabbitMqEndpoints();

            app.Run();
        }
    }
}
