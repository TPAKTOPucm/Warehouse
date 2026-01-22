using EventStore.Client;
using Microsoft.EntityFrameworkCore;
using System.Text;
using System.Text.Json;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHostedService<EventProjectionService>();
builder.Services.AddEventStoreClient("esdb://localhost:2113?tls=false");

builder.Services.AddDbContext<WarehouseDbContext>(options =>
{
    options.UseNpgsql("Host=localhost;Port=5432;Database=warehouse;Username=postgres;Password=postgres",
        b => b.MigrationsAssembly("Warehouse"));
});

var app = builder.Build();

app.MapPost("/orders", async (PlaceOrderCommand command, EventStoreClient client, WarehouseDbContext db) =>
{
    var product = await Projections.GetProductProjection(command.ProductId, client, db);

    if (product.QuantityInStock >= command.Quantity)
    {
        var orderPlacedEvent = new OrderPlacedEvent(command.ProductId, command.Quantity);
        var eventData = new EventData(
            Uuid.NewUuid(),
            nameof(OrderPlacedEvent),
            JsonSerializer.SerializeToUtf8Bytes((object)orderPlacedEvent)
        );

        await client.AppendToStreamAsync(
            $"product-{command.ProductId}",
            StreamState.Any,
            new[] { eventData }
        );

        return Results.Ok("Order placed successfully.");
    }
    else
    {
        return Results.BadRequest("Insufficient stock.");
    }
});

app.MapPost("/orders/cancel", async (CancelOrderCommand command, EventStoreClient client, WarehouseDbContext db) =>
{
    var product = await Projections.GetProductProjection(command.ProductId, client, db);
    if (product is null)
        return Results.NotFound();
    var @event = new OrderCancelledEvent(command.ProductId, command.Quantity);

    var eventData = new EventData(Uuid.NewUuid(), nameof(OrderCancelledEvent), JsonSerializer.SerializeToUtf8Bytes((object)@event));

    await client.AppendToStreamAsync($"product-{command.ProductId}", StreamState.Any, new[] { eventData });

    return Results.Ok();
});

app.MapPost("/products/restock", async (ProductRestockCommand command, EventStoreClient client, WarehouseDbContext db) =>
{
    var product = await Projections.GetProductProjection(command.ProductId, client, db);
    if (product is null)
    {
        product = new Product(command.ProductId, command.Quantity);
        db.Add(product);
        await db.SaveChangesAsync();
        return Results.Ok();
    }
    var @event = new ProductRestockEvent(command.ProductId, command.Quantity);

    var eventData = new EventData(Uuid.NewUuid(), nameof(ProductRestockEvent), JsonSerializer.SerializeToUtf8Bytes((object)@event));

    await client.AppendToStreamAsync($"product-{command.ProductId}", StreamState.Any, new[] { eventData });

    return Results.Ok();
});

app.MapGet("/products", async (WarehouseDbContext db) => { return await db.Products.ToListAsync(); });

app.MapGet("/products/{productId}/events", (Guid productId, EventStoreClient client) =>
{
    var events = client.ReadStreamAsync(
        Direction.Forwards,
        $"product-{productId}",
        StreamPosition.Start
    );
    return events.Select(e => JsonSerializer.Deserialize<OrderPlacedEvent>(e.Event.Data.ToArray()));
});
app.UseSwagger();
app.UseSwaggerUI();
app.Run();

public interface IEvent
{
    Guid EventId => Guid.NewGuid();
    public DateTime OccurredOn => DateTime.UtcNow;
}

public interface IDomainEvent : IEvent { }

public record OrderPlacedEvent(Guid ProductId, int Quantity) : IDomainEvent;

public record OrderCancelledEvent(Guid ProductId, int Quantity) : IDomainEvent;

public record ProductRestockEvent(Guid ProductId, int Quantity) : IDomainEvent;

public record PlaceOrderCommand(Guid ProductId, int Quantity);

public record CancelOrderCommand(Guid ProductId, int Quantity);

public record ProductRestockCommand(Guid ProductId, int Quantity);

public class Product
{
    public Guid Id { get; private set; }
    public int QuantityInStock { get; private set; }

    public Product(Guid id, int quantityInStock)
    {
        Id = id;
        QuantityInStock = quantityInStock;
    }

    public void Apply(OrderPlacedEvent @event)
    {
        if (QuantityInStock >= @event.Quantity)
            QuantityInStock -= @event.Quantity;
        else
            throw new InvalidOperationException("Insufficient stock.");
    }

    public void Apply(OrderCancelledEvent @event)
    {
        QuantityInStock += @event.Quantity;
    }

    public void Apply(ProductRestockEvent @event)
    {
        QuantityInStock += @event.Quantity;
    }
}

public class WarehouseDbContext : DbContext
{
    public DbSet<Product> Products { get; set; }

    public WarehouseDbContext(DbContextOptions<WarehouseDbContext> options) : base(options)
    {
    }

    public WarehouseDbContext()
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.Entity<Product>().HasKey(p => p.Id);
    }
}

public class Projections
{
    public static async Task<Product?> GetProductProjection(Guid productId, EventStoreClient client, WarehouseDbContext db)
    {
        var product = await db.Products.FindAsync(productId);
        if (product == null) return null;

        try
        {
            var events = client.ReadStreamAsync(Direction.Forwards, $"product-{productId}", StreamPosition.Start);

            await foreach (var resolvedEvent in events)
            {
                var eventData = resolvedEvent.Event.Data.ToArray();
                var type = Type.GetType(resolvedEvent.Event.EventType);
                dynamic orderPlacedEvent = JsonSerializer.Deserialize(eventData, type);
                if (orderPlacedEvent is not null)
                    product.Apply(orderPlacedEvent);
            }
        }
        catch (StreamNotFoundException)
        {
            return product;
        }
        return product;
    }

}

public class EventProjectionService : BackgroundService
{
    private readonly EventStoreClient _eventStoreClient;
    private readonly ILogger<EventProjectionService> _logger;
    private readonly IServiceProvider _serviceProvider;

    public EventProjectionService(EventStoreClient eventStoreClient, ILogger<EventProjectionService> logger, IServiceProvider serviceProvider)
    {
        _eventStoreClient = eventStoreClient;
        _logger = logger;
        _serviceProvider = serviceProvider;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => _eventStoreClient.SubscribeToStreamAsync(
            "$ce-product",
            FromStream.End,
            async (subscription, resolvedEvent, ct) =>
            {
                try
                {
                    using var scope = _serviceProvider.CreateScope();
                    var dbContext = scope.ServiceProvider.GetRequiredService<WarehouseDbContext>();

                    var eventData = resolvedEvent.Event.Data.ToArray();
                    var type = Type.GetType(resolvedEvent.Event.EventType);
                    dynamic domainEvent = JsonSerializer.Deserialize(eventData, type);

                    if (domainEvent is not null)
                    {
                        await ApplyProjection(dbContext, domainEvent);
                        await dbContext.SaveChangesAsync(ct);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing event {EventId}", resolvedEvent.Event.EventId);
                }
            },
            cancellationToken: stoppingToken
        );

    private static async Task ApplyProjection(WarehouseDbContext db, dynamic domainEvent)
    {
        Product product = await db.Products.FindAsync(domainEvent.ProductId);
        product.Apply(domainEvent);
    }
}
