using EventStore.Client;
using Microsoft.EntityFrameworkCore;
using System.Text;
using System.Text.Json;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Регистрация клиента для EventStoreDB
builder.Services.AddEventStoreClient("esdb://localhost:2113?tls=false");

// Регистрация DbContext для PostgreSQL
builder.Services.AddDbContext<WarehouseDbContext>(options =>
{
    options.UseNpgsql("Host=localhost;Port=5432;Database=warehouse;Username=postgres;Password=postgres",
        b => b.MigrationsAssembly("Warehouse"));
});

var app = builder.Build();

app.MapPost("/orders", async (PlaceOrderCommand command, EventStoreClient client, WarehouseDbContext db) =>
{
    // Получаем текущее состояние товара через проекцию
    var product = await Projections.GetProductProjection(command.ProductId, client, db);

    // Проверяем бизнес-логику (достаточно ли товара)
    if (product.QuantityInStock >= command.Quantity)
    {
        // Создаем экземпляр события, используя конструктор record'а
        var orderPlacedEvent = new OrderPlacedEvent(command.ProductId, command.Quantity);
        // Готовим событие к отправке (сериализуем в JSON/UTF8)
        var eventData = new EventData(
            Uuid.NewUuid(),
            nameof(OrderPlacedEvent), // Имя события - теперь безопасно получаем из типа
            JsonSerializer.SerializeToUtf8Bytes((object)orderPlacedEvent) // Приводим к object для полиморфизма
        );

        // Записываем событие в поток, связанный с продуктом
        await client.AppendToStreamAsync(
            $"product-{command.ProductId}", // Имя потока
            StreamState.Any, // Не проверяем версию потока
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
    // Читаем поток событий для продукта от начала до конца
    var events = client.ReadStreamAsync(
        Direction.Forwards,
        $"product-{productId}",
        StreamPosition.Start
    );
    // Десериализуем и возвращаем каждое событие
    return events.Select(e => JsonSerializer.Deserialize<OrderPlacedEvent>(e.Event.Data.ToArray()));
});
app.UseSwagger();
app.UseSwaggerUI();
app.Run();

// Базовый интерфейс для всех событий, добавляющий метаданные
public interface IEvent
{
    // Альтернатива - https://github.com/phatboyg/NewId
    Guid EventId => Guid.NewGuid();
    public DateTime OccurredOn => DateTime.UtcNow;
}

// Маркерный интерфейс для доменных событий.
public interface IDomainEvent : IEvent { }

// Событие, описывающее факт размещения заказа,
// определенное как неизменяемый record
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

    // Apply events to mutate the state of the product.
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
        // Получаем "снимок" или начальное состояние из PostgreSQL
        var product = await db.Products.FindAsync(productId);
        if (product == null) return null;

        try
        {
            // Читаем поток событий, связанных с этим продуктом
            var events = client.ReadStreamAsync(Direction.Forwards, $"product-{productId}", StreamPosition.Start);

            // В цикле применяем каждое событие к состоянию объекта
            await foreach (var resolvedEvent in events)
            {
                var eventData = resolvedEvent.Event.Data.ToArray();
                // В реальном проекте здесь будет логика выбора типа для десериализации
                // на основе resolvedEvent.Event.EventType. Для простоты пока используем один тип.
                var type = Type.GetType(resolvedEvent.Event.EventType);
                dynamic orderPlacedEvent = JsonSerializer.Deserialize(eventData, type);
                if (orderPlacedEvent is not null)
                    product.Apply(orderPlacedEvent); // <--- Ключевой момент!
            }
        }
        catch (StreamNotFoundException)
        {
            // Если событий еще не было, просто возвращаем начальное состояние
            return product;
        }
        return product;
    }

}

