using Orleans.GrainDirectory;
using Orleans.Runtime;
using StackExchange.Redis;
using System.Net;
using System.Text.Json;
using Take.Elephant.Redis;
using Take.Elephant.Redis.Converters;

namespace OrleansToolkit.RedisElephantGrainDirectory
{
    public class RedisGrainDirectory : IGrainDirectory, IAsyncDisposable
    {
        private readonly ConnectionMultiplexer multiplexer;
        private readonly RedisHashMap<string, string> hashMap;

        /// <summary>
        /// Connects to Redis and initializes the hash map used to store grain addresses for the cluster.
        /// </summary>
        /// <param name="connectionString">The Redis connection string.</param>
        /// <param name="clusterId">The cluster identifier used to name the Redis map.</param>
        /// <param name="db">The Redis database index to use.</param>
        public RedisGrainDirectory(string connectionString, string clusterId, int db = 0) :
            base()
        {
            this.multiplexer = ConnectionMultiplexer.Connect(connectionString);
            this.hashMap = new($"{clusterId}_directory", new ValueRedisDictionaryConverter<string>(), this.multiplexer, db);
        }

        /// <summary>
        /// Disposes the Redis hash map and its connection multiplexer.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            this.hashMap.Dispose();
            await this.multiplexer.DisposeAsync();
        }

        /// <summary>
        /// Looks up a grain's address in Redis.
        /// </summary>
        /// <param name="grainId">The identifier of the grain to look up.</param>
        /// <returns>The grain address if it is registered; otherwise, <see langword="null"/>.</returns>
        public async Task<GrainAddress> Lookup(GrainId grainId)
        {
            if (await this.hashMap.ContainsKeyAsync(grainId.ToString()))
            {
                Address address = Address.Parse(await this.hashMap.GetValueOrDefaultAsync(grainId.ToString()));
                return address.ToGrainAddress();
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Stores or updates a grain's address in Redis.
        /// </summary>
        /// <param name="address">The grain address to register.</param>
        /// <returns>The supplied grain address.</returns>
        public async Task<GrainAddress> Register(GrainAddress address)
        {
            _ = await this.hashMap.TryAddAsync(address.GrainId.ToString(), Address.FromGrainAddress(address).ToString(), true);
            return address;
        }

        /// <summary>
        /// Removes a grain's address from Redis.
        /// </summary>
        /// <param name="address">The grain address to unregister.</param>
        public async Task Unregister(GrainAddress address)
        {
            _ = await this.hashMap.TryRemoveAsync(address.GrainId.ToString());
        }

        /// <summary>
        /// Completes without removing entries for the supplied silos; silo-based cleanup is not implemented.
        /// </summary>
        /// <param name="siloAddresses">The silo addresses that would be used to find entries to remove.</param>
        /// <returns>A completed task.</returns>
        public Task UnregisterSilos(List<SiloAddress> siloAddresses)
        {
            return Task.CompletedTask;
        }
    }

    public sealed record Address
    {
        public string HackId { get; set; }
        public string Id { get; set; }
        public string Silo { get; set; }
        public long Version { get; set; }

        /// <summary>
        /// Serializes this address to a JSON string.
        /// </summary>
        /// <returns>The JSON representation of this address.</returns>
        public override string ToString()
        {
            return JsonSerializer.Serialize<Address>(this);
        }

        /// <summary>
        /// Deserializes an address from its JSON representation.
        /// </summary>
        /// <param name="value">The JSON string to deserialize.</param>
        /// <returns>The deserialized address.</returns>
        public static Address Parse(string value)
        {
            return JsonSerializer.Deserialize<Address>(value);
        }

        /// <summary>
        /// Converts this stored address representation to an Orleans grain address.
        /// </summary>
        /// <returns>The corresponding Orleans grain address.</returns>
        public GrainAddress ToGrainAddress()
        {
            return new()
            {
                ActivationId = ActivationId.FromParsableString(this.HackId),
                GrainId = GrainId.Parse(this.Id),
                SiloAddress = SiloAddress.FromParsableString(this.Silo),
                MembershipVersion = new(this.Version)
            };
        }

        /// <summary>
        /// Creates a stored address representation from an Orleans grain address.
        /// </summary>
        /// <param name="address">The Orleans grain address to convert.</param>
        /// <returns>The corresponding stored address representation.</returns>
        public static Address FromGrainAddress(GrainAddress address)
        {
            return new()
            {
                HackId = address.ActivationId.ToParsableString(),
                Id = address.GrainId.ToString(),
                Silo = address.SiloAddress.ToParsableString(),
                Version = address.MembershipVersion.Value
            };
        }
    }
}
