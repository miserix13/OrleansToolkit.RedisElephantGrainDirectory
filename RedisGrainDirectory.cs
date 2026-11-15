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

        public RedisGrainDirectory(string connectionString, string clusterId, int db = 0) :
            base()
        {
            this.multiplexer = ConnectionMultiplexer.Connect(connectionString);
            this.hashMap = new($"{clusterId}_directory", new ValueRedisDictionaryConverter<string>(), this.multiplexer, db);
        }

        public async ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            this.hashMap.Dispose();
            await this.multiplexer.DisposeAsync();
        }

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

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            _ = await this.hashMap.TryAddAsync(address.GrainId.ToString(), Address.FromGrainAddress(address).ToString(), true);
            return address;
        }

        public async Task Unregister(GrainAddress address)
        {
            _ = await this.hashMap.TryRemoveAsync(address.GrainId.ToString());
        }

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

        public override string ToString()
        {
            return JsonSerializer.Serialize<Address>(this);
        }

        public static Address Parse(string value)
        {
            return JsonSerializer.Deserialize<Address>(value);
        }

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
