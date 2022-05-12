using Abstractions;
using Orleans;

namespace Clients.WorkerService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IClusterClient _orleansClusterClient;

        public Worker(ILogger<Worker> logger, IClusterClient orleansClusterClient)
        {
            _logger = logger;
            _orleansClusterClient = orleansClusterClient;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await _orleansClusterClient.Connect();
            await base.StartAsync(cancellationToken);
        }

        public override async Task StopAsync(CancellationToken cancellationToken)
        {
            await _orleansClusterClient.DisposeAsync();
            await base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var rnd = new Random();
            var randomDeviceIDs = new List<string>();
            var randomDevices = new Dictionary<string, ISensorTwinGrain>();

            for (int i = 0; i < 256; i++)
            {
                var key = $"device{i.ToString().PadLeft(5, '0')}-{rnd.Next(10000, 99999)}-{Environment.MachineName}";
                randomDeviceIDs.Add(key);
                randomDevices.Add(key, _orleansClusterClient.GetGrain<ISensorTwinGrain>(key));
            }

            while (!stoppingToken.IsCancellationRequested)
            {
                await Parallel.ForEachAsync(randomDeviceIDs, async (deviceId, stoppingToken) =>
                {
                    await randomDevices[deviceId].ReceiveSensorState(new SensorState
                    {
                        SensorId = deviceId,
                        TimeStamp = DateTime.Now,
                        Type = SensorType.Unspecified,
                        Value = rnd.Next(0, 100)
                    });
                });
            }
        }
    }
}