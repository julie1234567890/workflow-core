using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace WorkflowCore.Providers.Redis.Services
{
    internal class RedisHelper
    {
        public static async Task<ConnectionMultiplexer> BuildConnectionMultiplexer(RedisConnectionCfg redisConnectionCfg)
        {
            if (redisConnectionCfg.ConnectionMode == ConnectionMode.UserAssignedManagedIdentity)
            {
                var configurationOptions = ConfigurationOptions.Parse($"{redisConnectionCfg.Host}:{redisConnectionCfg.Port}");
                await configurationOptions.ConfigureForAzureWithUserAssignedManagedIdentityAsync(redisConnectionCfg.UserAssignedManagedIdentityClientId);
                return await ConnectionMultiplexer.ConnectAsync(configurationOptions);
            }
            else
            {
                return await ConnectionMultiplexer.ConnectAsync(redisConnectionCfg.ConnectionString);
            }
        }
    }
}
