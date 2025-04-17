using System;
using Microsoft.Extensions.Logging;
using WorkflowCore.Models;
using WorkflowCore.Providers.Redis;
using WorkflowCore.Providers.Redis.Services;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class ServiceCollectionExtensions
    {
        public static WorkflowOptions UseRedisQueues(this WorkflowOptions options, RedisConnectionCfg redisConnectionCfg)
        {
            options.UseQueueProvider(sp => new RedisQueueProvider(redisConnectionCfg, sp.GetService<ILoggerFactory>()));
            return options;
        }

        public static WorkflowOptions UseRedisLocking(this WorkflowOptions options, RedisConnectionCfg redisConnectionCfg)
        {
            options.UseDistributedLockManager(sp => new RedisLockProvider(redisConnectionCfg, sp.GetService<ILoggerFactory>()));
            return options;
        }

        public static WorkflowOptions UseRedisPersistence(this WorkflowOptions options, RedisConnectionCfg redisConnectionCfg, bool deleteComplete = false)
        {
            options.UsePersistence(sp => new RedisPersistenceProvider(redisConnectionCfg, deleteComplete, sp.GetService<ILoggerFactory>()));
            return options;
        }

        public static WorkflowOptions UseRedisEventHub(this WorkflowOptions options, RedisConnectionCfg redisConnectionCfg)
        {
            options.UseEventHub(sp => new RedisLifeCycleEventHub(redisConnectionCfg, sp.GetService<ILoggerFactory>()));
            return options;
        }
    }
}
