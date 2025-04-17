using System;
using System.Collections.Generic;
using System.Text;

namespace WorkflowCore.Providers.Redis
{
    public class RedisConnectionCfg
    {
        public ConnectionMode ConnectionMode { get; set; }
        public string ConnectionString { get; set; }
        public string UserAssignedManagedIdentityClientId { get; set; }
        public string Prefix { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
        public string Channel { get; set; }
    }
}
