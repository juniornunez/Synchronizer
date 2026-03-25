using Newtonsoft.Json;

namespace PagilaSynchronizer.Services
{
   

    public class ColumnMap
    {
        [JsonProperty("master")] public string Master { get; set; } = "";
        [JsonProperty("slave")] public string Slave { get; set; } = "";
    }

    public class TableMap
    {
        [JsonProperty("name")] public string Name { get; set; } = "";
        [JsonProperty("masterTable")] public string MasterTable { get; set; } = "";
        [JsonProperty("slaveTable")] public string SlaveTable { get; set; } = "";
        [JsonProperty("logTable")] public string LogTable { get; set; } = "";
        [JsonProperty("primaryKey")] public string PrimaryKey { get; set; } = "";
        [JsonProperty("columns")] public List<ColumnMap> Columns { get; set; } = new();
    }

    public class MasterConfig
    {
        [JsonProperty("host")] public string Host { get; set; } = "";
        [JsonProperty("port")] public int Port { get; set; } = 5432;
        [JsonProperty("database")] public string Database { get; set; } = "";
        [JsonProperty("user")] public string User { get; set; } = "";
        [JsonProperty("password")] public string Password { get; set; } = "";
    }

    public class SlaveConfig
    {
        [JsonProperty("server")] public string Server { get; set; } = "";
        [JsonProperty("database")] public string Database { get; set; } = "";
        [JsonProperty("trustedConnection")] public bool TrustedConnection { get; set; } = true;
        [JsonProperty("trustServerCertificate")] public bool TrustServerCertificate { get; set; } = true;
    }

    public class SyncMapping
    {
        [JsonProperty("master")] public MasterConfig Master { get; set; } = new();
        [JsonProperty("slave")] public SlaveConfig Slave { get; set; } = new();
        [JsonProperty("tablesIN")] public List<TableMap> TablesIN { get; set; } = new();
        [JsonProperty("tablesOUT")] public List<TableMap> TablesOUT { get; set; } = new();
    }


    public class MappingService
    {
        public SyncMapping Mapping { get; private set; }

        public MappingService(IWebHostEnvironment env)
        {
            var path = Path.Combine(env.ContentRootPath, "mapping.json");
            var json = File.ReadAllText(path);
            Mapping = JsonConvert.DeserializeObject<SyncMapping>(json)!;
        }

        public string GetMasterConnectionString()
        {
            var m = Mapping.Master;
            return $"Host={m.Host};Port={m.Port};Database={m.Database};Username={m.User};Password={m.Password}";
        }

        public string GetSlaveConnectionString()
        {
            var s = Mapping.Slave;
            if (s.TrustedConnection)
                return $"Server={s.Server};Database={s.Database};Integrated Security=true;TrustServerCertificate={s.TrustServerCertificate};";
            return $"Server={s.Server};Database={s.Database};TrustServerCertificate={s.TrustServerCertificate};";
        }
    }
}