using Microsoft.AspNetCore.Mvc;
using PagilaSynchronizer.Services;

namespace PagilaSynchronizer.Controllers
{
    public class SyncController : Controller
    {
        private readonly SyncService _sync;
        private readonly MappingService _mapping;

        public SyncController(SyncService sync, MappingService mapping)
        {
            _sync = sync;
            _mapping = mapping;
        }

        public IActionResult Index()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> SyncIn()
        {
            var results = await _sync.SyncInAsync();
            return Json(results);
        }

       
        [HttpPost]
        public async Task<IActionResult> SyncOut()
        {
            var results = await _sync.SyncOutAsync();
            return Json(results);
        }

        
        [HttpGet]
        public async Task<IActionResult> Status()
        {
            var status = new
            {
                master = await TestMaster(),
                slave = await TestSlave()
            };
            return Json(status);
        }

        private async Task<object> TestMaster()
        {
            try
            {
                using var conn = new Npgsql.NpgsqlConnection(_mapping.GetMasterConnectionString());
                await conn.OpenAsync();
                return new { connected = true, message = "PostgreSQL OK" };
            }
            catch (Exception ex)
            {
                return new { connected = false, message = ex.Message };
            }
        }

        private async Task<object> TestSlave()
        {
            try
            {
                using var conn = new Microsoft.Data.SqlClient.SqlConnection(_mapping.GetSlaveConnectionString());
                await conn.OpenAsync();
                return new { connected = true, message = "SQL Server OK" };
            }
            catch (Exception ex)
            {
                return new { connected = false, message = ex.Message };
            }
        }
    }
}