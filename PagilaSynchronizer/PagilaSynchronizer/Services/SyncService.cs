using Microsoft.Data.SqlClient;
using Npgsql;
using System.Data;

namespace PagilaSynchronizer.Services
{
    public class SyncResult
    {
        public bool Success { get; set; }
        public string TableName { get; set; } = "";
        public int RowsAffected { get; set; }
        public string Message { get; set; } = "";
        public string Operation { get; set; } = "";
    }

    public class SyncService
    {
        private readonly MappingService _mappingService;
        private readonly ILogger<SyncService> _logger;

        private static readonly List<string> TodasLasTablas = new()
        {
            "payment", "rental", "customer",
            "inventory", "film_category", "film_actor",
            "store", "staff", "address", "city", "country",
            "film", "category", "actor", "language"
        };

        private static readonly List<string> OrdenInsercion = new()
        {
            "language", "actor", "category", "film",
            "film_actor", "film_category",
            "country", "city", "address",
            "staff", "store", "inventory"
        };

        public SyncService(MappingService mappingService, ILogger<SyncService> logger)
        {
            _mappingService = mappingService;
            _logger = logger;
        }

        public async Task<List<SyncResult>> SyncInAsync()
        {
            var resultados = new List<SyncResult>();

            using var conexionMaster = new NpgsqlConnection(_mappingService.GetMasterConnectionString());
            using var conexionSlave = new SqlConnection(_mappingService.GetSlaveConnectionString());

            await conexionMaster.OpenAsync();
            await conexionSlave.OpenAsync();

            DeshabilitarConstraints(conexionSlave);
            BorrarTablas(conexionSlave);

            foreach (var nombreTabla in OrdenInsercion)
            {
                var tabla = _mappingService.Mapping.TablesIN.FirstOrDefault(t => t.Name == nombreTabla);
                if (tabla == null) continue;

                var resultado = new SyncResult { TableName = tabla.Name, Operation = "SYNC-IN" };
                try
                {
                    var columnasMaster = string.Join(", ", tabla.Columns.Select(c => $"\"{c.Master}\""));
                    var datos = new DataTable();

                    await using (var query = new NpgsqlCommand($"SELECT {columnasMaster} FROM {tabla.MasterTable}", conexionMaster))
                    await using (var reader = await query.ExecuteReaderAsync())
                    {
                        datos.Load(reader);
                    }

                    var columnasSlave = string.Join(", ", tabla.Columns.Select(c => c.Slave));
                    var parametros = string.Join(", ", tabla.Columns.Select(c => $"@{c.Slave}"));
                    var sqlInsertar = $"INSERT INTO {tabla.SlaveTable} ({columnasSlave}) VALUES ({parametros})";

                    int filas = 0;
                    foreach (DataRow fila in datos.Rows)
                    {
                        await using var comando = new SqlCommand(sqlInsertar, conexionSlave);
                        foreach (var col in tabla.Columns)
                        {
                            var valor = fila[col.Master];
                            if (valor is Array arr)
                                valor = string.Join(", ", arr.Cast<object>().Select(o => o?.ToString() ?? ""));

                            comando.Parameters.AddWithValue($"@{col.Slave}", valor == DBNull.Value ? DBNull.Value : valor);
                        }
                        await comando.ExecuteNonQueryAsync();
                        filas++;
                    }

                    resultado.Success = true;
                    resultado.RowsAffected = filas;
                    resultado.Message = $"{filas} filas sincronizadas correctamente.";
                }
                catch (Exception ex)
                {
                    resultado.Success = false;
                    resultado.Message = ex.Message;
                    _logger.LogError(ex, "Error en Sync-IN tabla {Tabla}", tabla.Name);
                }

                resultados.Add(resultado);
            }

            HabilitarConstraints(conexionSlave);

            return resultados;
        }

        public async Task<List<SyncResult>> SyncOutAsync()
        {
            var resultados = new List<SyncResult>();

            using var conexionMaster = new NpgsqlConnection(_mappingService.GetMasterConnectionString());
            using var conexionSlave = new SqlConnection(_mappingService.GetSlaveConnectionString());

            await conexionMaster.OpenAsync();
            await conexionSlave.OpenAsync();

            foreach (var tabla in _mappingService.Mapping.TablesOUT)
            {
                var resultado = new SyncResult { TableName = tabla.Name, Operation = "SYNC-OUT" };
                try
                {
                    var datos = new DataTable();
                    await using (var queryLog = new SqlCommand($"SELECT * FROM {tabla.LogTable} ORDER BY log_id ASC", conexionSlave))
                    await using (var reader = await queryLog.ExecuteReaderAsync())
                    {
                        datos.Load(reader);
                    }

                    if (datos.Rows.Count == 0)
                    {
                        resultado.Success = true;
                        resultado.Message = "Sin cambios pendientes.";
                        resultados.Add(resultado);
                        continue;
                    }

                    int filas = 0;
                    var clavesPrimarias = tabla.PrimaryKey.Split(',').Select(p => p.Trim()).ToList();

                    foreach (DataRow fila in datos.Rows)
                    {
                        var operacion = fila["operation"].ToString()!.Trim().ToUpper();
                        try
                        {
                            if (operacion == "INSERT" || operacion == "UPDATE")
                            {
                                var condicionPK = string.Join(" AND ", clavesPrimarias.Select(pk => $"{pk} = @{pk}"));

                                await using var cmdExiste = new NpgsqlCommand(
                                    $"SELECT COUNT(1) FROM {tabla.MasterTable} WHERE {condicionPK}", conexionMaster);
                                foreach (var pk in clavesPrimarias)
                                    cmdExiste.Parameters.AddWithValue($"@{pk}", ObtenerValor(fila, tabla, pk));

                                var cantidad = (long)(await cmdExiste.ExecuteScalarAsync() ?? 0L);

                                if (cantidad > 0)
                                {
                                    var sets = tabla.Columns
                                        .Where(c => !clavesPrimarias.Contains(c.Master))
                                        .Select(c => $"{c.Master} = @{c.Master}");

                                    await using var cmdUpdate = new NpgsqlCommand(
                                        $"UPDATE {tabla.MasterTable} SET {string.Join(", ", sets)} WHERE {condicionPK}", conexionMaster);
                                    foreach (var col in tabla.Columns)
                                        cmdUpdate.Parameters.AddWithValue($"@{col.Master}", ObtenerValor(fila, tabla, col.Master));
                                    await cmdUpdate.ExecuteNonQueryAsync();
                                }
                                else
                                {
                                    var cols = string.Join(", ", tabla.Columns.Select(c => c.Master));
                                    var vals = string.Join(", ", tabla.Columns.Select(c => $"@{c.Master}"));

                                    await using var cmdInsert = new NpgsqlCommand(
                                        $"INSERT INTO {tabla.MasterTable} ({cols}) VALUES ({vals})", conexionMaster);
                                    foreach (var col in tabla.Columns)
                                        cmdInsert.Parameters.AddWithValue($"@{col.Master}", ObtenerValor(fila, tabla, col.Master));
                                    await cmdInsert.ExecuteNonQueryAsync();
                                }
                            }
                            else if (operacion == "DELETE")
                            {
                                var condicionPK = string.Join(" AND ", clavesPrimarias.Select(pk => $"{pk} = @{pk}"));
                                await using var cmdDelete = new NpgsqlCommand(
                                    $"DELETE FROM {tabla.MasterTable} WHERE {condicionPK}", conexionMaster);
                                foreach (var pk in clavesPrimarias)
                                    cmdDelete.Parameters.AddWithValue($"@{pk}", ObtenerValor(fila, tabla, pk));
                                await cmdDelete.ExecuteNonQueryAsync();
                            }

                            filas++;
                        }
                        catch (Exception exFila)
                        {
                            _logger.LogWarning(exFila, "Fila ignorada en {Tabla}: {Op}", tabla.Name, operacion);
                        }
                    }

                    await using (var limpiarLog = new SqlCommand($"DELETE FROM {tabla.LogTable}", conexionSlave))
                        await limpiarLog.ExecuteNonQueryAsync();

                    resultado.Success = true;
                    resultado.RowsAffected = filas;
                    resultado.Message = $"{filas} cambios aplicados al MASTER.";
                }
                catch (Exception ex)
                {
                    resultado.Success = false;
                    resultado.Message = ex.Message;
                    _logger.LogError(ex, "Error en Sync-OUT tabla {Tabla}", tabla.Name);
                }

                resultados.Add(resultado);
            }

            return resultados;
        }

        private void DeshabilitarConstraints(SqlConnection conexion)
        {
            foreach (var tabla in TodasLasTablas)
            {
                try
                {
                    using var cmd = new SqlCommand($"ALTER TABLE {tabla} NOCHECK CONSTRAINT ALL", conexion);
                    cmd.ExecuteNonQuery();
                }
                catch { }
            }
        }

        private void BorrarTablas(SqlConnection conexion)
        {
            foreach (var tabla in TodasLasTablas)
            {
                var tablaMap = _mappingService.Mapping.TablesIN.FirstOrDefault(t => t.Name == tabla);
                if (tablaMap == null) continue;
                try
                {
                    using var cmd = new SqlCommand($"DELETE FROM {tablaMap.SlaveTable}", conexion);
                    cmd.ExecuteNonQuery();
                }
                catch { }
            }
        }

        private void HabilitarConstraints(SqlConnection conexion)
        {
            foreach (var tabla in TodasLasTablas)
            {
                try
                {
                    using var cmd = new SqlCommand($"ALTER TABLE {tabla} WITH NOCHECK CHECK CONSTRAINT ALL", conexion);
                    cmd.ExecuteNonQuery();
                }
                catch { }
            }
        }

        private object ObtenerValor(DataRow fila, TableMap tabla, string columnaMaster)
        {
            var mapeo = tabla.Columns.FirstOrDefault(c => c.Master == columnaMaster);
            var nombreColumna = mapeo?.Slave ?? columnaMaster;

            if (fila.Table.Columns.Contains(nombreColumna))
            {
                var val = fila[nombreColumna];
                return val == DBNull.Value ? DBNull.Value : val;
            }

            if (fila.Table.Columns.Contains(columnaMaster))
            {
                var val = fila[columnaMaster];
                return val == DBNull.Value ? DBNull.Value : val;
            }

            return DBNull.Value;
        }
    }
}