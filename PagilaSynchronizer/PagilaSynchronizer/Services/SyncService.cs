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
        private readonly MappingService _mapping;
        private readonly ILogger<SyncService> _logger;

        // Orden correcto para borrar (inverso a FK) y para insertar
        private static readonly List<string> DeleteOrder = new()
        {
            "inventory", "film_category", "film_actor",
            "staff", "store", "address", "city", "country",
            "film", "category", "actor", "language"
        };

        private static readonly List<string> InsertOrder = new()
        {
            "language", "actor", "category", "film",
            "film_actor", "film_category",
            "country", "city", "address",
            "store", "staff", "inventory"
        };

        public SyncService(MappingService mapping, ILogger<SyncService> logger)
        {
            _mapping = mapping;
            _logger = logger;
        }

        // ══════════════════════════════════════════════════════════════════════
        // SYNC-IN: PostgreSQL (MASTER) → SQL Server (SLAVE)
        // ══════════════════════════════════════════════════════════════════════
        public async Task<List<SyncResult>> SyncInAsync()
        {
            var results = new List<SyncResult>();

            using var pgConn = new NpgsqlConnection(_mapping.GetMasterConnectionString());
            using var sqlConn = new SqlConnection(_mapping.GetSlaveConnectionString());

            await pgConn.OpenAsync();
            await sqlConn.OpenAsync();

            // Deshabilitar FK constraints tabla por tabla
            var allTables = DeleteOrder.ToList();
            foreach (var t in allTables)
            {
                try
                {
                    await using var cmd = new SqlCommand($"ALTER TABLE {t} NOCHECK CONSTRAINT ALL", sqlConn);
                    await cmd.ExecuteNonQueryAsync();
                }
                catch { /* tabla puede no existir, ignorar */ }
            }

            // Borrar en orden inverso
            foreach (var tableName in DeleteOrder)
            {
                var table = _mapping.Mapping.TablesIN.FirstOrDefault(t => t.Name == tableName);
                if (table == null) continue;
                try
                {
                    await using var delCmd = new SqlCommand($"DELETE FROM {table.SlaveTable}", sqlConn);
                    await delCmd.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning("No se pudo borrar {Table}: {Msg}", tableName, ex.Message);
                }
            }

            // Insertar en orden correcto
            foreach (var tableName in InsertOrder)
            {
                var table = _mapping.Mapping.TablesIN.FirstOrDefault(t => t.Name == tableName);
                if (table == null) continue;

                var result = new SyncResult { TableName = table.Name, Operation = "SYNC-IN" };
                try
                {
                    // Leer datos del MASTER
                    var masterCols = string.Join(", ", table.Columns.Select(c => $"\"{c.Master}\""));
                    var dt = new DataTable();

                    await using (var pgCmd = new NpgsqlCommand($"SELECT {masterCols} FROM {table.MasterTable}", pgConn))
                    await using (var reader = await pgCmd.ExecuteReaderAsync())
                    {
                        dt.Load(reader);
                    }

                    // Insertar en SLAVE
                    var slaveCols = string.Join(", ", table.Columns.Select(c => c.Slave));
                    var slaveParams = string.Join(", ", table.Columns.Select(c => $"@{c.Slave}"));
                    var insertSql = $"INSERT INTO {table.SlaveTable} ({slaveCols}) VALUES ({slaveParams})";

                    int rows = 0;
                    foreach (DataRow row in dt.Rows)
                    {
                        await using var insCmd = new SqlCommand(insertSql, sqlConn);
                        foreach (var col in table.Columns)
                        {
                            var val = row[col.Master];
                            // Convertir arrays de PostgreSQL a string (ej: special_features)
                            if (val is Array arr)
                                val = string.Join(", ", arr.Cast<object>().Select(o => o?.ToString() ?? ""));

                            insCmd.Parameters.AddWithValue($"@{col.Slave}", val == DBNull.Value ? DBNull.Value : val);
                        }
                        await insCmd.ExecuteNonQueryAsync();
                        rows++;
                    }

                    result.Success = true;
                    result.RowsAffected = rows;
                    result.Message = $"{rows} filas sincronizadas correctamente.";
                }
                catch (Exception ex)
                {
                    result.Success = false;
                    result.Message = ex.Message;
                    _logger.LogError(ex, "SYNC-IN error en tabla {Table}", table.Name);
                }

                results.Add(result);
            }

            // Re-habilitar FK constraints tabla por tabla (sin validar datos viejos)
            foreach (var t in allTables)
            {
                try
                {
                    await using var cmd = new SqlCommand($"ALTER TABLE {t} WITH NOCHECK CHECK CONSTRAINT ALL", sqlConn);
                    await cmd.ExecuteNonQueryAsync();
                }
                catch { /* ignorar */ }
            }

            return results;
        }

        // ══════════════════════════════════════════════════════════════════════
        // SYNC-OUT: SQL Server SLAVE (_log) → PostgreSQL MASTER
        // ══════════════════════════════════════════════════════════════════════
        public async Task<List<SyncResult>> SyncOutAsync()
        {
            var results = new List<SyncResult>();

            using var pgConn = new NpgsqlConnection(_mapping.GetMasterConnectionString());
            using var sqlConn = new SqlConnection(_mapping.GetSlaveConnectionString());

            await pgConn.OpenAsync();
            await sqlConn.OpenAsync();

            foreach (var table in _mapping.Mapping.TablesOUT)
            {
                var result = new SyncResult { TableName = table.Name, Operation = "SYNC-OUT" };
                try
                {
                    var dt = new DataTable();
                    await using (var logCmd = new SqlCommand($"SELECT * FROM {table.LogTable} ORDER BY log_id ASC", sqlConn))
                    await using (var reader = await logCmd.ExecuteReaderAsync())
                    {
                        dt.Load(reader);
                    }

                    if (dt.Rows.Count == 0)
                    {
                        result.Success = true;
                        result.Message = "Sin cambios pendientes.";
                        results.Add(result);
                        continue;
                    }

                    int rows = 0;
                    var pks = table.PrimaryKey.Split(',').Select(p => p.Trim()).ToList();

                    foreach (DataRow row in dt.Rows)
                    {
                        var operation = row["operation"].ToString()!.Trim().ToUpper();
                        try
                        {
                            if (operation == "INSERT" || operation == "UPDATE")
                            {
                                var pkCondition = string.Join(" AND ", pks.Select(pk => $"{pk} = @{pk}"));
                                await using var existsCmd = new NpgsqlCommand(
                                    $"SELECT COUNT(1) FROM {table.MasterTable} WHERE {pkCondition}", pgConn);
                                foreach (var pk in pks)
                                    existsCmd.Parameters.AddWithValue($"@{pk}", GetRowValue(row, table, pk));

                                var count = (long)(await existsCmd.ExecuteScalarAsync() ?? 0L);

                                if (count > 0)
                                {
                                    var setClauses = table.Columns
                                        .Where(c => !pks.Contains(c.Master))
                                        .Select(c => $"{c.Master} = @{c.Master}");
                                    await using var updCmd = new NpgsqlCommand(
                                        $"UPDATE {table.MasterTable} SET {string.Join(", ", setClauses)} WHERE {pkCondition}", pgConn);
                                    foreach (var col in table.Columns)
                                        updCmd.Parameters.AddWithValue($"@{col.Master}", GetRowValue(row, table, col.Master));
                                    await updCmd.ExecuteNonQueryAsync();
                                }
                                else
                                {
                                    var cols = table.Columns.Select(c => c.Master).ToList();
                                    var colList = string.Join(", ", cols);
                                    var paramList = string.Join(", ", cols.Select(c => $"@{c}"));
                                    await using var insCmd = new NpgsqlCommand(
                                        $"INSERT INTO {table.MasterTable} ({colList}) VALUES ({paramList})", pgConn);
                                    foreach (var col in table.Columns)
                                        insCmd.Parameters.AddWithValue($"@{col.Master}", GetRowValue(row, table, col.Master));
                                    await insCmd.ExecuteNonQueryAsync();
                                }
                            }
                            else if (operation == "DELETE")
                            {
                                var pkCondition = string.Join(" AND ", pks.Select(pk => $"{pk} = @{pk}"));
                                await using var delCmd = new NpgsqlCommand(
                                    $"DELETE FROM {table.MasterTable} WHERE {pkCondition}", pgConn);
                                foreach (var pk in pks)
                                    delCmd.Parameters.AddWithValue($"@{pk}", GetRowValue(row, table, pk));
                                await delCmd.ExecuteNonQueryAsync();
                            }
                            rows++;
                        }
                        catch (Exception exRow)
                        {
                            _logger.LogWarning(exRow, "SYNC-OUT fila ignorada en {Table}: {Op}", table.Name, operation);
                        }
                    }

                    await using (var truncCmd = new SqlCommand($"DELETE FROM {table.LogTable}", sqlConn))
                        await truncCmd.ExecuteNonQueryAsync();

                    result.Success = true;
                    result.RowsAffected = rows;
                    result.Message = $"{rows} cambios aplicados al MASTER.";
                }
                catch (Exception ex)
                {
                    result.Success = false;
                    result.Message = ex.Message;
                    _logger.LogError(ex, "SYNC-OUT error en tabla {Table}", table.Name);
                }

                results.Add(result);
            }

            return results;
        }

        private object GetRowValue(DataRow row, TableMap table, string masterCol)
        {
            var map = table.Columns.FirstOrDefault(c => c.Master == masterCol);
            var colName = map?.Slave ?? masterCol;

            if (row.Table.Columns.Contains(colName))
            {
                var val = row[colName];
                return val == DBNull.Value ? DBNull.Value : val;
            }
            if (row.Table.Columns.Contains(masterCol))
            {
                var val = row[masterCol];
                return val == DBNull.Value ? DBNull.Value : val;
            }
            return DBNull.Value;
        }
    }
}