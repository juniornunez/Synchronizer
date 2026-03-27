# Pagila Synchronizer

**Estudiante:** Junior Nuñez - 22411198
**Curso:** Teoría de Base de Datos II
**Catedrático:** Ing. Elvin Deras
**MASTER:** PostgreSQL (Pagila)
**SLAVE:** Microsoft SQL Server
**Lenguaje:** C# (.NET)
**Tipo:** Aplicación Web (ASP.NET Core MVC)

---

## Descripción

Herramienta de sincronización de datos entre dos motores de bases de datos heterogéneos bajo un modelo de replicación selectiva Master-Slave. El MASTER es una base de datos PostgreSQL con el esquema Pagila y el SLAVE es una base de datos en SQL Server que mantiene una estructura compatible. La sincronización se implementa mediante Shadow Tables y Triggers para la captura de cambios, y un archivo mapping.json para el mapeo de tablas y columnas entre motores sin hardcodear nada en el código.

---

## Arquitectura del Sistema

```
PostgreSQL (MASTER)              SQL Server (SLAVE)
      Pagila               ←→       pagila_slave
        |                                 |
        |          ASP.NET Core           |
        |        PagilaSynchronizer       |
        |                                 |
    Sync-IN                           Sync-OUT
MASTER → SLAVE                    SLAVE → MASTER
```

---

## Clasificación de Tablas

### Tablas IN (MASTER → SLAVE)

Representan el catálogo maestro. El SLAVE solo recibe estos datos y tiene prohibido modificarlos.

| Tabla | Descripción |
|-------|-------------|
| language | Idiomas registrados |
| actor | Listado de actores |
| category | Categorías de películas |
| film | Catálogo de películas |
| film_actor | Relación películas y actores |
| film_category | Relación películas y categorías |
| country | Listado de países |
| city | Listado de ciudades |
| address | Direcciones físicas |
| store | Sucursales |
| staff | Personal |
| inventory | Inventario de películas por sucursal |

### Tablas OUT (SLAVE → MASTER)

Representan la actividad transaccional generada localmente en el SLAVE.

| Tabla | Tabla Log |
|-------|-----------|
| customer | customer_log |
| rental | rental_log |
| payment | payment_log |

---

## Características Implementadas

### 1. Shadow Tables y Triggers

Por cada tabla OUT se creó una tabla `_log` en el SLAVE. Se implementó un trigger en cada tabla OUT que captura las operaciones de INSERT, UPDATE y DELETE registrando el tipo de operación, la fecha y los datos afectados.

### 2. Archivo mapping.json

La sincronización no está hardcodeada. El archivo define las credenciales de conexión de ambos motores y el mapeo completo de tablas y columnas entre MASTER y SLAVE, permitiendo flexibilidad si los nombres difieren entre motores.

### 3. Proceso Sync-IN

Descarga los datos del MASTER hacia el SLAVE. Deshabilita las foreign keys, borra los datos existentes, reinsertan en orden correcto respetando la integridad referencial y vuelve a habilitar las constraints. Maneja la conversión de tipos de datos entre PostgreSQL y SQL Server.

### 4. Proceso Sync-OUT

Lee los registros acumulados en las tablas `_log` del SLAVE y aplica cada operación en el MASTER verificando si debe hacer INSERT o UPDATE según si el registro ya existe. Una vez confirmada la subida exitosa limpia las tablas log.

### 5. Dashboard de Monitoreo

Interfaz web que muestra el estado de conexión de ambos motores, botones para ejecutar manualmente los procesos Sync-IN y Sync-OUT, y una tabla de log con el resultado de cada operación ejecutada incluyendo tabla afectada, filas procesadas y mensajes de error si los hay.

---

## Clases Principales

**MappingService**
Lee el mapping.json al arrancar la aplicación y lo convierte en objetos C#. Contiene los modelos ColumnMap, TableMap, MasterConfig, SlaveConfig y SyncMapping.
Métodos: GetMasterConnectionString(), GetSlaveConnectionString()

**SyncService**
Contiene toda la lógica de sincronización entre los dos motores.
Métodos públicos: SyncInAsync(), SyncOutAsync()
Métodos privados: DeshabilitarConstraints(), BorrarTablas(), HabilitarConstraints(), ObtenerValor()

**SyncController**
Expone la lógica del SyncService como endpoints HTTP para el dashboard.
Endpoints: GET /Sync/Status, POST /Sync/SyncIn, POST /Sync/SyncOut

---

## Requisitos del Sistema

- Windows 10 o superior
- .NET 10.0 Runtime
- SQL Server 2016 o superior
- Docker Desktop

---

## Instalación

```bash
git clone https://github.com/juniornunez/Synchronizer.git
cd Synchronizer
```

Levantar el MASTER con Docker:
```bash
cd docker
docker-compose up -d
```

Crear la base de datos SLAVE en SQL Server y ejecutar el script:
```sql
CREATE DATABASE pagila_slave;
```
Correr `01_slave_schema.sql` en SSMS sobre `pagila_slave`.

Correr la aplicación:
```bash
cd PagilaSynchronizer/PagilaSynchronizer
dotnet run
```

---

## Estructura del Proyecto

```
Proyecto2_TBD2/
├── docker/
│   ├── docker-compose.yml
│   └── pagila-schema.sql
├── PagilaSynchronizer/
│   └── PagilaSynchronizer/
│       ├── Controllers/
│       │   └── SyncController.cs
│       ├── Services/
│       │   ├── MappingService.cs
│       │   └── SyncService.cs
│       ├── Views/
│       │   └── Sync/
│       │       └── Index.cshtml
│       ├── mapping.json
│       └── Program.cs
└── 01_slave_schema.sql
```

---

## Estado del Proyecto

Todos los entregables solicitados en los lineamientos están completos:

| Entregable | Estado |
|------------|--------|
| Esquema SLAVE en SQL Server | Completo |
| Scripts de Triggers y Shadow Tables | Completo |
| Código de sincronización y mapping.json | Completo |
| Dashboard de monitoreo | Completo |
