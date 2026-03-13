/**
 * ventas-etl.ts
 *
 * Servicio ETL: lee un Excel con estructura det_comp_total y persiste
 * los datos en PostgreSQL (schema schema_ventas.sql) usando chunks.
 *
 * Dependencias:  npm install exceljs pg
 * Ejecución:     npx tsx ventas-etl.ts ./archivo.xlsx
 *                DB_URL=postgres://user:pass@host:5432/db npx tsx ventas-etl.ts ./archivo.xlsx
 */

import ExcelJS from "exceljs";
import { Pool, PoolClient } from "pg";
import path from "path";

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURACIÓN
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  // Ajustar según RAM disponible. 500 filas ≈ ~2 MB por chunk
  chunkSize: parseInt(process.env.CHUNK_SIZE ?? "500"),
  maxRetries: parseInt(process.env.MAX_RETRIES ?? "3"),
  db: {
    connectionString: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/postgres",
    max: 5,
    idleTimeoutMillis: 30_000,
    connectionTimeoutMillis: 5_000,
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// TIPOS
// ─────────────────────────────────────────────────────────────────────────────

interface ExcelRow {
  Cliente: number;
  Direccion: string | null;
  Fecha: Date | null;
  Comprobante: string;
  Art: number;
  Cantidad: number;
  Importe: number;
  Razon_Social: string | null;
  motivodev: number | null;
  descuento: number;
  cod_ven: number;
  articulo: string | null;
  neto: number;
  camion: number;
  IDUNICA: string | null;
  subcanal: string | null;
  reparto: number;
  pr_costo_uni_neto: number;
  chofer: number;
  valordesc: number;
  facturacion: number;
  CMV: number;
  d1: number;
  d2: number;
  peso: number;
  rubro: number;
  descripcion: string | null;
  capacidad_art: number | null;
  tipoV: string | null;
  segmentoproducto: string | null;
  linea: string | null;
  fecha_pedido: Date | null;
}

interface ETLResult {
  totalRows: number;
  insertedRows: number;
  skippedRows: number;
  failedChunks: number;
  durationMs: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────────────────────────────────────

const log = {
  info:  (msg: string, meta?: object) => console.log (`[INFO]  ${new Date().toISOString()} ${msg}`, meta ?? ""),
  warn:  (msg: string, meta?: object) => console.warn(`[WARN]  ${new Date().toISOString()} ${msg}`, meta ?? ""),
  error: (msg: string, meta?: object) => console.error(`[ERROR] ${new Date().toISOString()} ${msg}`, meta ?? ""),
  debug: (msg: string, meta?: object) => {
    if (process.env.LOG_LEVEL === "debug")
      console.log(`[DEBUG] ${new Date().toISOString()} ${msg}`, meta ?? "");
  },
};

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS DE CONVERSIÓN
// ─────────────────────────────────────────────────────────────────────────────

function toNum(v: unknown, fallback = 0): number {
  if (v == null || v === "") return fallback;
  const n = Number(v);
  return isNaN(n) ? fallback : n;
}

function toInt(v: unknown, fallback = 0): number {
  return Math.round(toNum(v, fallback));
}

function toStr(v: unknown): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function toDate(v: unknown): Date | null {
  if (v == null || v === "") return null;
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v;
  const d = new Date(String(v));
  return isNaN(d.getTime()) ? null : d;
}

function chunks<T>(arr: T[], n: number): T[][] {
  const result: T[][] = [];
  for (let i = 0; i < arr.length; i += n) result.push(arr.slice(i, i + n));
  return result;
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ─────────────────────────────────────────────────────────────────────────────
// PASO 1 — LEER EXCEL
// ExcelJS lee el archivo completo pero cell por cell, sin parsear DOM XML
// Para 50k filas esto consume ~150–300 MB de RAM, que es manejable.
// ─────────────────────────────────────────────────────────────────────────────

async function readExcel(filePath: string): Promise<ExcelRow[]> {
  log.info(`Leyendo archivo: ${filePath}`);

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.readFile(filePath);

  const ws = wb.worksheets[0];
  if (!ws) throw new Error("El archivo no tiene hojas");

  // Primera fila = encabezados
  const headerRow = ws.getRow(1);
  const headers: string[] = [];
  headerRow.eachCell((cell) => headers.push(toStr(cell.value) ?? ""));

  const get = (cells: ExcelJS.Cell[], col: string): unknown => {
    const idx = headers.indexOf(col);
    return idx >= 0 ? cells[idx]?.value : undefined;
  };

  const rows: ExcelRow[] = [];

  ws.eachRow((row, rowIndex) => {
    if (rowIndex === 1) return; // saltar encabezado

    const cells: ExcelJS.Cell[] = [];
    row.eachCell({ includeEmpty: true }, (cell) => cells.push(cell));

    const comprobante = toStr(get(cells, "Comprobante")) ?? "";
    const cliente     = toInt(get(cells, "Cliente"));

    // Saltar filas vacías
    if (!comprobante || cliente === 0) return;

    rows.push({
      Cliente:           cliente,
      Direccion:         toStr(get(cells, "Direccion")),
      Fecha:             toDate(get(cells, "Fecha")),
      Comprobante:       comprobante,
      Art:               toInt(get(cells, "Art")),
      Cantidad:          toInt(get(cells, "Cantidad")),
      Importe:           toNum(get(cells, "Importe")),
      Razon_Social:      toStr(get(cells, "Razon_Social")),
      motivodev:         get(cells, "motivodev") != null ? toInt(get(cells, "motivodev")) : null,
      descuento:         toInt(get(cells, "descuento")),
      cod_ven:           toInt(get(cells, "cod_ven")),
      articulo:          toStr(get(cells, "articulo")),
      neto:              toNum(get(cells, "neto")),
      camion:            toInt(get(cells, "camion")),
      IDUNICA:           toStr(get(cells, "IDUNICA")),
      subcanal:          toStr(get(cells, "subcanal")),
      reparto:           toInt(get(cells, "reparto")),
      pr_costo_uni_neto: toNum(get(cells, "pr_costo_uni_neto")),
      chofer:            toInt(get(cells, "chofer")),
      valordesc:         toNum(get(cells, "valordesc")),
      facturacion:       toNum(get(cells, "facturacion")),
      CMV:               toNum(get(cells, "CMV")),
      d1:                toNum(get(cells, "d1")),
      d2:                toNum(get(cells, "d2")),
      peso:              toNum(get(cells, "peso")),
      rubro:             toInt(get(cells, "rubro")),
      descripcion:       toStr(get(cells, "descripcion")),
      capacidad_art:     get(cells, "capacidad_art") != null ? toNum(get(cells, "capacidad_art")) : null,
      tipoV:             toStr(get(cells, "tipoV")),
      segmentoproducto:  toStr(get(cells, "segmentoproducto")),
      linea:             toStr(get(cells, "linea")),
      fecha_pedido:      toDate(get(cells, "fecha_pedido")),
    });
  });

  log.info(`Filas válidas leídas: ${rows.length}`);
  return rows;
}

// ─────────────────────────────────────────────────────────────────────────────
// PASO 2 — UPSERT DE DIMENSIONES (una sola vez antes de los chunks de hechos)
// ─────────────────────────────────────────────────────────────────────────────

async function upsertDimensions(client: PoolClient, rows: ExcelRow[]): Promise<void> {
  log.info("Sincronizando tablas de dimensiones...");

  // Deduplicar en memoria antes de hacer queries
  const clientes  = new Map<number, ExcelRow>();
  const articulos = new Map<number, ExcelRow>();
  const vendedores = new Set<number>();
  const rutas     = new Map<number, ExcelRow>();

  for (const r of rows) {
    if (!clientes.has(r.Cliente))   clientes.set(r.Cliente, r);
    if (!articulos.has(r.Art))      articulos.set(r.Art, r);
    vendedores.add(r.cod_ven);
    if (!rutas.has(r.reparto))      rutas.set(r.reparto, r);
  }

  // dim_cliente
  for (const [id, r] of clientes) {
    await client.query(
      `INSERT INTO dim_cliente (cliente_id, razon_social, direccion, subcanal, segmento)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (cliente_id) DO UPDATE SET
         razon_social = EXCLUDED.razon_social,
         direccion    = COALESCE(EXCLUDED.direccion,   dim_cliente.direccion),
         subcanal     = COALESCE(EXCLUDED.subcanal,    dim_cliente.subcanal),
         segmento     = COALESCE(EXCLUDED.segmento,    dim_cliente.segmento),
         updated_at   = NOW()`,
      [id, r.Razon_Social, r.Direccion, r.subcanal, r.segmentoproducto]
    );
  }

  // dim_articulo
  for (const [id, r] of articulos) {
    await client.query(
      `INSERT INTO dim_articulo (art_id, nombre, linea, rubro, descripcion, capacidad_art, peso_unitario_g)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (art_id) DO UPDATE SET
         nombre          = COALESCE(EXCLUDED.nombre,          dim_articulo.nombre),
         linea           = COALESCE(EXCLUDED.linea,           dim_articulo.linea),
         rubro           = COALESCE(EXCLUDED.rubro,           dim_articulo.rubro),
         descripcion     = COALESCE(EXCLUDED.descripcion,     dim_articulo.descripcion),
         capacidad_art   = COALESCE(EXCLUDED.capacidad_art,   dim_articulo.capacidad_art),
         peso_unitario_g = COALESCE(EXCLUDED.peso_unitario_g, dim_articulo.peso_unitario_g),
         updated_at      = NOW()`,
      [id, r.articulo, r.linea, r.rubro, r.descripcion, r.capacidad_art, r.peso > 0 ? r.peso : null]
    );
  }

  // dim_vendedor
  for (const id of vendedores) {
    await client.query(
      `INSERT INTO dim_vendedor (cod_ven) VALUES ($1) ON CONFLICT (cod_ven) DO NOTHING`,
      [id]
    );
  }

  // dim_ruta
  for (const [id, r] of rutas) {
    await client.query(
      `INSERT INTO dim_ruta (reparto_id, camion, chofer_id)
       VALUES ($1, $2, $3)
       ON CONFLICT (reparto_id) DO UPDATE SET
         camion    = EXCLUDED.camion,
         chofer_id = EXCLUDED.chofer_id`,
      [id, r.camion, r.chofer]
    );
  }

  log.info(
    `Dimensiones: ${clientes.size} clientes, ${articulos.size} artículos, ` +
    `${vendedores.size} vendedores, ${rutas.size} rutas`
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// PASO 3 — INSERT DE HECHOS (chunk a chunk, una transacción por chunk)
// Usa multi-row INSERT para minimizar round-trips a la DB.
// ─────────────────────────────────────────────────────────────────────────────

// Columnas de fact_comprobante_detalle que insertamos
// (las columnas GENERATED ALWAYS AS y id/created_at las maneja Postgres sola)
const FACT_COLS = [
  "comprobante", "tipo_comprobante", "id_unica",
  "fecha_entrega", "fecha_pedido",
  "cliente_id", "art_id", "cod_ven", "reparto_id",
  "cantidad",
  "importe", "neto", "facturacion", "cmv", "pr_costo_uni_neto",
  "descuento_pct", "d1", "d2", "valordesc",
  "motivo_devolucion",
] as const;

type FactCols = typeof FACT_COLS;

function buildRowParams(r: ExcelRow): unknown[] {
  return [
    r.Comprobante,                               // comprobante
    r.Comprobante.substring(0, 3).toUpperCase(), // tipo_comprobante
    r.IDUNICA,                                   // id_unica
    r.Fecha,                                     // fecha_entrega
    r.fecha_pedido,                              // fecha_pedido
    r.Cliente,                                   // cliente_id
    r.Art,                                       // art_id
    r.cod_ven,                                   // cod_ven
    r.reparto,                                   // reparto_id
    r.Cantidad,                                  // cantidad
    r.Importe,                                   // importe
    r.neto,                                      // neto
    r.facturacion,                               // facturacion
    r.CMV,                                       // cmv
    r.pr_costo_uni_neto,                         // pr_costo_uni_neto
    r.descuento,                                 // descuento_pct
    r.d1,                                        // d1
    r.d2,                                        // d2
    r.valordesc,                                 // valordesc
    r.motivodev ?? null,                         // motivo_devolucion
  ];
}

async function insertChunk(
  client: PoolClient,
  chunk: ExcelRow[],
  chunkIndex: number
): Promise<number> {
  if (chunk.length === 0) return 0;

  const N = FACT_COLS.length;

  // Generar ($1,$2,...,$N), ($N+1,...,$2N), ...
  const valuePlaceholders = chunk
    .map((_, i) =>
      `(${Array.from({ length: N }, (_, j) => `$${i * N + j + 1}`).join(", ")})`
    )
    .join(",\n    ");

  const params: unknown[] = chunk.flatMap(buildRowParams);

  const sql = `
    INSERT INTO fact_comprobante_detalle (${FACT_COLS.join(", ")})
    VALUES
      ${valuePlaceholders}
    ON CONFLICT DO NOTHING`;

  const result = await client.query(sql, params);
  const inserted = result.rowCount ?? 0;
  log.debug(`Chunk ${chunkIndex}: ${inserted}/${chunk.length} filas insertadas`);
  return inserted;
}

// ─────────────────────────────────────────────────────────────────────────────
// ORQUESTADOR
// ─────────────────────────────────────────────────────────────────────────────

async function runETL(filePath: string): Promise<ETLResult> {
  const startMs = Date.now();
  const pool = new Pool(CONFIG.db);

  try {
    await pool.query("SELECT 1");
    log.info("Conexión a PostgreSQL: OK");
  } catch (err) {
    log.error("No se pudo conectar a PostgreSQL", { err });
    throw err;
  }

  // 1. Leer Excel
  const rows = await readExcel(filePath);
  if (rows.length === 0) throw new Error("El archivo no contiene filas válidas");

  // 2. Sincronizar dimensiones en una sola transacción
  {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await upsertDimensions(client, rows);
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      log.error("Error en dimensiones — rollback", { err });
      throw err;
    } finally {
      client.release();
    }
  }

  // 3. Insertar hechos chunk a chunk
  const rowChunks  = chunks(rows, CONFIG.chunkSize);
  const totalChunks = rowChunks.length;
  log.info(`Insertando ${rows.length} filas en ${totalChunks} chunks de ${CONFIG.chunkSize}`);

  let insertedRows = 0;
  let skippedRows  = 0;
  let failedChunks = 0;

  for (let i = 0; i < totalChunks; i++) {
    const chunk = rowChunks[i];
    let attempt = 0;
    let success = false;

    while (attempt < CONFIG.maxRetries && !success) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        const inserted = await insertChunk(client, chunk, i + 1);
        await client.query("COMMIT");

        insertedRows += inserted;
        skippedRows  += chunk.length - inserted; // filas ignoradas por ON CONFLICT DO NOTHING
        success = true;

        const pct = (((i + 1) / totalChunks) * 100).toFixed(1);
        log.info(`  [${pct}%] chunk ${i + 1}/${totalChunks} — +${inserted} insertadas`);
      } catch (err) {
        await client.query("ROLLBACK");
        attempt++;
        if (attempt < CONFIG.maxRetries) {
          const waitMs = 500 * attempt;
          log.warn(`  Chunk ${i + 1} falló (intento ${attempt}/${CONFIG.maxRetries}), reintentando en ${waitMs}ms`);
          await sleep(waitMs);
        } else {
          failedChunks++;
          log.error(`  Chunk ${i + 1} descartado tras ${CONFIG.maxRetries} intentos`, { err });
        }
      } finally {
        client.release();
      }
    }
  }

  await pool.end();

  return {
    totalRows: rows.length,
    insertedRows,
    skippedRows,
    failedChunks,
    durationMs: Date.now() - startMs,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// ENTRY POINT
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const filePath = process.argv[2];

  if (!filePath) {
    console.error("\nUso:  npx tsx ventas-etl.ts <ruta-al-archivo.xlsx>\n");
    console.error("Variables de entorno opcionales:");
    console.error("  DB_URL       postgres://user:pass@host:5432/db");
    console.error("  DB_HOST      (default: localhost)");
    console.error("  DB_PORT      (default: 5432)");
    console.error("  DB_NAME      (default: ventas)");
    console.error("  DB_USER      (default: postgres)");
    console.error("  DB_PASSWORD");
    console.error("  CHUNK_SIZE   filas por batch (default: 500)");
    console.error("  MAX_RETRIES  reintentos por chunk (default: 3)");
    console.error("  LOG_LEVEL    debug | info (default: info)\n");
    process.exit(1);
  }

  log.info(`Iniciando ETL`);
  log.info(`Archivo:    ${path.resolve(filePath)}`);
  log.info(`Chunk size: ${CONFIG.chunkSize} filas`);
  log.info(`Reintentos: ${CONFIG.maxRetries}`);

  try {
    const r = await runETL(path.resolve(filePath));

    const mins = Math.floor(r.durationMs / 60_000);
    const secs = ((r.durationMs % 60_000) / 1000).toFixed(1);

    console.log(`
─────────────────────────────────────────────
  ETL COMPLETADO
─────────────────────────────────────────────
  Filas leídas:     ${r.totalRows.toLocaleString()}
  Filas insertadas: ${r.insertedRows.toLocaleString()}
  Duplicados:       ${r.skippedRows.toLocaleString()}
  Chunks fallidos:  ${r.failedChunks}
  Duración:         ${mins > 0 ? `${mins}m ` : ""}${secs}s
─────────────────────────────────────────────
`);

    if (r.failedChunks > 0) {
      log.warn(`${r.failedChunks} chunk(s) no se pudieron insertar. Revisar logs arriba.`);
      process.exit(1);
    }
  } catch (err) {
    log.error("Error fatal en el ETL", { err });
    process.exit(1);
  }
}

