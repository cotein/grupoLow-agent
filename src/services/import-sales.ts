import * as ExcelJS_ from 'exceljs';
const ExcelJS = (ExcelJS_ as any).default || ExcelJS_;

import * as pg from 'pg';
const { Pool } = pg;
import 'dotenv/config';

// Configuration
const CONFIG = {
    filePath: '/home/coto/Github/Kaiahub.ar/grupoLow-demo/det comp total.xlsx',
    tableName: 'ventas_detalle',
    batchSize: 100,
    connectionString: process.env.DATABASE_URL
};

async function importSales() {
    console.log('🚀 Starting Exact Sales Import (35+ Columns)...');
    
    const pool = new Pool({
        connectionString: CONFIG.connectionString,
        ssl: { rejectUnauthorized: false }
    });

    pool.on('error', (err) => {
        console.error('⚠️ Unexpected error on idle client:', err);
    });

    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(CONFIG.filePath);
        const worksheet = workbook.getWorksheet(1);
        if (!worksheet) {
            throw new Error('❌ No se encontró la primera hoja del Excel.');
        }
        
        // Log Headers
        const headerRow = worksheet.getRow(1);
        console.log('📋 Excel Headers detected:', headerRow.values);

        const totalRows = worksheet.rowCount - 1; // Excluding headers
        console.log(`📊 Total rows to process: ${totalRows}`);

        let batch: any[] = [];
        let importedCount = 0;
        let totalInserted = 0;
        let totalConflicts = 0;
        let totalSkippedMissingId = 0;

        // Start from row 2 (headers are row 1)
        for (let i = 2; i <= worksheet.rowCount; i++) {
            const row = worksheet.getRow(i);
            const values = row.values as any[];
            
            // Exact mapping based on analysis in step 395
            const data = {
                cliente: values[1],
                direccion: values[2],
                fecha: toLocalDate(values[3]),
                comprobante: values[4],
                art: values[5],
                cantidad: parseFloat(values[6]) || 0,
                importe: parseFloat(values[7]) || 0,
                razon_social: values[8],
                motivodev: values[9],
                descuento: parseFloat(values[10]) || 0,
                cod_ven: values[11],
                articulo: values[12],
                neto: parseFloat(values[13]) || 0,
                camion: values[14],
                comentario: values[15],
                idunica: values[16],
                subcanal: values[17],
                reparto: values[18],
                pr_costo_uni_neto: parseFloat(values[19]) || 0,
                chofer: values[20],
                valordesc: parseFloat(values[21]) || 0,
                facturacion: parseFloat(values[22]) || 0,
                cmv: parseFloat(values[23]) || 0,
                d1: parseFloat(values[24]) || 0,
                d2: parseFloat(values[25]) || 0,
                peso: parseFloat(values[26]) || 0,
                rubro: values[27],
                descripcion: values[28],
                capacidad_art: parseFloat(values[29]) || 0,
                usuariopicking: values[30],
                nombrepicking: values[31],
                tipov: values[32],
                segmentoproducto: values[33],
                linea: values[34],
                fecha_pedido: toLocalDate(values[35]),  
            };

            if (data.idunica) {
                batch.push(data);
            } else {
                totalSkippedMissingId++;
                console.log(`⚠️ Skipped row ${i}: Missing idunica (Comprobante: ${data.comprobante}, Artículo: ${data.articulo})`);
            }

            if (batch.length >= CONFIG.batchSize || i === worksheet.rowCount) {
                if (batch.length > 0) {
                    const results = await upsertBatch(pool, batch);
                    totalInserted += results.inserted;
                    totalConflicts += results.conflicts;
                    importedCount += batch.length;
                    console.log(`✅ Progress: ${importedCount}/${totalRows} rows processed. (Batch: ${results.inserted} new, ${results.conflicts} existing)`);
                    batch = [];
                }
            }
        }

        console.log('\n✨ Import finished successfully!');
        console.log('-------------------------------------------');
        console.log(`📊 TOTAL SUMMARY:`);
        console.log(`✅ Total Inserted (New): ${totalInserted}`);
        console.log(`🔁 Total Existing (Conflicts/Skipped): ${totalConflicts}`);
        console.log(`⚠️ Total Missing idunica (Skipped): ${totalSkippedMissingId}`);
        console.log(`📈 Grand Total Processed: ${importedCount + totalSkippedMissingId}`);
        console.log('-------------------------------------------');

    } catch (error) {
        console.error('❌ Error during import:', error);
    } finally {
        await pool.end();
    }
}

async function upsertBatch(pool: pg.Pool, batch: any[]) {
    const client = await pool.connect();
    let inserted = 0;
    let conflicts = 0;

    try {
        for (const record of batch) {
            const query = `
                INSERT INTO ${CONFIG.tableName} (
                    idunica, cliente, direccion, fecha, comprobante, art, cantidad, importe, 
                    razon_social, motivodev, descuento, cod_ven, articulo, neto, camion, 
                    comentario, subcanal, reparto, pr_costo_uni_neto, chofer, valordesc, 
                    facturacion, cmv, d1, d2, peso, rubro, descripcion, capacidad_art, 
                    usuariopicking, nombrepicking, tipov, segmentoproducto, linea, fecha_pedido
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35)
                ON CONFLICT (comprobante, articulo) DO NOTHING
                RETURNING 1;
            `;
            
            const values = [
                record.idunica, record.cliente, record.direccion, record.fecha, record.comprobante, 
                record.art, record.cantidad, record.importe, record.razon_social, record.motivodev, 
                record.descuento, record.cod_ven, record.articulo, record.neto, record.camion, 
                record.comentario, record.subcanal, record.reparto, record.pr_costo_uni_neto, 
                record.chofer, record.valordesc, record.facturacion, record.cmv, record.d1, 
                record.d2, record.peso, record.rubro, record.descripcion, record.capacidad_art, 
                record.usuariopicking, record.nombrepicking, record.tipov, record.segmentoproducto, 
                record.linea, record.fecha_pedido
            ];
            
            try {
                const res = await client.query(query, values);
                if (res.rowCount && res.rowCount > 0) {
                    inserted++;
                } else {
                    conflicts++;
                }
            } catch (err: any) {
                console.error(`⚠️ Fila omitida. No se pudo insertar (Comprobante: ${record.comprobante}, Artículo: ${record.articulo}). Razón: ${err.message}`);
            }
        }
        return { inserted, conflicts };
    } catch (e: any) {
        console.error('❌ Error de conexión en el lote:', e.message);
        throw e;
    } finally {
        client.release();
    }
}

// Función helper para corregir el desfase
function toLocalDate(value: any): string | null {
    if (!value) return null;
    const d = value instanceof Date ? value : new Date(value);
    const year = d.getUTCFullYear();
    const month = String(d.getUTCMonth() + 1).padStart(2, '0');
    const day = String(d.getUTCDate()).padStart(2, '0');
    return `${year}-${month}-${day}`; // "2026-02-02"
}

importSales();
