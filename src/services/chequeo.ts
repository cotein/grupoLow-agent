import * as ExcelJS_ from 'exceljs';
const ExcelJS = (ExcelJS_ as any).default || ExcelJS_;

import * as pg from 'pg';
const { Pool } = pg;
import 'dotenv/config';

// Tipado para los resultados de auditoría
interface AuditTotals {
    importe: number;
    descuento: number;
    neto: number;
    pr_costo_uni_neto: number;
    facturacion: number;
    cmv: number;
    d1: number;
    d2: number;
    count: number;
}

const CONFIG = {
    filePath: '/home/coto/Github/Kaiahub.ar/grupoLow-demo/det comp total.xlsx',
    tableName: 'ventas_detalle',
    connectionString: process.env.DATABASE_URL
};

async function auditData() {
    console.log('🔍 Iniciando auditoría de integridad...');
    const pool = new Pool({ connectionString: CONFIG.connectionString, ssl: { rejectUnauthorized: false } });

    try {
        // 1. Calcular totales desde el EXCEL
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.readFile(CONFIG.filePath);
        const worksheet = workbook.getWorksheet(1);
        
        const excelTotals: AuditTotals = {
            importe: 0, descuento: 0, neto: 0, pr_costo_uni_neto: 0,
            facturacion: 0, cmv: 0, d1: 0, d2: 0, count: 0
        };

        worksheet?.eachRow((row, rowNumber) => {
            if (rowNumber <= 1) return; // Saltar cabecera
            const v = row.values as any[];
            
            // Usamos la misma lógica de índices que tu importador para ser consistentes
            if (v[16]) { // idunica check
                excelTotals.importe += parseFloat(v[7]) || 0;
                excelTotals.descuento += parseFloat(v[10]) || 0;
                excelTotals.neto += parseFloat(v[13]) || 0;
                excelTotals.pr_costo_uni_neto += parseFloat(v[19]) || 0;
                excelTotals.facturacion += parseFloat(v[22]) || 0;
                excelTotals.cmv += parseFloat(v[23]) || 0;
                excelTotals.d1 += parseFloat(v[24]) || 0;
                excelTotals.d2 += parseFloat(v[25]) || 0;
                excelTotals.count++;
            }
        });

        // 2. Calcular totales desde SUPABASE (PostgreSQL)
        // Redondeamos a 2 decimales para evitar el ruido del punto flotante
        const dbQuery = `
            SELECT 
                COUNT(*) as count,
                SUM(importe)::NUMERIC as importe,
                SUM(descuento)::NUMERIC as descuento,
                SUM(neto)::NUMERIC as neto,
                SUM(pr_costo_uni_neto)::NUMERIC as pr_costo_uni_neto,
                SUM(facturacion)::NUMERIC as facturacion,
                SUM(cmv)::NUMERIC as cmv,
                SUM(d1)::NUMERIC as d1,
                SUM(d2)::NUMERIC as d2
            FROM ${CONFIG.tableName}
        `;
        const { rows } = await pool.query(dbQuery);
        const dbTotals = rows[0];

        // 3. Comparación y reporte
        console.log('\n📊 REPORTE DE CONCORDANCIA:');
        const fields: (keyof AuditTotals)[] = ['count', 'importe', 'descuento', 'neto', 'pr_costo_uni_neto', 'facturacion', 'cmv', 'd1', 'd2'];
        
        const report = fields.map(field => {
            const excelVal = Number(excelTotals[field]);
            const dbVal = Number(dbTotals[field]);
            const diff = excelVal - dbVal;
            return {
                Campo: field.toUpperCase(),
                Excel: excelVal.toLocaleString('es-AR', { minimumFractionDigits: 2 }),
                Supabase: dbVal.toLocaleString('es-AR', { minimumFractionDigits: 2 }),
                Diferencia: diff.toFixed(4),
                Estado: Math.abs(diff) < 0.01 ? '✅ OK' : '❌ ERROR'
            };
        });

        console.table(report);

    } catch (error) {
        console.error('❌ Error en auditoría:', error);
    } finally {
        await pool.end();
    }
}

auditData();