import pkg from 'exceljs';
const { Workbook } = pkg;
import { createClient } from '@supabase/supabase-js';
import 'dotenv/config';

// Cliente con Service Role Key para saltar cualquier restricción y ser más rápido
const supabase = createClient(
    process.env.SUPABASE_URL!, 
    process.env.SUPABASE_SERVICE_ROLE_KEY!
);

async function onlyInsertSales() {
    console.log('🚀 Iniciando Inserción Directa (Sin Upsert)...');
    console.time('⏱️ Tiempo de ejecución');

    try {
        const workbook = new Workbook();
        await workbook.xlsx.readFile('/home/coto/Github/Kaiahub.ar/grupoLow-demo/det comp total.xlsx');
        const worksheet = workbook.getWorksheet(1);
        
        if (!worksheet) throw new Error('❌ Hoja de Excel no encontrada');

        const rowsToInsert: any[] = [];

        // Mapeo de las 35 columnas
        worksheet.eachRow((row: any, rowNumber: number) => {
            if (rowNumber === 1) return; // Saltar cabecera

            const v = row.values as any[];
            
            // Insertamos TODO lo que venga, tal cual
            rowsToInsert.push({
                cliente: v[1],
                direccion: v[2],
                fecha: formatDate(v[3]),
                comprobante: v[4],
                art: v[5],
                cantidad: parseNumeric(v[6]),
                importe: parseNumeric(v[7]), // Mantenemos los 4 decimales
                razon_social: v[8],
                motivodev: v[9],
                descuento: parseNumeric(v[10]),
                cod_ven: v[11],
                articulo: v[12],
                neto: parseNumeric(v[13]),
                camion: v[14],
                comentario: v[15],
                idunica: v[16] ? v[16].toString() : null,
                subcanal: v[17],
                reparto: v[18],
                pr_costo_uni_neto: parseNumeric(v[19]),
                chofer: v[20],
                valordesc: parseNumeric(v[21]),
                facturacion: parseNumeric(v[22]),
                cmv: parseNumeric(v[23]),
                d1: parseNumeric(v[24]),
                d2: parseNumeric(v[25]),
                peso: parseNumeric(v[26]),
                rubro: v[27],
                descripcion: v[28],
                capacidad_art: parseNumeric(v[29]),
                usuariopicking: v[30],
                nombrepicking: v[31],
                tipov: v[32],
                segmentoproducto: v[33],
                linea: v[34],
                fecha_pedido: formatDate(v[35])
            });
        });

        console.log(`📦 Filas totales a insertar: ${rowsToInsert.length}`);

        // Insertar en lotes de 1000 para no saturar la red, pero usando .insert()
        for (let i = 0; i < rowsToInsert.length; i += 1000) {
            const batch = rowsToInsert.slice(i, i + 1000);
            
            // CAMBIO CLAVE: .insert() puro, sin On Conflict
            const { error } = await supabase
                .from('ventas_detalle')
                .insert(batch);

            if (error) {
                console.error(`❌ Error en lote ${i/1000}:`, error.message);
            } else {
                console.log(`✅ Lote ${i/1000 + 1} insertado (${Math.min(i + 1000, rowsToInsert.length)}/${rowsToInsert.length})`);
            }
        }

        console.timeEnd('⏱️ Tiempo de ejecución');
        console.log('✨ Inserción terminada.');

    } catch (error) {
        console.error('❌ Error crítico:', error);
    }
}

function parseNumeric(val: any): number {
    if (val === null || val === undefined) return 0;
    const n = typeof val === 'object' ? val.result || val.value : val;
    return parseFloat(n) || 0;
}

function formatDate(val: any): string | null {
    if (!val) return null;
    const d = new Date(val);
    // Usamos formato YYYY-MM-DD para evitar problemas de zona horaria [cite: 2, 64, 1811]
    return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
}

onlyInsertSales();