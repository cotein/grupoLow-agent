import { createTool } from '@mastra/core/tools';
import { z } from 'zod';
import { Pool } from 'pg';

import 'dotenv/config';

// Use a single connection pool for the tools
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

export const getHealthMetrics = createTool({
    id: 'getHealthMetrics',
    description: 'Calcula indicadores de salud financiera: Ingresos Totales (Ventas Netas), CMV (Costo de Mercadería Vendida), Utilidad Bruta, y Margen Bruto %. Excluye muestras gratis (descuento 100%).',
    inputSchema: z.object({
        startDate: z.string().describe('Fecha de inicio en formato YYYY-MM-DD. Opcional.').optional(),
        endDate: z.string().describe('Fecha de fin en formato YYYY-MM-DD. Opcional.').optional(),
    }),
    execute: async (data: any) => {
        let query = `
            SELECT 
                SUM(importe) as ingresos_totales,
                SUM(cantidad * pr_costo_uni_neto) as cmv_total,
                SUM(importe) - SUM(cantidad * pr_costo_uni_neto) as utilidad_bruta,
                ((SUM(importe) - SUM(cantidad * pr_costo_uni_neto)) / NULLIF(SUM(importe), 0)) * 100 as margen_bruto_porcentaje
            FROM ventas_detalle
            WHERE descuento < 100  -- Excluir muestras gratis
        `;
        const params: any[] = [];
        if (data.startDate) {
            params.push(data.startDate);
            query += ` AND fecha >= $${params.length}`;
        }
        if (data.endDate) {
            params.push(data.endDate);
            query += ` AND fecha <= $${params.length}`;
        }
        
        try {
            const res = await pool.query(query, params);
            return res.rows[0];
        } catch (error) {
            console.error(error);
            return { error: 'Error ejecutando la consulta SQL de health metrics' };
        }
    }
});

export const getRFMAnalysis = createTool({
    id: 'getRFMAnalysis',
    description: 'Genera un análisis RFM (Recency, Frequency, Monetary) de los clientes para segmentar la base de datos basándose en comportamiento real.',
    inputSchema: z.object({
        limit: z.number().describe('Límitar la cantidad de clientes devueltos. Por defecto 50.').optional(),
    }),
    execute: async (data: any) => {
        const query = `
            SELECT 
                cliente as cliente_id,
                MAX(razon_social) as cliente_nombre,
                EXTRACT(DAY FROM (CURRENT_DATE - MAX(fecha))) as recencia_dias,
                COUNT(DISTINCT idunica) as frecuencia_compras,
                SUM(importe) as valor_monetario
            FROM ventas_detalle
            WHERE cliente IS NOT NULL AND importe > 0
            GROUP BY cliente
            ORDER BY valor_monetario DESC
            LIMIT $1
        `;
        const limit = data.limit || 50;
        
        try {
            const res = await pool.query(query, [limit]);
            return res.rows;
        } catch (error) {
            console.error(error);
            return { error: 'Error ejecutando análisis RFM' };
        }
    }
});

export const getBreakevenPoint = createTool({
    id: 'getBreakevenPoint',
    description: 'Calcula el Punto de Equilibrio en Pesos. Indica cuánto se debe vender para que la utilidad operativa sea cero. Requiere proveer los costos fijos.',
    inputSchema: z.object({
        costosFijos: z.number().describe('El valor monetario de los costos fijos (ej. sueldos, alquileres).'),
        startDate: z.string().describe('Fecha inicio YYYY-MM-DD. Opcional.').optional(),
        endDate: z.string().describe('Fecha fin YYYY-MM-DD. Opcional.').optional(),
    }),
    execute: async (data: any) => {
        let query = `
            SELECT 
                SUM(importe) as ventas_totales,
                SUM(cantidad * pr_costo_uni_neto) as cmv_total
            FROM ventas_detalle
            WHERE descuento < 100
        `;
        const params: any[] = [];
        if (data.startDate) {
            params.push(data.startDate);
            query += ` AND fecha >= $${params.length}`;
        }
        if (data.endDate) {
            params.push(data.endDate);
            query += ` AND fecha <= $${params.length}`;
        }

        try {
            const res = await pool.query(query, params);
            if (res.rows.length === 0) return { error: 'Sin datos en ese periodo' };
            
            const ventas = parseFloat(res.rows[0].ventas_totales) || 0;
            const cmv = parseFloat(res.rows[0].cmv_total) || 0;
            
            if (ventas === 0) return { error: 'Las ventas totales son cero, no se puede calcular' };
            
            const puntoEquilibrio = data.costosFijos / (1 - (cmv / ventas));
            
            return {
                punto_equilibrio_pesos: puntoEquilibrio,
                costos_fijos_ingresados: data.costosFijos,
                ventas_periodo: ventas,
                cmv_periodo: cmv
            };
        } catch (error) {
            console.error(error);
            return { error: 'Error calculando Punto de Equilibrio' };
        }
    }
});

export const getAdditionalKPIs = createTool({
    id: 'getAdditionalKPIs',
    description: 'Calcula KPIs estratégicos: Ticket Promedio, Índice de Devoluciones (% de la venta que se cae) y Riesgo de Concentración Pareto (% de margen del top 20% de clientes).',
    inputSchema: z.object({
        startDate: z.string().optional(),
        endDate: z.string().optional()
    }),
    execute: async (data: any) => {
        const params: any[] = [];
        let dateFilter = '';
        if (data.startDate) {
            params.push(data.startDate);
            dateFilter += ` AND fecha >= $${params.length}`;
        }
        if (data.endDate) {
            params.push(data.endDate);
            dateFilter += ` AND fecha <= $${params.length}`;
        }

        const ticketQuery = `
            SELECT SUM(importe) / COUNT(DISTINCT idunica) as ticket_promedio 
            FROM ventas_detalle 
            WHERE importe > 0 ${dateFilter}
        `;
        
        const devQuery = `
            SELECT 
                ABS(SUM(CASE WHEN importe < 0 THEN importe ELSE 0 END)) / NULLIF(SUM(CASE WHEN importe > 0 THEN importe ELSE 0 END), 0) * 100 as indice_devoluciones_pct
            FROM ventas_detalle
            WHERE 1=1 ${dateFilter}
        `;

        const paretoQuery = `
            WITH VentasClientes AS (
                SELECT cliente, SUM(importe - (cantidad * pr_costo_uni_neto)) as margen
                FROM ventas_detalle
                WHERE cliente IS NOT NULL ${dateFilter}
                GROUP BY cliente
            ),
            Totales AS (
                SELECT SUM(margen) as margen_total, COUNT(*) as clientes_totales FROM VentasClientes WHERE margen > 0
            ),
            TopClientes AS (
                SELECT margen
                FROM VentasClientes
                WHERE margen > 0
                ORDER BY margen DESC
                LIMIT GREATEST(1, (SELECT clientes_totales FROM Totales) * 0.20)
            )
            SELECT (SUM(t.margen) / NULLIF((SELECT margen_total FROM Totales), 0)) * 100 as concentracion_pareto_20_pct
            FROM TopClientes t
        `;

        try {
            const [ticketRes, devRes, paretoRes] = await Promise.all([
                pool.query(ticketQuery, params),
                pool.query(devQuery, params),
                pool.query(paretoQuery, params)
            ]);

            return {
                ticket_promedio: ticketRes.rows[0]?.ticket_promedio,
                indice_devoluciones_pct: devRes.rows[0]?.indice_devoluciones_pct,
                concentracion_pareto_20_pct: paretoRes.rows[0]?.concentracion_pareto_20_pct
            };
        } catch (error) {
            console.error(error);
            return { error: 'Error calculando KPIs adicionales' };
        }
    }
});

export const getSalesAggregations = createTool({
    id: 'getSalesAggregations',
    description: 'Agrupa las ventas, márgenes y rentabilidad por una dimensión específica (ej. vendedor, rubro, articulo, cliente, proveedor/descripcion, etc).',
    inputSchema: z.object({
        groupBy: z.enum(['cod_ven', 'rubro', 'articulo', 'cliente', 'descripcion', 'subcanal', 'segmentoproducto', 'linea']).describe('La dimensión por la cual agrupar los datos.'),
        limit: z.number().describe('La cantidad máxima de resultados a retornar. Por defecto 20.').optional(),
        startDate: z.string().describe('Fecha inicio YYYY-MM-DD. Opcional.').optional(),
        endDate: z.string().describe('Fecha fin YYYY-MM-DD. Opcional.').optional(),
        orderBy: z.enum(['ventas', 'margen', 'cantidad']).describe('Por qué métrica ordenar. Por defecto "ventas".').optional()
    }),
    execute: async (data: any) => {
        const params: any[] = [];
        let dateFilter = '';
        
        if (data.startDate) {
            params.push(data.startDate);
            dateFilter += ` AND fecha >= $${params.length}`;
        }
        if (data.endDate) {
            params.push(data.endDate);
            dateFilter += ` AND fecha <= $${params.length}`;
        }

        const groupByCol = data.groupBy;
        let orderByClause = 'ventas_totales_brutas DESC';
        if (data.orderBy === 'margen') orderByClause = 'margen_total DESC';
        if (data.orderBy === 'cantidad') orderByClause = 'cantidad_total DESC';

        const limit = data.limit || 20;
        params.push(limit);

        const query = `
            SELECT 
                ${groupByCol} as categoria,
                COUNT(DISTINCT cliente) as clientes_compradores,
                SUM(cantidad) as cantidad_total,
                SUM(importe) as ventas_totales_brutas,
                SUM(importe) as importe_neta,
                SUM(d1) as total_descuentos_pesos,
                (SUM(d1) / NULLIF(SUM(importe), 0)) * 100 as porcentaje_descuentos_sobre_ventas,
                SUM(importe) - SUM(cantidad * pr_costo_uni_neto) as margen_total,
                ((SUM(importe) - SUM(cantidad * pr_costo_uni_neto)) / NULLIF(SUM(importe), 0)) * 100 as margen_porcentaje
            FROM ventas_detalle
            WHERE ${groupByCol} IS NOT NULL ${dateFilter}
            GROUP BY ${groupByCol}
            ORDER BY ${orderByClause}
            LIMIT $${params.length}
        `;

        try {
            const res = await pool.query(query, params);
            return res.rows;
        } catch (error) {
            console.error(error);
            return { error: 'Error calculando agregaciones de ventas' };
        }
    }
});



