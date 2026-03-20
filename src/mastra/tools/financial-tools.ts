import { createTool } from '@mastra/core/tools';
import { z } from 'zod';
import { Pool } from 'pg';
import 'dotenv/config';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

const COLUMN_MAPPER: Record<string, string> = {
    vendedor: 'cod_ven',
    cliente: 'razon_social',
    articulo: 'articulo',
    rubro: 'rubro',
    subcanal: 'subcanal',
    linea: 'linea',
    chofer: 'chofer',
    segmento: 'segmentoproducto',
    fecha: 'DATE(fecha)',
};

/**
 * HELPER: Gestión de filtros de fecha para reutilización
 */
const getFechaFilters = (startDate?: string, endDate?: string, paramOffset = 0) => {
    const params: any[] = [];
    let querySnippet = '';
    if (startDate) {
        params.push(startDate);
        querySnippet += ` AND fecha >= $${params.length + paramOffset}`;
    }
    if (endDate) {
        params.push(endDate);
        querySnippet += ` AND fecha <= $${params.length + paramOffset}`;
    }
    return { params, querySnippet };
};

/**
 * TOOL 1: Salud Financiera Global
 * Optimizado: Solo métricas core.
 */
export const getHealthMetrics = createTool({
    id: 'getHealthMetrics',
    description: 'Calcula ingresos, CMV, utilidad y margen bruto global. Excluye muestras gratis.',
    inputSchema: z.object({
        startDate: z.string()
    .describe("Fecha de inicio del análisis en formato ISO (YYYY-MM-DD). Ejemplo: '2024-01-01'"),
    
  endDate: z.string()
    .describe("Fecha de fin del análisis en formato ISO (YYYY-MM-DD). Debe ser igual o posterior a startDate"),
    }),
    execute: async ({ startDate, endDate }) => {
        const { params, querySnippet } = getFechaFilters(startDate, endDate);
        const query = `
            SELECT 
    -- Ventas Totales (Lo que entró)
    SUM(importe) as ingresos_brutos,
    
    -- El impacto real de los descuentos (¿Cuánto dinero dejamos de ganar?)
    SUM(importe / (1 - (descuento/100)) * (descuento/100)) as monto_descuentos,
    
    -- Costo de Ventas
    SUM(cantidad * pr_costo_uni_neto) as cmv_comercial,
    
    -- Utilidad y Margen
    SUM(importe) - SUM(cantidad * pr_costo_uni_neto) as utilidad_bruta,
    
    ROUND(((SUM(importe) - SUM(cantidad * pr_costo_uni_neto)) / NULLIF(SUM(importe), 0) * 100)::numeric, 2) as margen_bruto_pct,

    -- Análisis de "Regalos" (Lo que filtraste antes, ahora dáselo como dato)
    (SELECT SUM(cantidad * pr_costo_uni_neto) FROM ventas_detalle WHERE descuento >= 100) as costo_inversion_marketing

FROM ventas_detalle
WHERE importe > 0 
  ${querySnippet}
        `;

        // AGREGAR ESTO PARA DEBUG:
        console.log("--- AGENT QUERY START ---");
        console.log("SQL:", query);
        console.log("PARAMS:", params);
        console.log("--- AGENT QUERY END ---");
        const res = await pool.query(query, params);
        if (!res.rows[0]) {
            return { message: "No se encontraron movimientos para los filtros aplicados (Línea/Fecha)." };
        }
        console.log("--- AGENT RESPONSE START ---");
        console.log(res.rows[0]);
        console.log("--- AGENT RESPONSE END ---");
        return res.rows[0];
    }
});

/**
 * TOOL 2: Análisis de Clientes (RFM & Segmentación)
 * Separado para reducir carga cognitiva.
 */
export const getCustomerAnalytics = createTool({
    id: 'getCustomerAnalytics',
    description: 'Análisis de comportamiento de clientes: RFM, Clientes Dormidos o Campeones.',
    inputSchema: z.object({
        type: z.enum(['rfm', 'dormidos', 'campeones']),
        limit: z.number().default(10),
        diasSinCompra: z.number().optional().describe('Para tipo "dormidos". Default 60.')
    }),
    execute: async ({ type, limit, diasSinCompra = 60 }) => {
        let query = '';
        const params: any[] = [limit];

        if (type === 'rfm') {
            query = `
                SELECT cliente as id, MAX(razon_social) as nombre,
                EXTRACT(DAY FROM (CURRENT_DATE - MAX(fecha))) as recencia,
                COUNT(DISTINCT idunica) as frecuencia, SUM(importe) as monetario
                FROM ventas_detalle WHERE importe > 0 GROUP BY cliente
                ORDER BY monetario DESC LIMIT $1`;

            // AGREGAR ESTO PARA DEBUG:
            console.log("--- AGENT QUERY START ---");
            console.log("SQL:", query);
            console.log("PARAMS:", params);
            console.log("--- AGENT QUERY END ---");
        } else if (type === 'dormidos') {
            params.push(diasSinCompra);
            query = `
                SELECT cliente, razon_social, MAX(fecha) as ultima_vta
                FROM ventas_detalle GROUP BY cliente, razon_social
                HAVING MAX(fecha) < CURRENT_DATE - $2
                ORDER BY ultima_vta DESC LIMIT $1`;

            // AGREGAR ESTO PARA DEBUG:
            console.log("--- AGENT QUERY START ---");
            console.log("SQL:", query);
            console.log("PARAMS:", params);
            console.log("--- AGENT QUERY END ---");
        } else {
            query = `SELECT cliente, razon_social, SUM(importe) as total FROM ventas_detalle 
                     GROUP BY cliente, razon_social ORDER BY total DESC LIMIT $1`;

            // AGREGAR ESTO PARA DEBUG:
            console.log("--- AGENT QUERY START ---");
            console.log("SQL:", query);
            console.log("PARAMS:", params);
            console.log("--- AGENT QUERY END ---");
        }
        
        const res = await pool.query(query, params);
        if (res.rows.length === 0) {
            return { message: "No se encontraron movimientos para los filtros aplicados (Línea/Fecha)." };
        }
        console.log("--- AGENT RESPONSE START ---");
        console.log(res.rows);
        console.log("--- AGENT RESPONSE END ---");
        return res.rows;
    }
});

/**
 * TOOL 3: Análisis de Rentabilidad Detallado
 * Aquí manejamos la lógica compleja de "Real vs Comercial"
 */
export const getProfitabilityAnalysis = createTool({
    id: 'getProfitabilityAnalysis',
    description: 'Analiza rentabilidad real vs comercial discriminando muestras gratis y descuentos.',
    inputSchema: z.object({
        groupBy: z.enum(['vendedor', 'articulo', 'linea', 'subcanal', 'fecha']),
        startDate: z.string()
    .describe("Fecha de inicio del análisis en formato ISO (YYYY-MM-DD). Ejemplo: '2024-01-01'"),
    
  endDate: z.string()
    .describe("Fecha de fin del análisis en formato ISO (YYYY-MM-DD). Debe ser igual o posterior a startDate"),
        limit: z.number().default(20)
    }),
    execute: async ({ groupBy, startDate, endDate, limit }) => {
        const col = COLUMN_MAPPER[groupBy];
        const { params, querySnippet } = getFechaFilters(startDate, endDate);
        params.push(limit);

        const query = `
            WITH RawData AS (
    SELECT 
        ${col} as dimension,
        
        -- Sumamos primero los componentes básicos usando COALESCE para evitar NULLs
        COALESCE(SUM(facturacion) FILTER (WHERE importe > 0), 0) as v_brutas,
        COALESCE(SUM(d1), 0) as d_totales,
        COALESCE(SUM(cmv) FILTER (WHERE importe > 0), 0) as c_ventas,
        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0) as c_muestras

    FROM ventas_detalle
    WHERE ${col} IS NOT NULL ${querySnippet}
    GROUP BY ${col}
)
, RankedData AS (
    SELECT 
        dimension,
        v_brutas as ventas_brutas,
        d_totales as descuentos_totales,
        (v_brutas - d_totales) as ventas_netas,
        c_ventas as costo_de_ventas,
        (v_brutas - d_totales - c_ventas) as utilidad_bruta,
        c_muestras as costo_muestras,
        (v_brutas - d_totales - c_ventas - c_muestras) as rentabilidad_real_neta
    FROM RawData
    ORDER BY (v_brutas - d_totales - c_ventas - c_muestras) DESC
    LIMIT $${params.length}
)
SELECT 
    *,
    -- Calculamos los porcentajes solo sobre el set final de datos para total precisión
    ROUND((utilidad_bruta / NULLIF(ventas_netas, 0) * 100)::numeric, 2) as margen_bruto_pct,
    ROUND((rentabilidad_real_neta / NULLIF(ventas_netas, 0) * 100)::numeric, 2) as margen_real_pct
FROM RankedData
ORDER BY rentabilidad_real_neta DESC;
        `;


        // AGREGAR ESTO PARA DEBUG:
        console.log("--- AGENT QUERY START ---");
        console.log("SQL:", query);
        console.log("PARAMS:", params);
        console.log("--- AGENT QUERY END ---");
        const res = await pool.query(query, params);
        if (res.rows.length === 0) {
            return { message: "No se encontraron movimientos para los filtros aplicados (Línea/Fecha)." };
        }
        console.log("--- AGENT RESPONSE START ---");
        console.log(res.rows);
        console.log("--- AGENT RESPONSE END ---");
        return res.rows;
    }
});

// Importamos tus utilidades existentes para mantener la consistencia
// import { pool, COLUMN_MAPPER, getFechaFilters } from '../lib/db'; 

export const getSalesOpportunities = createTool({
    id: 'getSalesOpportunities',
    description: 'Detecta brechas de venta comparando la matriz ideal de productos contra ventas reales.',
    inputSchema: z.object({
        startDate: z.string().optional(),
        endDate: z.string().optional(),
        groupBy: z.string().optional(),
        limit: z.number().optional().default(20)
    }),
    // USANDO LA FIRMA QUE TU COMPILADOR PIDE:
    execute: async ({ groupBy, startDate, endDate, limit }) => {
        const col = COLUMN_MAPPER[groupBy || 'vendedor'] || 'cod_ven';
        const params: any[] = [];
        let querySnippet = '';

        // 1. Construcción de parámetros para el WHERE
        if (startDate) {
            params.push(startDate);
            querySnippet += ` AND fecha >= $${params.length}`;
        }
        if (endDate) {
            params.push(endDate);
            querySnippet += ` AND fecha <= $${params.length}`;
        }

        const limitVal = limit || 20;
        params.push(limitVal);
        const limitIdx = params.length;

        // RECUPERANDO TU QUERY ORIGINAL COMPLETA
        const query = `
            WITH ClientesTop AS (
                SELECT 
                    razon_social as cliente, 
                    ${col} as dimension_nombre,
                    SUM(facturacion) as total_comprado
                FROM public.ventas_detalle
                WHERE ${col} IS NOT NULL ${querySnippet}
                GROUP BY razon_social, ${col}
                ORDER BY total_comprado DESC
                LIMIT $${limitIdx}
            ),
            LineasDisponibles AS (
                SELECT DISTINCT linea 
                FROM public.ventas_detalle 
                WHERE linea IS NOT NULL AND linea != ''
            ),
            MatrizIdeal AS (
                SELECT c.cliente, c.dimension_nombre, l.linea
                FROM ClientesTop c
                CROSS JOIN LineasDisponibles l
            ),
            VentasReales AS (
                SELECT DISTINCT razon_social as cliente, linea
                FROM public.ventas_detalle
                WHERE 1=1 ${querySnippet}
            )
            SELECT 
                m.dimension_nombre as entidad,
                m.cliente,
                m.linea as marca_ausente,
                'Oportunidad de Venta' as accion_sugerida,
                'Sin compras detectadas en el periodo' as observacion
            FROM MatrizIdeal m
            LEFT JOIN VentasReales v ON m.cliente = v.cliente AND m.linea = v.linea
            WHERE v.linea IS NULL
            ORDER BY m.dimension_nombre ASC, m.linea ASC;
        `;

        try {
            const res = await pool.query(query, params);
            return res.rows;
        } catch (error: any) {
            // Manejo de error para evitar el crash del servidor
            console.error("SQL Error en getSalesOpportunities:", error.message);
            return [{ 
                error: "Error en base de datos", 
                detail: error.message,
                suggestion: "Revisar conexión o parámetros de fecha"
            }];
        }
    }
});

/**
 * TOOL 5: Performance Comercial por Vendedor
 */
export const getSellerPerformance = createTool({
    id: 'getSellerPerformance',
    description: 'Analiza el performance comercial por vendedor: operaciones, unidades, venta neta, descuentos y ticket promedio.',
    inputSchema: z.object({
        startDate: z.string()
            .describe("Fecha de inicio del análisis en formato ISO (YYYY-MM-DD). Ejemplo: '2024-01-01'"),
        endDate: z.string()
            .describe("Fecha de fin del análisis en formato ISO (YYYY-MM-DD). Debe ser igual o posterior a startDate"),
        limit: z.number().optional().default(20)
            .describe("Cantidad máxima de vendedores a mostrar (default: 20)")
    }),
    execute: async ({ startDate, endDate, limit }) => {
        const { params, querySnippet } = getFechaFilters(startDate, endDate);
        
        const limitVal = limit || 20;
        params.push(limitVal);
        const limitIdx = params.length;

        const query = `
            SELECT 
                cod_ven AS Codigo_Vendedor,
                COUNT(DISTINCT comprobante) AS Total_Operaciones,
                SUM(cantidad) AS Volumen_Unidades,
                SUM(neto) AS Venta_Neta_Total,
                SUM(valordesc) AS Total_Descuentos_Aplicados,
                ROUND(SUM(neto) / NULLIF(COUNT(DISTINCT comprobante), 0), 2) AS Ticket_Promedio
            FROM 
                ventas_detalle
            WHERE 1=1 ${querySnippet}
            GROUP BY 
                cod_ven
            ORDER BY 
                Venta_Neta_Total DESC
            LIMIT $${limitIdx};
        `;

        try {
            const res = await pool.query(query, params);
            return res.rows;
        } catch (error: any) {
            console.error("SQL Error en getSellerPerformance:", error.message);
            return [{ 
                error: "Error en base de datos", 
                detail: error.message,
                suggestion: "Revisar conexión o parámetros de fecha"
            }];
        }
    }
});

/**
 * TOOL 6: Performance Comercial por Categoría (Rubro)
 */
export const getCategoryPerformance = createTool({
    id: 'getCategoryPerformance',
    description: 'Analiza el performance comercial por categoría (rubro): unidades, venta neta, costo y margen de contribución.',
    inputSchema: z.object({
        startDate: z.string()
            .describe("Fecha de inicio del análisis en formato ISO (YYYY-MM-DD). Ejemplo: '2024-01-01'"),
        endDate: z.string()
            .describe("Fecha de fin del análisis en formato ISO (YYYY-MM-DD). Debe ser igual o posterior a startDate"),
        limit: z.number().optional().default(20)
            .describe("Cantidad máxima de categorías a mostrar (default: 20)")
    }),
    execute: async ({ startDate, endDate, limit }) => {
        const { params, querySnippet } = getFechaFilters(startDate, endDate);
        
        const limitVal = limit || 20;
        params.push(limitVal);
        const limitIdx = params.length;

        const query = `
            SELECT 
                rubro AS Categoria_Producto,
                SUM(cantidad) AS Unidades_Vendidas,
                SUM(neto) AS Venta_Neta_Total,
                SUM(cmv) AS Costo_Total_Ventas,
                SUM(neto) - SUM(cmv) AS Margen_Contribucion_Pesos,
                ROUND(((SUM(neto) - SUM(cmv)) / NULLIF(SUM(neto), 0)) * 100, 2) AS Porcentaje_Margen
            FROM 
                ventas_detalle
            WHERE 1=1 ${querySnippet}
            GROUP BY 
                rubro
            ORDER BY 
                Venta_Neta_Total DESC
            LIMIT $${limitIdx};
        `;

        try {
            const res = await pool.query(query, params);
            return res.rows;
        } catch (error: any) {
            console.error("SQL Error en getCategoryPerformance:", error.message);
            return [{ 
                error: "Error en base de datos", 
                detail: error.message,
                suggestion: "Revisar conexión o parámetros de fecha"
            }];
        }
    }
});