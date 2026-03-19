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
            SELECT 
                ${col} as dimension,
                
                -- 1. Ventas Brutas
                SUM(facturacion) FILTER (WHERE importe > 0) as ventas_brutas,
                
                -- 2. Deducciones
                SUM(d1) as descuentos_totales,
                
                -- 3. Ventas Netas
                SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1) as ventas_netas,
                
                -- 4. Costo de Ventas (CMV de ventas comerciales)
                SUM(cmv) FILTER (WHERE importe > 0) as costo_de_ventas,
                
                -- 5. Utilidad Bruta (Ventas Netas - Costo de Ventas)
                SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1) - SUM(cmv) FILTER (WHERE importe > 0) as utilidad_bruta,
                
                -- 6. Otros Costos (Muestras, etc)
                SUM(cmv) FILTER (WHERE importe = 0) as costo_muestras,
                
                -- 7. RENTABILIDAD REAL (Utilidad Bruta - Otros Costos)
                (SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1) - SUM(cmv) FILTER (WHERE importe > 0) - SUM(cmv) FILTER (WHERE importe = 0)) as rentabilidad_real_neta,
                
                -- 8. Márgenes
                ROUND(((SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1) - SUM(cmv) FILTER (WHERE importe > 0)) / 
                       NULLIF(SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1), 0) * 100)::numeric, 2) as margen_bruto_pct,
                
                ROUND(((SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1) - SUM(cmv) FILTER (WHERE importe > 0) - SUM(cmv) FILTER (WHERE importe = 0)) / 
                       NULLIF(SUM(facturacion) FILTER (WHERE importe > 0) - SUM(d1), 0) * 100)::numeric, 2) as margen_real_pct

            FROM ventas_detalle
            WHERE ${col} IS NOT NULL ${querySnippet}
            GROUP BY ${col}
            ORDER BY rentabilidad_real_neta DESC
            LIMIT $${params.length}
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
    description: 'Identifica oportunidades de venta analizando qué líneas de productos NO están comprando los clientes top en un periodo dado.',
    inputSchema: z.object({
        groupBy: z.enum(['vendedor', 'cliente', 'linea', 'subcanal', 'fecha']).default('cliente'),
        startDate: z.string()
    .describe("Fecha de inicio del análisis en formato ISO (YYYY-MM-DD). Ejemplo: '2024-01-01'"),
    
  endDate: z.string()
    .describe("Fecha de fin del análisis en formato ISO (YYYY-MM-DD). Debe ser igual o posterior a startDate"),
        limitTop: z.number().default(50).describe('Cantidad de clientes más grandes a auditar'),
        recentMonths: z.number().default(4).describe('Meses hacia atrás para verificar si hubo ventas reales')
    }),
    execute: async ({ groupBy, startDate, endDate, limitTop, recentMonths }) => {
        // 1. Mapeo de columnas y filtros de fecha siguiendo tu patrón
        const col = COLUMN_MAPPER[groupBy] || 'razon_social';
        const { params, querySnippet } = getFechaFilters(startDate, endDate);
        
        // Añadimos parámetros adicionales para el LIMIT y el intervalo de ventas recientes
        params.push(limitTop);
        const limitParamIndex = params.length;
        
        params.push(`${recentMonths} months`);
        const intervalParamIndex = params.length;

        /**
         * SQL EXPLAINED:
         * - ClientesTop: Filtra el universo de clientes grandes según facturación en el rango de fechas.
         * - LineasDisponibles: Obtiene el catálogo activo de líneas.
         * - MatrizIdeal: Cross Join para crear la expectativa de "Todo cliente debe comprar toda línea".
         * - VentasReales: Cruza con la realidad de los últimos N meses.
         */
        const query = `
            WITH ClientesTop AS (
                SELECT 
                    cliente, 
                    ${col} as dimension_nombre,
                    SUM(facturacion) as total_comprado
                FROM public.ventas_detalle
                WHERE ${col} IS NOT NULL ${querySnippet}
                GROUP BY cliente, ${col}
                ORDER BY total_comprado DESC
                LIMIT $${limitParamIndex}
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
                SELECT DISTINCT cliente, linea
                FROM public.ventas_detalle
                WHERE 1=1 ${querySnippet ? querySnippet : `AND fecha >= CURRENT_DATE - CAST($${intervalParamIndex} AS INTERVAL)`}
            )
            SELECT 
                m.dimension_nombre as entidad,
                m.linea as marca_ausente,
                'Oportunidad de Venta' as accion_sugerida,
                (CASE WHEN ${startDate ? 'true' : 'false'} OR ${endDate ? 'true' : 'false'} 
                      THEN 'Sin compras entre ${startDate || '(inicio)'} y ${endDate || '(fin)'}'
                      ELSE 'Sin compras entre ${startDate || '(inicio)'} y ${endDate || '(fin)'}' 
                 END) as observacion
            FROM MatrizIdeal m
            LEFT JOIN VentasReales v ON m.cliente = v.cliente AND m.linea = v.linea
            WHERE v.linea IS NULL
            ORDER BY m.dimension_nombre ASC, m.linea ASC;
        `;

        // DEBUG siguiendo tu estándar
        console.log("--- AGENT OPPORTUNITY QUERY START ---");
        console.log("SQL:", query);
        console.log("PARAMS:", params);
        console.log("--- AGENT OPPORTUNITY QUERY END ---");

        try {
            const res = await pool.query(query, params);

            if (res.rows.length === 0) {
                return { message: "No se detectaron brechas de venta. Todos los clientes top están comprando todas las líneas." };
            }

            console.log("--- AGENT RESPONSE START ---");
            console.log(`Detectadas ${res.rows.length} oportunidades.`);
            console.log("--- AGENT RESPONSE END ---");

            return res.rows;
        } catch (error) {
            console.error("ERROR EN getSalesOpportunities:", error);
            /**
             * ESTRATEGIA SENIOR: 
             * En lugar de 'throw', devolvemos un objeto de error estructurado.
             * Esto permite que el Agente le diga al usuario: "Hubo un problema con la DB, verifica el formato de fecha".
             */
            
            return {
                status: "error",
                message: "No pude completar el análisis de oportunidades.",
                technical_details: error,
                suggestion: "Por favor, verifica que las fechas tengan el formato YYYY-MM-DD y que la base de datos esté accesible."
            };
        }
    }
});