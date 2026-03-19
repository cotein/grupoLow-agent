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

export const consultarIndicadores = createTool({
    id: 'consultarIndicadores',
    description: `
        Consulta indicadores puntuales financieros y comerciales en la base de datos.
        - 'rentabilidad_bruta_discriminada': resumen global comercial vs real.
        - 'rentabilidad_vendedor': por vendedor con descuentos y muestras.
        - 'rentabilidad_volumen_subcanal': por subcanal con descuentos y muestras.
        - 'clientes_no_rentables': clientes con rentabilidad real negativa.
        - 'productos_no_rentables': productos con rentabilidad real negativa.
        - 'aumentar_margen_sin_caida_ventas': productos alto volumen bajo margen.
        - 'margen_por_articulo': margen real por artículo.
        - 'margen_por_segmento': margen real por segmento producto.
        - 'margen_descuentos_por_proveedor': margen y descuentos por línea/proveedor.
        - 'ganancia_por_categoria': ganancia real por categoría (linea).
        - 'descuentos_por_vendedor': dto en pesos + muestras + costo total por vendedor.
        - 'descuentos_detalle': detalle fila a fila de muestras entregadas.
        - 'inversion_dtos': total descuentos en pesos.
        - 'pct_venta_sin_dto': % de venta sin descuento.
        - 'clientes_dto_arriba_stnd': clientes con descuento por arriba del estándar.
        - 'clientes_no_compran_marca': clientes que no compran las marcas foco.
        - 'clientes_campeones': mejores clientes según criterio definido.
        - 'clientes_compradores': clientes activos en el período.
        - 'nuevos_clientes_periodo': clientes que compraron por primera vez.
        - 'clientes_dormidos': clientes que no han comprado recientemente.
        - 'reactivacion_clientes': clientes dormidos que volvieron a comprar.
        - 'frecuencia_compra_cliente': frecuencia de compra por cliente.
        - 'venta_cruzada': productos comprados juntos frecuentemente.
        - 'mix_cliente': composición de compra por cliente.
        - 'rfm_segmentacion': segmentación por recencia, frecuencia y monto.
        - 'venta_total': venta total del período.
        - 'venta_por_vendedor': venta total por vendedor.
        - 'venta_por_categoria': venta por categoría de producto.
        - 'venta_marcas_foco': venta de las marcas prioritarias.
        - 'producto_mas_vendido': ranking de productos más vendidos.
        - 'evolucion_precio_promedio': evolución del precio promedio en el tiempo.
        - 'evolucion_diaria_ventas': evolución día a día de las ventas.
        - 'evolucion_mensual_ventas': evolución mes a mes de las ventas.
        - 'comparativo_periodos': comparativa vs período anterior / año anterior.
        - 'rechazos_devoluciones': análisis de rechazos y devoluciones.
        - 'tasa_devolucion_vendedor': tasa de devolución por vendedor.
        - 'potencial_venta_zonas': potencial de venta por zona geográfica.
        - 'ticket_promedio_subcanal': ticket promedio por subcanal.
        - 'ticket_promedio_cliente': ticket promedio por cliente.
        - 'rendimiento_chofer': rendimiento por chofer/preventista.
        - 'indice_concentracion_pareto': índice de concentración (ej. 80/20).
        - 'cumplimiento_mix_ideal': cumplimiento del mix de productos deseado.
        - 'productos_sin_rotacion': productos sin movimiento en el período.
        - 'dias_semana_mas_ventas': días de la semana con mayores ventas.
    `,
    inputSchema: z.object({
        indicador: z.enum([
            // ── Rentabilidad (todas incluyen descuentos y muestras discriminados) ──
            'rentabilidad_bruta_discriminada',   // resumen global: comercial vs real
            'rentabilidad_vendedor',             // por vendedor con descuentos y muestras
            'rentabilidad_volumen_subcanal',     // por subcanal con descuentos y muestras
            'clientes_no_rentables',             // clientes con rentabilidad real negativa
            'productos_no_rentables',            // productos con rentabilidad real negativa
            'aumentar_margen_sin_caida_ventas',  // productos alto volumen bajo margen
            'margen_por_articulo',               // margen real por artículo
            'margen_por_segmento',               // margen real por segmento producto
            'margen_descuentos_por_proveedor',   // margen y descuentos por línea/proveedor
            'ganancia_por_categoria',            // ganancia real por categoría (linea)
            // ── Descuentos y muestras ──
            'descuentos_por_vendedor',           // dto en pesos + muestras + costo total por vendedor
            'descuentos_detalle',           // detalle fila a fila de muestras entregadas
            'inversion_dtos',                    // total descuentos en pesos
            'pct_venta_sin_dto',                 // % de venta sin descuento
            // ── Clientes ──
            'clientes_dto_arriba_stnd',
            'clientes_no_compran_marca',
            'clientes_campeones',
            'clientes_compradores',
            'nuevos_clientes_periodo',
            'clientes_dormidos',
            'reactivacion_clientes',
            'frecuencia_compra_cliente',
            'venta_cruzada',
            'mix_cliente',
            'rfm_segmentacion',
            // ── Ventas ──
            'venta_total',
            'venta_por_vendedor',
            'venta_por_categoria',
            'venta_marcas_foco',
            'producto_mas_vendido',
            'evolucion_precio_promedio',
            'evolucion_diaria_ventas',
            'evolucion_mensual_ventas',
            'comparativo_periodos',
            // ── Devoluciones ──
            'rechazos_devoluciones',
            'tasa_devolucion_vendedor',
            // ── Zonas / Canales ──
            'potencial_venta_zonas',
            'ticket_promedio_subcanal',
            'ticket_promedio_cliente',
            'rendimiento_chofer',
            // ── Análisis estratégico ──
            'indice_concentracion_pareto',
            'cumplimiento_mix_ideal',
            'productos_sin_rotacion',
            'dias_semana_mas_ventas',
        ]).describe('El identificador exacto del indicador a consultar.'),
        marca: z.string().optional().describe('Marca específica para "clientes_no_compran_marca"'),
        marcasFoco: z.union([z.string(), z.array(z.string())])
            .optional()
            .transform(val => {
                if (typeof val === 'string') {
                    return val.trim() === '' ? [] : val.split(',').map(s => s.trim());
                }
                return val;
            })
            .describe('Lista de marcas para "venta_marcas_foco" (array o string separado por comas)'),
        clienteId: z.number().optional().describe('ID de cliente para "mix_cliente"'),
        diasSinCompra: z.number().optional().describe('Días sin compra para "clientes_dormidos". Default 60.'),
        diasPrevios: z.number().optional().describe('Días previos al período para "reactivacion_clientes". Default 90.'),
        fechaInicioAnterior: z.string().optional().describe('Fecha inicio período anterior para "comparativo_periodos" (YYYY-MM-DD)'),
        fechaFinAnterior: z.string().optional().describe('Fecha fin período anterior para "comparativo_periodos" (YYYY-MM-DD)'),
        fechaInicio: z.string().optional().describe('Fecha de inicio en formato YYYY-MM-DD'),
        fechaFin: z.string().optional().describe('Fecha de fin en formato YYYY-MM-DD'),
        limite: z.number().max(50).default(10).describe('Cantidad de registros a devolver, máximo 50.'),
        orden: z.enum(['DESC', 'ASC']).default('DESC').describe('DESC para mayores valores primero, ASC para menores.')
    }),
    execute: async (inputData: any) => {
        let sql = '';
        const params: any[] = [];
        const limite = inputData.limite ?? 10;
        const orden = inputData.orden === 'ASC' ? 'ASC' : 'DESC';

        // ─────────────────────────────────────────────────────────────────────
        // HELPER: agrega filtro de fecha usando los índices ACTUALES de params.
        // Llamar DESPUÉS de cualquier params.push() propio del case,
        // para que los índices $N sean siempre correlativos y correctos.
        // ─────────────────────────────────────────────────────────────────────
        const agregarFiltroFecha = (): string => {
            if (inputData.fechaInicio && inputData.fechaFin) {
                params.push(inputData.fechaInicio, inputData.fechaFin);
                return `fecha BETWEEN $${params.length - 1} AND $${params.length}`;
            } else if (inputData.fechaInicio) {
                params.push(inputData.fechaInicio);
                return `fecha >= $${params.length}`;
            }
            return '';
        };

        // ─────────────────────────────────────────────────────────────────────
        // FRAGMENTOS SQL REUTILIZABLES
        // Rentabilidad real = facturacion - cmv - d1  (solo importe > 0)
        // Impacto muestras  = SUM(cmv) FILTER (WHERE importe = 0)
        // Rentabilidad neta = rentabilidad_real - impacto_muestras
        // ─────────────────────────────────────────────────────────────────────

        switch (inputData.indicador) {

            // ════════════════════════════════════════════════════════════════
            // RENTABILIDAD — todas discriminan comercial, descuentos y muestras
            // ════════════════════════════════════════════════════════════════

            case 'rentabilidad_vendedor': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        cod_ven,

                        -- ── Vista comercial ───────────────────────────────────────
                        SUM(facturacion) FILTER (WHERE importe > 0)                       AS venta_neta_comercial,
                        SUM(cmv)         FILTER (WHERE importe > 0)                       AS cmv_comercial,
                        SUM(facturacion - cmv) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                           AS margen_pct_comercial,

                        -- ── Muestras gratis del vendedor ──────────────────────────
                        COUNT(*)         FILTER (WHERE importe = 0)                       AS items_muestra_gratis,
                        SUM(cmv)         FILTER (WHERE importe = 0)                       AS cmv_muestras_gratis,

                        -- ── Vista real total ──────────────────────────────────────
                        SUM(facturacion - cmv)                                             AS rentabilidad_real_total,
                        ROUND((SUM(facturacion - cmv)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                           AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY cod_ven
                    ORDER BY rentabilidad_comercial ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'clientes_no_rentables': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        Cliente,
                        Razon_Social,
                        SUM(facturacion)                                                        AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(cmv)         FILTER (WHERE importe > 0)                            AS cmv_ventas,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    HAVING (SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)) <= 0
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'productos_no_rentables': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        Art,
                        articulo,
                        SUM(facturacion)                                                        AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(cmv)         FILTER (WHERE importe > 0)                            AS cmv_ventas,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Art, articulo
                    HAVING (SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)) <= 0
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'aumentar_margen_sin_caida_ventas': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        Art,
                        articulo,
                        SUM(Cantidad)                                                           AS volumen_unidades,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Art, articulo
                    HAVING SUM(Cantidad) FILTER (WHERE importe > 0) > 1000
                       AND (SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)) < 0.15
                    ORDER BY volumen_unidades ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'margen_por_articulo': {
                // fusiona top_productos_margen — ambos indicadores resueltos en uno
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        Art,
                        articulo,
                        SUM(Cantidad)    FILTER (WHERE importe > 0)                            AS volumen_unidades,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(cmv)         FILTER (WHERE importe > 0)                            AS cmv_ventas,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    WHERE importe >= 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY Art, articulo
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'margen_total': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        -- ── Vista comercial (excluye muestras gratis) ──────────────
                        SUM(facturacion) FILTER (WHERE importe > 0)                         AS venta_neta_comercial,
                        SUM(cmv)         FILTER (WHERE importe > 0)                         AS cmv_comercial,
                        SUM(facturacion - cmv) FILTER (WHERE importe > 0)                   AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                             AS margen_pct_comercial,

                        -- ── Costo de muestras gratis ──────────────────────────────
                        COUNT(*)         FILTER (WHERE importe = 0)                         AS items_muestra_gratis,
                        SUM(cmv)         FILTER (WHERE importe = 0)                         AS cmv_muestras_gratis,

                        -- ── Vista real total (incluye muestras) ───────────────────
                        SUM(facturacion)                                                     AS venta_neta_total,
                        SUM(cmv)                                                             AS cmv_total,
                        SUM(facturacion - cmv)                                               AS rentabilidad_real_total,
                        ROUND((SUM(facturacion - cmv)
                            / NULLIF(SUM(facturacion), 0)
                            * 100)::numeric, 2)                                             AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            case 'margen_descuentos_por_proveedor': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        linea                                                                   AS proveedor_proxy,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_otorgados,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY linea
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'ganancia_por_categoria': {
                // fusiona venta_por_categoria con rentabilidad real
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        linea                                                                   AS categoria,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(cmv)         FILTER (WHERE importe > 0)                            AS cmv_ventas,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    WHERE linea IS NOT NULL
                    ${f ? 'AND ' + f : ''}
                    GROUP BY linea
                    ORDER BY rentabilidad_real_neta DESC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'margen_por_segmento': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        segmentoproducto,
                        COUNT(DISTINCT Cliente) FILTER (WHERE importe > 0)                     AS clientes_compradores,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    WHERE segmentoproducto IS NOT NULL
                    ${f ? 'AND ' + f : ''}
                    GROUP BY segmentoproducto
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // CLIENTES
            // ════════════════════════════════════════════════════════════════

            case 'clientes_dto_arriba_stnd': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Cliente, Razon_Social,
                           (SUM(d1) / NULLIF(SUM(importe), 0)) * 100 AS pct_descuento_otorgado
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    HAVING (SUM(d1) / NULLIF(SUM(importe), 0)) > 0.05
                    ORDER BY pct_descuento_otorgado ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'clientes_no_compran_marca': {
                if (!inputData.marca) return { error: 'Falta el parámetro marca' };
                params.push(inputData.marca);           // $1 = marca
                const f = agregarFiltroFecha();         // $2/$3 = fechas (si aplica)
                sql = `
                    SELECT DISTINCT Cliente, Razon_Social
                    FROM ventas_detalle
                    WHERE Cliente NOT IN (
                        SELECT Cliente FROM ventas_detalle WHERE linea = $1
                    )
                    ${f ? 'AND ' + f : ''}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'clientes_campeones': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        Cliente,
                        Razon_Social,
                        COUNT(DISTINCT Comprobante) FILTER (WHERE importe > 0)                 AS frecuencia_compras,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    ORDER BY frecuencia_compras ${orden}, venta_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'clientes_compradores': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT COUNT(DISTINCT Cliente) AS clientes_compradores_unicos
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            case 'nuevos_clientes_periodo': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Cliente, Razon_Social, MIN(fecha) AS primera_compra
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    HAVING MIN(fecha) = (
                        SELECT MIN(fecha2.fecha)
                        FROM ventas_detalle fecha2
                        WHERE fecha2.Cliente = ventas_detalle.Cliente
                    )
                    ORDER BY primera_compra ASC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'clientes_dormidos': {
                const dias = inputData.diasSinCompra || 60;
                params.push(dias);                      // $1 = dias
                // No usa filtroFecha: busca en toda la historia
                sql = `
                    SELECT Cliente, Razon_Social,
                           MAX(fecha) AS ultima_compra,
                           CURRENT_DATE - MAX(fecha) AS dias_sin_comprar
                    FROM ventas_detalle
                    GROUP BY Cliente, Razon_Social
                    HAVING MAX(fecha) < CURRENT_DATE - $1
                    ORDER BY dias_sin_comprar DESC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'reactivacion_clientes': {
                // Clientes que compraron en el período actual pero no en los N días previos a ese período
                const diasPrevios = inputData.diasPrevios || 90;
                params.push(diasPrevios);               // $1 = diasPrevios
                const f = agregarFiltroFecha();         // $2/$3 = fechas período actual
                sql = `
                    SELECT vp.Cliente, vp.Razon_Social,
                           MIN(vp.fecha) AS fecha_reactivacion
                    FROM ventas_detalle vp
                    WHERE ${f || '1=1'}
                      AND vp.Cliente NOT IN (
                          SELECT DISTINCT Cliente
                          FROM ventas_detalle
                          WHERE fecha >= (
                              SELECT MIN(f2.fecha) FROM ventas_detalle f2
                              WHERE ${f || '1=1'}
                          ) - $1
                            AND fecha < (
                              SELECT MIN(f2.fecha) FROM ventas_detalle f2
                              WHERE ${f || '1=1'}
                          )
                      )
                    GROUP BY vp.Cliente, vp.Razon_Social
                    ORDER BY fecha_reactivacion ASC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'frecuencia_compra_cliente': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Cliente, Razon_Social,
                           COUNT(DISTINCT Comprobante) AS total_compras,
                           MIN(fecha) AS primera_compra,
                           MAX(fecha) AS ultima_compra,
                           ROUND(
                               (MAX(fecha) - MIN(fecha))::numeric
                               / NULLIF(COUNT(DISTINCT Comprobante) - 1, 0),
                           2) AS dias_entre_compras
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    HAVING COUNT(DISTINCT Comprobante) > 1
                    ORDER BY dias_entre_compras ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'venta_cruzada': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Cliente, Razon_Social,
                           COUNT(DISTINCT linea) AS lineas_distintas_compradas,
                           SUM(importe) AS venta_total
                    FROM ventas_detalle
                    WHERE importe > 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    HAVING COUNT(DISTINCT linea) > 1
                    ORDER BY lineas_distintas_compradas ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'mix_cliente': {
                if (!inputData.clienteId) return { error: 'Falta el parámetro clienteId' };
                params.push(inputData.clienteId);       // $1 = clienteId
                const f = agregarFiltroFecha();         // $2/$3 = fechas (si aplica)
                sql = `
                    SELECT linea,
                           SUM(importe) AS venta_linea,
                           ROUND(
                               (SUM(importe) / NULLIF(SUM(SUM(importe)) OVER (), 0) * 100)::numeric,
                           2) AS pct_del_cliente
                    FROM ventas_detalle
                    WHERE Cliente = $1
                      AND importe > 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY linea
                    ORDER BY venta_linea DESC;
                `;
                break;
            }

            case 'rfm_segmentacion': {
                const f = agregarFiltroFecha();
                sql = `
                    WITH rfm AS (
                        SELECT Cliente, Razon_Social,
                               CURRENT_DATE - MAX(fecha)              AS recencia_dias,
                               COUNT(DISTINCT Comprobante)            AS frecuencia,
                               SUM(importe)                           AS monto_total
                        FROM ventas_detalle
                        WHERE importe > 0
                        ${f ? 'AND ' + f : ''}
                        GROUP BY Cliente, Razon_Social
                    )
                    SELECT *,
                           NTILE(4) OVER (ORDER BY recencia_dias ASC)  AS r_score,
                           NTILE(4) OVER (ORDER BY frecuencia DESC)    AS f_score,
                           NTILE(4) OVER (ORDER BY monto_total DESC)   AS m_score
                    FROM rfm
                    ORDER BY m_score DESC, f_score DESC, r_score DESC
                    LIMIT ${limite};
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // VENTAS
            // ════════════════════════════════════════════════════════════════

            case 'venta_total': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT SUM(importe) AS venta_bruta,
                           SUM(importe) AS venta_facturada
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            case 'venta_por_vendedor': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT cod_ven, SUM(importe) AS venta_facturada
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY cod_ven
                    ORDER BY venta_facturada ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'venta_por_categoria': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT rubro, SUM(importe) AS venta_facturada
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY rubro
                    ORDER BY venta_facturada ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'venta_marcas_foco': {
                if (inputData.marcasFoco && inputData.marcasFoco.length > 0) {
                    params.push(inputData.marcasFoco);  // $1 = array de marcas
                    const f = agregarFiltroFecha();     // $2/$3 = fechas
                    sql = `
                        SELECT linea AS marca_foco, SUM(importe) AS venta_facturada
                        FROM ventas_detalle
                        WHERE linea = ANY($1)
                        ${f ? 'AND ' + f : ''}
                        GROUP BY linea
                        ORDER BY venta_facturada ${orden}
                        LIMIT ${limite};
                    `;
                } else {
                    const f = agregarFiltroFecha();
                    sql = `
                        SELECT linea AS marca_foco, SUM(importe) AS venta_facturada
                        FROM ventas_detalle
                        ${f ? 'WHERE ' + f : ''}
                        GROUP BY linea
                        ORDER BY venta_facturada ${orden}
                        LIMIT ${limite};
                    `;
                }
                break;
            }

            case 'producto_mas_vendido': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Art, articulo,
                           SUM(Cantidad) AS volumen_unidades,
                           SUM(importe) AS venta_total
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Art, articulo
                    ORDER BY volumen_unidades ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'evolucion_precio_promedio': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT Art, articulo,
                           SUM(importe) AS venta_total,
                           SUM(Cantidad) AS unidades_vendidas,
                           (SUM(importe) / NULLIF(SUM(Cantidad), 0)) AS precio_promedio
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Art, articulo
                    ORDER BY precio_promedio ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'evolucion_diaria_ventas': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        to_char(fecha, 'DD/MM/YYYY')                                           AS fecha_formateada,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY fecha
                    ORDER BY fecha ASC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'evolucion_mensual_ventas': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        TO_CHAR(fecha, 'YYYY-MM')                                              AS mes,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY TO_CHAR(fecha, 'YYYY-MM')
                    ORDER BY mes ASC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'comparativo_periodos': {
                // Usa sus propias 4 fechas — NO usa agregarFiltroFecha()
                if (!inputData.fechaInicio || !inputData.fechaFin || !inputData.fechaInicioAnterior || !inputData.fechaFinAnterior) {
                    return { error: 'Se requieren fechaInicio, fechaFin, fechaInicioAnterior y fechaFinAnterior' };
                }
                params.push(
                    inputData.fechaInicio,          // $1
                    inputData.fechaFin,             // $2
                    inputData.fechaInicioAnterior,  // $3
                    inputData.fechaFinAnterior      // $4
                );
                sql = `
                    SELECT
                        SUM(CASE WHEN fecha BETWEEN $1 AND $2 THEN importe ELSE 0 END)       AS venta_periodo_actual,
                        SUM(CASE WHEN fecha BETWEEN $3 AND $4 THEN importe ELSE 0 END)       AS venta_periodo_anterior,
                        ROUND((
                            (SUM(CASE WHEN fecha BETWEEN $1 AND $2 THEN importe ELSE 0 END)
                             - SUM(CASE WHEN fecha BETWEEN $3 AND $4 THEN importe ELSE 0 END))
                            / NULLIF(SUM(CASE WHEN fecha BETWEEN $3 AND $4 THEN importe ELSE 0 END), 0) * 100
                        )::numeric, 2)                                                       AS variacion_porcentual,
                        SUM(CASE WHEN fecha BETWEEN $1 AND $2 THEN importe - CMV ELSE 0 END) AS margen_actual,
                        SUM(CASE WHEN fecha BETWEEN $3 AND $4 THEN importe - CMV ELSE 0 END) AS margen_anterior
                    FROM ventas_detalle
                    WHERE importe > 0;
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // DESCUENTOS
            // ════════════════════════════════════════════════════════════════

            case 'inversion_dtos': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT SUM(d1) AS inversion_total_descuentos
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            case 'pct_venta_sin_dto': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT (SUM(CASE WHEN d1 = 0 OR d1 IS NULL THEN importe ELSE 0 END)
                            / NULLIF(SUM(importe), 0)) * 100 AS pct_ventas_sin_descuento
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // DEVOLUCIONES
            // ════════════════════════════════════════════════════════════════

            case 'rechazos_devoluciones': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT motivodev AS motivo_rechazo,
                           COUNT(DISTINCT Comprobante) AS cantidad_facturas_rechazadas,
                           SUM(importe) AS valor_rechazado
                    FROM ventas_detalle
                    WHERE motivodev IS NOT NULL
                      AND motivodev NOT IN ('', '0')
                    ${f ? 'AND ' + f : ''}
                    GROUP BY motivodev
                    ORDER BY cantidad_facturas_rechazadas ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'tasa_devolucion_vendedor': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT cod_ven,
                           COUNT(DISTINCT CASE
                               WHEN motivodev IS NOT NULL AND motivodev NOT IN ('','0')
                               THEN Comprobante END)                        AS facturas_rechazadas,
                           COUNT(DISTINCT Comprobante)                      AS total_facturas,
                           ROUND(
                               COUNT(DISTINCT CASE
                                   WHEN motivodev IS NOT NULL AND motivodev NOT IN ('','0')
                                   THEN Comprobante END)::numeric
                               / NULLIF(COUNT(DISTINCT Comprobante), 0) * 100
                           , 2)                                             AS pct_rechazo
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY cod_ven
                    ORDER BY pct_rechazo ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // ZONAS / CANALES
            // ════════════════════════════════════════════════════════════════

            case 'potencial_venta_zonas': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        reparto                                                                 AS zona,
                        COUNT(DISTINCT Cliente) FILTER (WHERE importe > 0)                     AS clientes_activos,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta
                    FROM ventas_detalle
                    WHERE reparto IS NOT NULL
                    ${f ? 'AND ' + f : ''}
                    GROUP BY reparto
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'rentabilidad_volumen_subcanal': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        subcanal,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(cmv)         FILTER (WHERE importe > 0)                            AS cmv_ventas,
                        SUM(peso)        FILTER (WHERE importe > 0)                            AS total_kilos_vendidos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    WHERE subcanal IS NOT NULL
                    ${f ? 'AND ' + f : ''}
                    GROUP BY subcanal
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'ticket_promedio_subcanal': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT subcanal,
                           COUNT(DISTINCT comprobante) AS cantidad_facturas_emitidas,
                           SUM(importe) AS importe_total,
                           ROUND((SUM(importe) / NULLIF(COUNT(DISTINCT comprobante), 0))::numeric, 2) AS ticket_promedio_pesos
                    FROM ventas_detalle
                    WHERE importe > 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY subcanal
                    ORDER BY ticket_promedio_pesos ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'ticket_promedio_cliente': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT cliente AS cod_cliente, razon_social, subcanal,
                           COUNT(DISTINCT comprobante) AS cantidad_compras,
                           SUM(importe) AS importe_total,
                           ROUND((SUM(importe) / NULLIF(COUNT(DISTINCT comprobante), 0))::numeric, 2) AS ticket_promedio_pesos
                    FROM ventas_detalle
                    WHERE importe > 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY cliente, razon_social, subcanal
                    HAVING COUNT(DISTINCT comprobante) > 1
                    ORDER BY ticket_promedio_pesos ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'rendimiento_chofer': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        chofer,
                        COUNT(DISTINCT Comprobante) FILTER (WHERE importe > 0)                 AS facturas_entregadas,
                        COUNT(DISTINCT Cliente)     FILTER (WHERE importe > 0)                 AS clientes_atendidos,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta
                    FROM ventas_detalle
                    WHERE chofer IS NOT NULL
                    ${f ? 'AND ' + f : ''}
                    GROUP BY chofer
                    ORDER BY rentabilidad_real_neta ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // ANÁLISIS ESTRATÉGICO
            // ════════════════════════════════════════════════════════════════

            case 'indice_concentracion_pareto': {
                const f = agregarFiltroFecha();
                sql = `
                    WITH total AS (
                        SELECT
                            SUM(facturacion) FILTER (WHERE importe > 0)  AS total_venta,
                            SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                                - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0) AS total_rent_neta
                        FROM ventas_detalle
                        ${f ? 'WHERE ' + f : ''}
                    )
                    SELECT
                        Cliente,
                        Razon_Social,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        ROUND((SUM(facturacion) FILTER (WHERE importe > 0)
                            / NULLIF((SELECT total_venta FROM total), 0)
                            * 100)::numeric, 2)                                                AS pct_del_total_venta,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF((SELECT total_rent_neta FROM total), 0)
                            * 100)::numeric, 2)                                                AS pct_del_total_rentabilidad
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY Cliente, Razon_Social
                    ORDER BY venta_neta DESC
                    LIMIT ${limite};
                `;
                break;
            }

            case 'cumplimiento_mix_ideal': {
                const f = agregarFiltroFecha();
                sql = `
                    WITH total AS (
                        SELECT SUM(facturacion) FILTER (WHERE importe > 0) AS total_venta
                        FROM ventas_detalle
                        ${f ? 'WHERE ' + f : ''}
                    )
                    SELECT
                        linea                                                                   AS categoria,
                        SUM(facturacion) FILTER (WHERE importe > 0)                            AS venta_neta,
                        ROUND((SUM(facturacion) FILTER (WHERE importe > 0)
                            / NULLIF((SELECT total_venta FROM total), 0)
                            * 100)::numeric, 2)                                                AS peso_en_el_mix,
                        SUM(d1)          FILTER (WHERE importe > 0)                            AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS pct_descuento,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)                 AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_comercial,
                        COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                       AS cmv_muestras,
                        SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                            - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)                 AS rentabilidad_real_neta,
                        ROUND(((SUM(facturacion - cmv - d1) FILTER (WHERE importe > 0)
                             - COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                                AS margen_pct_real
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY linea
                    ORDER BY peso_en_el_mix ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'productos_sin_rotacion': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT DISTINCT vd_all.Art, vd_all.articulo
                    FROM ventas_detalle vd_all
                    WHERE vd_all.Art NOT IN (
                        SELECT DISTINCT Art
                        FROM ventas_detalle
                        WHERE importe > 0
                        ${f ? 'AND ' + f : ''}
                    )
                    LIMIT ${limite};
                `;
                break;
            }

            case 'dias_semana_mas_ventas': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT TO_CHAR(fecha, 'Day') AS dia_semana,
                           EXTRACT(DOW FROM fecha) AS num_dia,
                           COUNT(DISTINCT Comprobante) AS facturas,
                           SUM(importe) AS venta_total,
                           ROUND(AVG(importe)::numeric, 2) AS ticket_promedio
                    FROM ventas_detalle
                    WHERE importe > 0
                    ${f ? 'AND ' + f : ''}
                    GROUP BY dia_semana, num_dia
                    ORDER BY num_dia ASC;
                `;
                break;
            }

            // ════════════════════════════════════════════════════════════════
            // RENTABILIDAD DISCRIMINADA (comercial vs real)
            // ════════════════════════════════════════════════════════════════

            case 'rentabilidad_bruta_discriminada': {
                // Igual que margen_total pero con alias más explícitos
                // y separando claramente las tres secciones para el LLM
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        -- ── 1. Rentabilidad comercial (lo que coincide con Excel) ─
                        SUM(facturacion) FILTER (WHERE importe > 0)                         AS venta_neta_comercial,
                        SUM(cmv)         FILTER (WHERE importe > 0)                         AS cmv_ventas_reales,
                        SUM(facturacion - cmv) FILTER (WHERE importe > 0)                   AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                             AS margen_pct_comercial,

                        -- ── 2. Costo de muestras gratis (política comercial) ──────
                        COUNT(*)         FILTER (WHERE importe = 0)                         AS cantidad_items_muestra,
                        SUM(cmv)         FILTER (WHERE importe = 0)                         AS cmv_muestras_gratis,
                        ROUND((SUM(cmv)  FILTER (WHERE importe = 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                             AS pct_muestras_sobre_venta,

                        -- ── 3. Resultado real del período (comercial - muestras) ──
                        SUM(facturacion - cmv)                                               AS rentabilidad_real_total,
                        ROUND((SUM(facturacion - cmv)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                             AS margen_pct_real,

                        -- ── 4. Diferencia entre ambas vistas ─────────────────────
                        SUM(facturacion - cmv) FILTER (WHERE importe > 0)
                            - SUM(facturacion - cmv)                                        AS impacto_muestras_en_resultado
                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''};
                `;
                break;
            }

            case 'rentabilidad_vendedor_discriminada': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        cod_ven,

                        -- ── Ventas reales ─────────────────────────────────────────
                        COUNT(DISTINCT comprobante) FILTER (WHERE importe > 0)             AS facturas_emitidas,
                        SUM(facturacion) FILTER (WHERE importe > 0)                        AS venta_neta_comercial,
                        SUM(d1)          FILTER (WHERE importe > 0)                        AS descuentos_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_descuento,
                        SUM(cmv)         FILTER (WHERE importe > 0)                        AS cmv_ventas,
                        SUM(facturacion - cmv) FILTER (WHERE importe > 0)                  AS rentabilidad_comercial,
                        ROUND((SUM(facturacion - cmv) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS margen_pct_comercial,

                        -- ── Muestras gratis ───────────────────────────────────────
                        COUNT(*) FILTER (WHERE importe = 0)                                AS items_muestra_gratis,
                        SUM(cmv) FILTER (WHERE importe = 0)                                AS cmv_muestras,
                        ROUND((SUM(cmv) FILTER (WHERE importe = 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_muestras_sobre_venta,

                        -- ── Costo comercial total (dto + muestras) ────────────────
                        ROUND(((SUM(d1) FILTER (WHERE importe > 0)
                             + SUM(cmv)  FILTER (WHERE importe = 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_costo_comercial_total,

                        -- ── Rentabilidad real (descuenta muestras) ────────────────
                        SUM(facturacion - cmv)                                              AS rentabilidad_real,
                        ROUND((SUM(facturacion - cmv)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS margen_pct_real

                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY cod_ven
                    ORDER BY rentabilidad_comercial ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'descuentos_por_vendedor': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        cod_ven,
                        COUNT(DISTINCT comprobante) FILTER (WHERE importe > 0)             AS facturas_emitidas,
                        SUM(facturacion) FILTER (WHERE importe > 0)                        AS venta_neta,

                        -- ── Descuentos en pesos (d1) ──────────────────────────────
                        SUM(d1) FILTER (WHERE importe > 0)                                 AS descuento_pesos,
                        ROUND((SUM(d1) FILTER (WHERE importe > 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_descuento,

                        -- ── Muestras gratis (descuento = 100) ────────────────────
                        COUNT(*) FILTER (WHERE importe = 0)                                AS items_muestra_gratis,
                        SUM(cmv) FILTER (WHERE importe = 0)                                AS cmv_muestras,
                        ROUND((SUM(cmv) FILTER (WHERE importe = 0)
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_muestras,

                        -- ── Costo comercial total ─────────────────────────────────
                        SUM(d1) FILTER (WHERE importe > 0)
                            + COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0)             AS costo_comercial_total_pesos,
                        ROUND(((SUM(d1) FILTER (WHERE importe > 0)
                             + COALESCE(SUM(cmv) FILTER (WHERE importe = 0), 0))
                            / NULLIF(SUM(facturacion) FILTER (WHERE importe > 0), 0)
                            * 100)::numeric, 2)                                            AS pct_costo_comercial_total

                    FROM ventas_detalle
                    ${f ? 'WHERE ' + f : ''}
                    GROUP BY cod_ven
                    ORDER BY pct_costo_comercial_total ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            case 'descuentos_detalle': {
                const f = agregarFiltroFecha();
                sql = `
                    SELECT
                        cod_ven,
                        razon_social                                AS cliente,
                        articulo,
                        linea,
                        fecha,
                        cantidad,
                        pr_costo_uni_neto                          AS costo_unitario,
                        cmv                                        AS costo_total_muestra
                    FROM ventas_detalle
                    WHERE importe = 0
                    ${f ? 'AND ' + f : ''}
                    ORDER BY cmv ${orden}
                    LIMIT ${limite};
                `;
                break;
            }

            default:
                return { error: 'Indicador no reconocido' };
        }

        console.log(`Ejecutando consulta para: ${inputData.indicador}`);

        try {
            const res = await pool.query(sql, params);
            return res.rows;
        } catch (error: any) {
            console.error(error);
            return { error: `Error ejecutando consulta: ${error.message}` };
        }
    }
});