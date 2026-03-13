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
    description: 'Consulta indicadores puntuales financieros y comerciales en la base de datos.',
    inputSchema: z.object({
        indicador: z.enum([
            'rentabilidad_vendedor',
            'clientes_no_rentables',
            'clientes_dto_arriba_stnd',
            'clientes_no_compran_marca',
            'productos_no_rentables',
            'aumentar_margen_sin_caida_ventas',
            'clientes_campeones',
            'potencial_venta_zonas',
            'venta_total',
            'venta_por_vendedor',
            'venta_por_categoria',
            'clientes_compradores',
            'venta_marcas_foco',
            'rechazos_devoluciones',
            'inversion_dtos',
            'pct_venta_sin_dto',
            'margen_por_articulo',
            'margen_total',
            'ganancia_por_categoria',
            'margen_descuentos_por_proveedor',
            'rentabilidad_volumen_subcanal',
            'ticket_promedio_subcanal',
            'ticket_promedio_cliente',
            'producto_mas_vendido',
            'evolucion_precio_promedio',
            'indice_concentracion_pareto',
            'cumplimiento_mix_ideal',
            'evolucion_diaria_ventas',
            'evolucion_mensual_ventas'
        ]).describe('El identificador exacto del indicador a consultar.'),
        marca: z.string().optional().describe('Marca específica para consultar "clientes_no_compran_marca"'),
        marcasFoco: z.union([z.string(), z.array(z.string())])
            .optional()
            .transform(val => {
                if (typeof val === 'string') {
                    return val.trim() === '' ? [] : val.split(',').map(s => s.trim());
                }
                return val;
            })
            .describe('Lista de marcas para consultar "venta_marcas_foco" (puede ser un array o string separado por comas)'),
        fechaInicio: z.string().optional().describe('Fecha de inicio en formato YYYY-MM-DD'),
        fechaFin: z.string().optional().describe('Fecha de fin en formato YYYY-MM-DD'),
        limite: z.number().max(50).default(10).describe('Cantidad de registros a devolver, máximo 50. Úsalo para pedir el top X.'),
        orden: z.enum(['DESC', 'ASC']).default('DESC').describe('DESC para traer los de mayor margen (mejores), ASC para los de menor margen (peores).')
    }),
    execute: async (inputData: any) => {
        let sql = '';
        const params: any[] = [];
        const limite = inputData.limite ?? 10;
        const orden = inputData.orden === 'ASC' ? 'ASC' : 'DESC';

        // --- LÓGICA DE FECHAS ---
        let filtroFecha = '';
        if (inputData.fechaInicio && inputData.fechaFin) {
            filtroFecha = `fecha BETWEEN $${params.length + 1} AND $${params.length + 2}`;
            params.push(inputData.fechaInicio, inputData.fechaFin);
        } else if (inputData.fechaInicio) {
            filtroFecha = `fecha >= $${params.length + 1}`;
            params.push(inputData.fechaInicio);
        }
        // ------------------------

        switch (inputData.indicador) {
            case 'rentabilidad_vendedor':
                sql = `SELECT cod_ven, SUM(importe - CMV) AS rentabilidad_bruta, (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) * 100 AS margen_porcentual FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY cod_ven ORDER BY rentabilidad_bruta ${orden} LIMIT ${limite};`;
                break;
            case 'clientes_no_rentables':
                sql = `SELECT Cliente, Razon_Social, SUM(importe - CMV) AS rentabilidad FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Cliente, Razon_Social HAVING SUM(importe - CMV) <= 0 ORDER BY rentabilidad ${orden} LIMIT ${limite};`;
                break;
            case 'clientes_dto_arriba_stnd':
                sql = `SELECT Cliente, Razon_Social, (SUM(d1) / NULLIF(SUM(importe), 0)) * 100 AS pct_descuento_otorgado FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Cliente, Razon_Social HAVING (SUM(d1) / NULLIF(SUM(importe), 0)) > 0.05 ORDER BY pct_descuento_otorgado ${orden} LIMIT ${limite};`;
                break;
            case 'clientes_no_compran_marca':
                if (!inputData.marca) return { error: 'Falta el parámetro marca' };
                params.push(inputData.marca);
                sql = `SELECT DISTINCT Cliente, Razon_Social FROM ventas_detalle WHERE Cliente NOT IN (SELECT Cliente FROM ventas_detalle WHERE linea = $${params.length}) ${filtroFecha ? 'AND ' + filtroFecha : ''} LIMIT ${limite};`;
                break;
            case 'productos_no_rentables':
                sql = `SELECT Art, articulo, SUM(importe - CMV) AS rentabilidad FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Art, articulo HAVING SUM(importe - CMV) <= 0 ORDER BY rentabilidad ${orden} LIMIT ${limite};`;
                break;
            case 'aumentar_margen_sin_caida_ventas':
                sql = `SELECT Art, articulo, SUM(Cantidad) AS volumen_unidades, (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) * 100 AS margen_porcentual FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Art, articulo HAVING SUM(Cantidad) > 1000 AND (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) < 0.15 ORDER BY volumen_unidades ${orden} LIMIT ${limite};`;
                break;
            case 'clientes_campeones':
                sql = `SELECT Cliente, Razon_Social, COUNT(DISTINCT Comprobante) AS frecuencia_compras, SUM(importe) AS valor_monetario, SUM(importe - CMV) AS rentabilidad_actual FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Cliente, Razon_Social ORDER BY frecuencia_compras ${orden}, valor_monetario ${orden} LIMIT ${limite};`;
                break;
            case 'potencial_venta_zonas':
                sql = `SELECT reparto AS zona, COUNT(DISTINCT Cliente) AS clientes_activos, SUM(importe) AS venta_total_zona, SUM(importe - CMV) AS rentabilidad_zona FROM ventas_detalle WHERE reparto IS NOT NULL ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY reparto ORDER BY venta_total_zona ${orden} LIMIT ${limite};`;
                break;
            case 'venta_total':
                sql = `SELECT SUM(importe) AS venta_bruta, SUM(importe) AS venta_facturada FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''};`;
                break;
            case 'venta_por_vendedor':
                sql = `SELECT cod_ven, SUM(importe) AS venta_facturada FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY cod_ven ORDER BY venta_facturada ${orden} LIMIT ${limite};`;
                break;
            case 'venta_por_categoria':
                sql = `SELECT rubro, SUM(importe) AS venta_facturada FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY rubro ORDER BY venta_facturada ${orden} LIMIT ${limite};`;
                break;
            case 'clientes_compradores':
                sql = `SELECT COUNT(DISTINCT Cliente) AS clientes_compradores_unicos FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''};`;
                break;
            case 'venta_marcas_foco':
                if (inputData.marcasFoco && inputData.marcasFoco.length > 0) {
                    params.push(inputData.marcasFoco);
                    sql = `SELECT linea AS marca_foco, SUM(importe) AS venta_facturada FROM ventas_detalle WHERE linea = ANY($${params.length}) ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY linea ORDER BY venta_facturada ${orden} LIMIT ${limite};`;
                } else {
                    // Si no hay marcas foco, traemos las marcas líderes en ventas
                    sql = `SELECT linea AS marca_foco, SUM(importe) AS venta_facturada FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY linea ORDER BY venta_facturada ${orden} LIMIT ${limite};`;
                }
                break;
            case 'rechazos_devoluciones':
                sql = `SELECT motivodev AS motivo_rechazo, COUNT(DISTINCT Comprobante) AS cantidad_facturas_rechazadas, SUM(importe) AS valor_rechazado FROM ventas_detalle WHERE motivodev IS NOT NULL AND motivodev NOT IN ('', '0') ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY motivodev ORDER BY cantidad_facturas_rechazadas ${orden} LIMIT ${limite};`;
                break;
            case 'inversion_dtos':
                sql = `SELECT SUM(d1) AS inversion_total_descuentos FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''};`;
                break;
            case 'pct_venta_sin_dto':
                sql = `SELECT (SUM(CASE WHEN d1 = 0 OR d1 IS NULL THEN importe ELSE 0 END) / NULLIF(SUM(importe), 0)) * 100 AS pct_ventas_sin_descuento FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''};`;
                break;
            case 'margen_por_articulo':
                sql = `SELECT Art, articulo, SUM(importe - CMV) AS margen_dolares, (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) * 100 AS margen_porcentual FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY Art, articulo ORDER BY margen_dolares ${orden} LIMIT ${limite};`;
                break;
            case 'margen_total':
                sql = `SELECT SUM(importe - CMV) AS margen_total_dolares, (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) * 100 AS margen_total_porcentual FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''};`;
                break;
            case 'margen_descuentos_por_proveedor':
                sql = `SELECT linea AS proveedor_proxy, SUM(d1) AS total_descuentos_otorgados, SUM(importe - CMV) AS margen_aportado FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''} GROUP BY linea ORDER BY margen_aportado ${orden} LIMIT ${limite};`;
                break;
            case 'rentabilidad_volumen_subcanal':
                sql = `SELECT subcanal, SUM(importe) AS ingresos_netos, SUM(cmv) AS costo_mercaderia, (SUM(importe) - SUM(cmv)) AS rentabilidad_bruta, ROUND(( (SUM(importe) - SUM(cmv)) / NULLIF(SUM(importe), 0) * 100 )::numeric, 2) AS margen_porcentual, SUM(peso) AS total_kilos_vendidos FROM ventas_detalle WHERE importe > 0 ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY subcanal ORDER BY rentabilidad_bruta ${orden} LIMIT ${limite};`;
                break;
            case 'ticket_promedio_subcanal':
                sql = `SELECT subcanal, COUNT(DISTINCT comprobante) AS cantidad_facturas_emitidas, SUM(importe) AS importe_total, ROUND(( SUM(importe) / NULLIF(COUNT(DISTINCT comprobante), 0) )::numeric, 2) AS ticket_promedio_pesos FROM ventas_detalle WHERE importe > 0 ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY subcanal ORDER BY ticket_promedio_pesos ${orden} LIMIT ${limite};`;
                break;
            case 'ticket_promedio_cliente':
                sql = `SELECT cliente AS cod_cliente, razon_social, subcanal, COUNT(DISTINCT comprobante) AS cantidad_compras, SUM(importe) AS importe_total, ROUND(( SUM(importe) / NULLIF(COUNT(DISTINCT comprobante), 0) )::numeric, 2) AS ticket_promedio_pesos FROM ventas_detalle WHERE importe > 0 ${filtroFecha ? 'AND ' + filtroFecha : ''} GROUP BY cliente, razon_social, subcanal HAVING COUNT(DISTINCT comprobante) > 1 ORDER BY ticket_promedio_pesos ${orden} LIMIT ${limite};`;
                break;
            case 'producto_mas_vendido':
                sql = `
                    SELECT 
                        Art, 
                        articulo, 
                        SUM(Cantidad) AS volumen_unidades,
                        SUM(importe) AS venta_total
                    FROM ventas_detalle 
                    ${filtroFecha ? 'WHERE ' + filtroFecha : ''}
                    GROUP BY Art, articulo 
                    ORDER BY volumen_unidades ${orden} 
                    LIMIT ${limite};
                `;
                break;
            case 'evolucion_precio_promedio':
                sql = `
                    SELECT 
                        Art, 
                        articulo, 
                        SUM(importe) AS venta_total, 
                        SUM(Cantidad) AS unidades_vendidas,
                        (SUM(importe) / NULLIF(SUM(Cantidad), 0)) AS precio_promedio
                    FROM ventas_detalle
                    ${filtroFecha ? 'WHERE ' + filtroFecha : ''}
                    GROUP BY Art, articulo
                    ORDER BY precio_promedio ${orden}
                    LIMIT ${limite};
                `;
                break;
            case 'indice_concentracion_pareto':
                sql = `
                    SELECT 
                        Cliente, 
                        Razon_Social, 
                        SUM(importe) AS venta_cliente,
                        (SUM(importe) / NULLIF((SELECT SUM(importe) FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''}), 0)) * 100 AS porcentaje_del_total_empresa
                    FROM ventas_detalle
                    ${filtroFecha ? 'WHERE ' + filtroFecha : ''}
                    GROUP BY Cliente, Razon_Social
                    ORDER BY venta_cliente DESC
                    LIMIT ${limite};
                `;
                break;
            case 'cumplimiento_mix_ideal':
                sql = `
                    SELECT 
                        linea AS categoria, 
                        SUM(importe) AS venta_total,
                        (SUM(importe) / NULLIF((SELECT SUM(importe) FROM ventas_detalle ${filtroFecha ? 'WHERE ' + filtroFecha : ''}), 0)) * 100 AS peso_en_el_mix,
                        (SUM(importe - CMV) / NULLIF(SUM(importe), 0)) * 100 AS margen_porcentual
                    FROM ventas_detalle
                    ${filtroFecha ? 'WHERE ' + filtroFecha : ''}
                    GROUP BY linea
                    ORDER BY peso_en_el_mix ${orden}
                    LIMIT ${limite};
                `;
                break;
            case 'evolucion_diaria_ventas':
            sql = `
                SELECT 
                    to_char(fecha, 'DD/MM/YYYY') AS fecha_formateada, 
                    SUM(importe) AS venta_neta, 
                    SUM(importe - cmv) AS rentabilidad_bruta, 
                    ROUND(( (SUM(importe) - SUM(cmv)) / NULLIF(SUM(importe), 0) * 100 )::numeric, 2) AS margen_porcentual 
                FROM ventas_detalle 
                WHERE importe > 0 
                ${filtroFecha ? 'AND ' + filtroFecha : ''} 
                GROUP BY fecha 
                ORDER BY fecha ASC 
                LIMIT ${limite};
            `;
                break;
            case 'evolucion_mensual_ventas':
            sql = `
                SELECT 
                    TO_CHAR(fecha, 'YYYY-MM') AS mes, 
                    SUM(importe) AS venta_neta, 
                    SUM(importe - cmv) AS rentabilidad_bruta, 
                    ROUND(( (SUM(importe) - SUM(cmv)) / NULLIF(SUM(importe), 0) * 100 )::numeric, 2) AS margen_porcentual 
                FROM ventas_detalle 
                WHERE importe > 0 
                ${filtroFecha ? 'AND ' + filtroFecha : ''} 
                GROUP BY TO_CHAR(fecha, 'YYYY-MM') 
                ORDER BY mes ASC 
                LIMIT ${limite};
            `;
            break;
            case 'ganancia_por_categoria':
            sql = `
                SELECT 
                    linea AS categoria, 
                    SUM(facturacion) AS venta_neta, 
                    SUM(facturacion - cmv) AS ganancia_dinero,
                    ROUND(((SUM(facturacion - cmv) / NULLIF(SUM(facturacion), 0)) * 100)::numeric, 2) AS margen_porcentual
                FROM ventas_detalle 
                WHERE facturacion > 0 
                ${filtroFecha ? 'AND ' + filtroFecha : ''} 
                GROUP BY linea 
                ORDER BY ganancia_dinero DESC 
                LIMIT ${limite};
            `;
            break;
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

