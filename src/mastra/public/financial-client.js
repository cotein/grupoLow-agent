/**
 * Helper client to consume financial indicators from the frontend.
 */
export async function fetchIndicatorData({ 
    indicador, 
    limite = 10, 
    orden = 'DESC', 
    fechaInicio, 
    fechaFin,
    marca,
    marcasFoco,
    baseUrl = 'http://localhost:4111' // Port where Mastra is running
}) {
    const API_URL = `${baseUrl}/indicators`;
    
    try {
        const response = await fetch(API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                indicador,
                limite,
                orden,
                fechaInicio,
                fechaFin,
                marca,
                marcasFoco
            }),
        });

        if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.error || 'Error fetching indicators');
        }

        return await response.json();
    } catch (error) {
        console.error('Financial Client Error:', error);
        throw error;
    }
}

/**
 * Examples of usage:
 * 
 * 1. Ranking of profitable sellers:
 * const data = await fetchIndicatorData({ indicador: 'rentabilidad_vendedor', limite: 5 });
 * 
 * 2. Daily sales evolution with dates:
 * const history = await fetchIndicatorData({ 
 *    indicador: 'evolucion_diaria_ventas', 
 *    fechaInicio: '2024-01-01', 
 *    fechaFin: '2024-01-31' 
 * });
 */
