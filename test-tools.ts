import { getHealthMetrics, getRFMAnalysis, getBreakevenPoint, getAdditionalKPIs, getSalesAggregations } from './src/mastra/tools/financial-tools.js';

async function runTests() {
    try {
        console.log('--- TEST: getHealthMetrics ---');
        const health = await getHealthMetrics.execute!({}, {}); 
        console.log(health);

        console.log('\n--- TEST: getRFMAnalysis (limit 3) ---');
        const rfm = await getRFMAnalysis.execute!({ limit: 3 }, {}); 
        console.log(rfm);

        console.log('\n--- TEST: getBreakevenPoint (costosFijos = 5000000) ---');
        const bep = await getBreakevenPoint.execute!({ costosFijos: 5000000 }, {});
        console.log(bep);

        console.log('\n--- TEST: getAdditionalKPIs ---');
        const kpis = await getAdditionalKPIs.execute!({}, {});
        console.log(kpis);

        console.log('\n--- TEST: getSalesAggregations (groupBy = cod_ven, orderBy = margen, limit = 3) ---');
        const aggsVendedores = await getSalesAggregations.execute!({ groupBy: 'cod_ven', orderBy: 'margen', limit: 3 }, {});
        console.log(aggsVendedores);

        console.log('\n--- TEST: getSalesAggregations (groupBy = rubro, limit = 2) ---');
        const aggsRubro = await getSalesAggregations.execute!({ groupBy: 'rubro', limit: 2 }, {});
        console.log(aggsRubro);
        
    } catch (err) {
        console.error("Test failed:", err);
    }
    
    process.exit(0);
}

runTests();
