import pkg from 'pg';
const { Pool } = pkg;
import 'dotenv/config';

async function applyAdditonalIndices() {
    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });

    try {
        console.log('⏳ Applying additional indices to Supabase...');
        await pool.query(`
            CREATE INDEX IF NOT EXISTS idx_ventas_razon_social ON public.ventas_detalle(razon_social);
            CREATE INDEX IF NOT EXISTS idx_ventas_subcanal ON public.ventas_detalle(subcanal);
            CREATE INDEX IF NOT EXISTS idx_ventas_chofer ON public.ventas_detalle(chofer);
            CREATE INDEX IF NOT EXISTS idx_ventas_segmentoproducto ON public.ventas_detalle(segmentoproducto);
            CREATE INDEX IF NOT EXISTS idx_ventas_linea ON public.ventas_detalle(linea);
            CREATE INDEX IF NOT EXISTS idx_ventas_reparto ON public.ventas_detalle(reparto);
        `);
        console.log('✅ Additional indices applied successfully!');
    } catch (error) {
        console.error('❌ Error applying indices:', error);
    } finally {
        await pool.end();
    }
}

applyAdditonalIndices();
