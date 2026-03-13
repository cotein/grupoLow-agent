import { Pool } from 'pg';
import 'dotenv/config';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function run() {
    try {
        const res = await pool.query('SELECT COUNT(*) FROM ventas_detalle;');
        console.log('Current row count:', res.rows[0].count);
    } catch (e) {
        console.error('Error:', e);
    } finally {
        await pool.end();
    }
}

run();
