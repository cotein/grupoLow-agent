import pkg from 'pg';
const { Pool } = pkg;
import fs from 'fs';
import 'dotenv/config';

async function applySchema() {
    const sql = fs.readFileSync('./database/sales_schema.sql', 'utf8');
    const pool = new Pool({
        connectionString: process.env.DATABASE_URL,
        ssl: { rejectUnauthorized: false }
    });

    try {
        console.log('⏳ Applying SQL schema to Supabase...');
        await pool.query(sql);
        console.log('✅ Schema applied successfully!');
    } catch (error) {
        console.error('❌ Error applying schema:', error);
    } finally {
        await pool.end();
    }
}

applySchema();
