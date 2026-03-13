import { Pool } from 'pg';
import fs from 'fs';
import 'dotenv/config';

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

async function run() {
    try {
        const sql = fs.readFileSync('database/sales_schema.sql', 'utf8');
        console.log('Executing schema...');
        await pool.query(sql);
        console.log('Schema executed successfully.');
    } catch (e) {
        console.error('Error executing schema:', e);
    } finally {
        await pool.end();
    }
}

run();
