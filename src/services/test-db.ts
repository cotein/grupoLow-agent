import { Pool } from 'pg';
import 'dotenv/config';

async function testConnection() {
  const connectionString = process.env.DATABASE_URL;

  if (!connectionString || connectionString.includes('[YOUR_')) {
    console.error('❌ ERROR: DATABASE_URL is not configured in .env');
    process.exit(1);
  }

  console.log('--- Testing Database Connection ---');
  console.log(`Target: ${connectionString.split('@')[1] || connectionString}`);

  const pool = new Pool({ connectionString });

  try {
    const client = await pool.connect();
    console.log('✅ SUCCESS: Connected to Supabase/PostgreSQL!');
    
    const res = await client.query('SELECT version()');
    console.log(`Database Version: ${res.rows[0].version}`);
    
    // Check for pgvector
    const vectorRes = await client.query("SELECT * FROM pg_extension WHERE extname = 'vector'");
    if (vectorRes.rows.length > 0) {
      console.log('✅ SUCCESS: pgvector extension is enabled.');
    } else {
      console.warn('⚠️ WARNING: pgvector extension is NOT enabled. You may need to run: CREATE EXTENSION IF NOT EXISTS vector;');
    }

    client.release();
  } catch (err) {
    console.error('❌ ERROR: Could not connect to the database.');
    console.error(err);
  } finally {
    await pool.end();
  }
}

testConnection();
