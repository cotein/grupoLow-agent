import pkg from 'pg';
const { Pool } = pkg;
import 'dotenv/config';

async function test(url, label) {
  console.log(`\n--- Testing ${label} ---`);
  console.log(`URL (masked): ${url?.replace(/:([^@]+)@/, ':****@')}`);
  
  const pool = new Pool({ 
    connectionString: url,
    ssl: { rejectUnauthorized: false }
  });

  try {
    const start = Date.now();
    const client = await pool.connect();
    const res = await client.query('SELECT current_user, current_database()');
    console.log(`✅ SUCCESS [${Date.now() - start}ms]:`, res.rows[0]);
    
    try {
      const countRes = await client.query('SELECT count(*) FROM pdf_embeddings');
      console.log(`📊 Current row count in pdf_embeddings: ${countRes.rows[0].count}`);
    } catch (e) {
      console.log('ℹ️ Table pdf_embeddings not ready or empty yet.');
    }
    
    client.release();
    return true;
  } catch (err) {
    console.error(`❌ FAILED: ${err.message}`);
    return false;
  } finally {
    await pool.end();
  }
}

async function run() {
  const currentUrl = process.env.DATABASE_URL;
  await test(currentUrl, "Current Connection (Pooler)");

  // Try to construct Direct Connection URL if possible
  if (currentUrl && currentUrl.includes('pooler.supabase.com')) {
    const match = currentUrl.match(/postgres\.([^:]+):([^@]+)@aws-1-us-east-2\.pooler\.supabase\.com:6543\/(.+)/);
    if (match) {
      const [_, projectRef, password, dbName] = match;
      const directUrl = `postgresql://postgres:${password}@db.${projectRef}.supabase.co:5432/${dbName}`;
      await test(directUrl, "Direct Connection (Port 5432)");
    }
  }
}

run();
