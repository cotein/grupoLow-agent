import pkg from 'pg';
const { Pool } = pkg;
import 'dotenv/config';

async function testParams() {
  // Extracting from the current URL in .env
  const rawUrl = process.env.DATABASE_URL?.replace(/^"|"$/g, ''); // strip quotes if any
  const urlParams = new URL(rawUrl);

  const config = {
    user: urlParams.username,
    password: urlParams.password,
    host: urlParams.hostname,
    port: parseInt(urlParams.port),
    database: urlParams.pathname.split('/')[1],
    ssl: { rejectUnauthorized: false }
  };

  console.log('--- Testing with Separate Parameters ---');
  console.log(`User: ${config.user}`);
  console.log(`Host: ${config.host}`);
  console.log(`Port: ${config.port}`);
  console.log(`Pass: **** (Length: ${config.password?.length})`);

  const pool = new Pool(config);

  try {
    const client = await pool.connect();
    console.log('✅ SUCCESS!');
    client.release();
  } catch (err) {
    console.error(`❌ FAILED: ${err.message}`);
    if (err.message.includes('password authentication failed')) {
      console.log('TIP: Check if special characters in password need URL encoding or if the password is correct.');
    }
  } finally {
    await pool.end();
  }
}

testParams();
