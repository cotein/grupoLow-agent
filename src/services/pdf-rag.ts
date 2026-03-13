/**
 * pdf-rag.ts
 *
 * Script to read a PDF file, extract text, chunk it using MDocument,
 * embed the chunks using OpenAI via Mastra's model router, and 
 * upsert the vectors into a local PostgreSQL database using pgvector.
 *
 * Requirements:
 * - npm install @mastra/rag @mastra/core @mastra/pg @ai-sdk/openai ai pdf-parse
 * - OPENAI_API_KEY environment variable set
 * - Local Postgres running with pgvector extension
 * 
 * Usage:
 * - npx tsx src/services/pdf-rag.ts ./path/to/document.pdf
 */

import * as fs from 'fs';
import * as path from 'path';
import 'dotenv/config';

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const pdfParse = require('pdf-parse');

import { MDocument } from '@mastra/rag';
import { embedMany } from 'ai';
import { ModelRouterEmbeddingModel } from '@mastra/core/llm';
import { PgVector } from '@mastra/pg';

// ─────────────────────────────────────────────────────────────────────────────
// CONFIGURATION
// ─────────────────────────────────────────────────────────────────────────────

const CONFIG = {
  db: {
    // Supabase usually provides a full DATABASE_URL
    connectionString: process.env.DATABASE_URL,
  },
  chunking: {
    strategy: 'recursive' as const,
    maxSize: Number(process.env.CHUNK_MAX_SIZE ?? 512),
    overlap: Number(process.env.CHUNK_OVERLAP ?? 50),
  },
  embedding: {
    modelStr: process.env.EMBEDDING_MODEL ?? 'openai/text-embedding-3-small',
  },
  vectorStore: {
    indexName: process.env.VECTOR_INDEX_NAME ?? 'pdf_embeddings',
  }
};

const log = {
  info:  (msg: string, meta?: any) => console.log (`[INFO]  ${new Date().toISOString()} ${msg}`, meta ? meta : ""),
  error: (msg: string, meta?: any) => console.error(`[ERROR] ${new Date().toISOString()} ${msg}`, meta ? meta : ""),
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN PROCESS
// ─────────────────────────────────────────────────────────────────────────────

async function main() {
  const filePathStr = process.argv[2];

  if (!filePathStr) {
    console.error("\nUsage: npx tsx src/services/pdf-rag.ts <path-to-pdf>\n");
    console.error("Required env vars:");
    console.error("  OPENAI_API_KEY");
    process.exit(1);
  }

  const filePath = path.resolve(filePathStr);
  log.info(`Checking file: ${filePath}`);

  if (!fs.existsSync(filePath)) {
    log.error(`File does not exist: ${filePath}`);
    process.exit(1);
  }

  try {
    // 1. Read PDF and extract text
    log.info(`Reading PDF file and extracting text...`);
    const dataBuffer = fs.readFileSync(filePath);
    const pdfData = await pdfParse(dataBuffer);
    
    // Sanitize text: Remove NULL characters (\u0000) which Postgres does not support in text/jsonb fields
    const pdfText = (pdfData.text || '').replace(/\u0000/g, '');
    
    if (!pdfText || pdfText.trim().length === 0) {
      log.error("No text could be extracted from the PDF.");
      process.exit(1);
    }
    
    log.info(`Successfully extracted ${pdfText.length} characters.`);

    // 2. Initialize MDocument and Chunking
    log.info(`Chunking document using strategy '${CONFIG.chunking.strategy}'...`);
    const mDoc = MDocument.fromText(pdfText);
    const chunks = await mDoc.chunk({
      strategy: CONFIG.chunking.strategy,
      maxSize: CONFIG.chunking.maxSize,
      overlap: CONFIG.chunking.overlap,
    });
    
    log.info(`Created ${chunks.length} chunks.`);
    
    if (chunks.length === 0) {
       log.error("No chunks generated. Check your chunking parameters.");
       process.exit(1);
    }

    // 3. Generate Embeddings
    log.info(`Generating embeddings using model '${CONFIG.embedding.modelStr}'...`);
    if(!process.env.OPENAI_API_KEY) {
        log.error("OPENAI_API_KEY environment variable is not set. Embedding may fail.");
    }

    const { embeddings } = await embedMany({
      values: chunks.map(chunk => chunk.text),
      model: new ModelRouterEmbeddingModel(CONFIG.embedding.modelStr),
    });

    log.info(`Generated ${embeddings.length} embeddings.`);

    // 4. Store in Postgres using pgvector
    const maskedConn = CONFIG.db.connectionString?.replace(/:([^@]+)@/, ':****@');
    log.info(`Connecting to Postgres vector store with: ${maskedConn}`);
    
    const pgVector = new PgVector({
      id: 'pdf-rag-pg-vector',
      connectionString: CONFIG.db.connectionString,
    });
    
    log.info(`Creating vector index/table '${CONFIG.vectorStore.indexName}' (if it doesn't exist)...`);
    // 'text-embedding-3-small' creates vectors of dimension 1536 by default.
    // If you used options { dimensions: 256 }, you'd put 256 here.
    await pgVector.createIndex({
      indexName: CONFIG.vectorStore.indexName,
      dimension: 1536,
      metric: 'cosine'
    });

    log.info(`Upserting embeddings into index '${CONFIG.vectorStore.indexName}'...`);
    
    // We map chunks & embeddings together into the format pgVector.upsert expects.
    // Assuming upsert requires an array of { id, vector, metadata, content } or similar
    // The chunk objects from MDocument might need mapping.
    
    // Let's create an array of formatted values.
    // The exact signature for Mastra PgVector.upsert usually requires:
    // { indexName: string, vectors: number[][], metadata?: object[] } 
    // Wait, the docs say:
    // pgVector.upsert({ indexName: 'embeddings', vectors: embeddings })
    // If it requires mapping the original text, let's include metadata if supported.
    
    const vectorData = embeddings.map((vector, i) => ({
      vector,
      metadata: {
        text: chunks[i]?.text,
        source: path.basename(filePath),
        chunkIndex: i,
      }
    }));

    // Some upsert signatures are generic:
    // upsert({ indexName: string, vectors: number[][] })
    // We'll follow the exact syntax from docs as a baseline:
    // await pgVector.upsert({ indexName: 'embeddings', vectors: embeddings })
    // Note: To make metadata searchable, we insert them manually or hope upsert accepts metadata.
    // We will use the standard object format for upsert as documented.
    
    const BATCH_SIZE = 50;
    log.info(`Upserting ${embeddings.length} embeddings in batches of ${BATCH_SIZE}...`);

    for (let i = 0; i < embeddings.length; i += BATCH_SIZE) {
      const batchVectors = embeddings.slice(i, i + BATCH_SIZE);
      const batchMetadata = vectorData.slice(i, i + BATCH_SIZE).map(v => v.metadata);
      
      const currentBatch = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(embeddings.length / BATCH_SIZE);
      
      log.info(`[${currentBatch}/${totalBatches}] Upserting ${batchVectors.length} vectors...`);
      
      await pgVector.upsert({
        indexName: CONFIG.vectorStore.indexName,
        vectors: batchVectors,
        metadata: batchMetadata
      });
    }

    log.info("Finished successfully!");
    
    // Clean up connections if necessary (e.g. close the pool)
    
  } catch (error) {
    log.error("Error during processing:", error);
    process.exit(1);
  }
}

main();
