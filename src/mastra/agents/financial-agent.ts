import { Agent } from '@mastra/core/agent';
import { createVectorQueryTool } from '@mastra/rag';
import { ModelRouterEmbeddingModel } from '@mastra/core/llm';
import { PGVECTOR_PROMPT } from '@mastra/pg';
import { instructions } from '../prompts/financial-prompt';
import { Memory } from "@mastra/memory";
import { PostgresStore } from '@mastra/pg';
import { z } from 'zod';
// 1. Create the vector query tool to search the PDF embeddings
export const pdfVectorQueryTool = createVectorQueryTool({
  vectorStoreName: 'pgVector',
  indexName: process.env.VECTOR_INDEX_NAME || 'pdf_embeddings',
  model: new ModelRouterEmbeddingModel(process.env.EMBEDDING_MODEL || 'openai/text-embedding-3-small'),
});

const perfilEstrategicoSchema = z.object({
  targetRentabilidad: z.string().optional()
    .describe("Margen u objetivo de rentabilidad esperado por la directiva (ej. '> 15%')"),
  marcasFoco: z.array(z.string()).optional()
    .describe("Lista de marcas principales a impulsar o proteger este trimestre"),
  objetivosTrimestre: z.array(z.string()).optional()
    .describe("Metas clave del negocio discutidas con el usuario"),
  contextoCritico: z.string().optional()
    .describe("Cualquier otro detalle vital sobre la situación actual de la empresa (ej. 'crisis de stock', 'expansión de zona')")
});

const agentMemory = new Memory({
  storage: new PostgresStore({
    id: 'memoria-financiera',
    connectionString: process.env.DATABASE_URL, 
  }),
  options: {
    workingMemory: {
      enabled: true,
      scope: "resource",
      schema: perfilEstrategicoSchema 
    },
    lastMessages: 15 
  }
});

// 2. Create the Agent that uses the tool
/*export const pdfRagAgent = new Agent({
  id: 'pdf-rag-agent',
  name: 'Asistente de Documentos PDF RAG',
  instructions: `
  Eres un asistente experto. Utiliza la herramienta de búsqueda para obtener contexto de los documentos PDF de la base de datos y responde a las preguntas del usuario basándote en ese contexto.
  Si no encuentras la respuesta en el contexto, díselo al usuario claramente. Responde siempre de forma amable y en español.
  
  ${PGVECTOR_PROMPT}
  `,
  model: 'openai/gpt-4o',
  tools: { pdfVectorQueryTool },
});*/

import { getHealthMetrics, getRFMAnalysis, getBreakevenPoint, getAdditionalKPIs, getSalesAggregations, consultarIndicadores } from '../tools/financial-tools';

// 3. Agente Especialista en Finanzas y Ventas
export const financialAnalystAgent = new Agent({
  id: 'financial-analyst-agent',
  name: 'Analista Comercial y Financiero',
  model: 'openai/gpt-4o',
  tools: { 
    consultarIndicadores
  },
  memory: agentMemory,
  instructions: `
  ${instructions}

  ${PGVECTOR_PROMPT}
  `
});
