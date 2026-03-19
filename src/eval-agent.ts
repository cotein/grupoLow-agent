import crypto from 'crypto';
if (!global.crypto) {
  global.crypto = crypto as any;
}
import { Mastra } from '@mastra/core';
import { createScorer } from '@mastra/core/evals';
import { PostgresStore } from '@mastra/pg';
import { financialAnalystAgent } from './mastra/agents/financial-agent'; // Ajusta la ruta a tu agente

async function runEvaluation() {
  const accuracyScorer = createScorer({
    id: 'accuracy',
    description: 'Checks tool calls, expected phrases, and forbidden words',
  }).generateScore(({ run }) => {
    const output = run.output as any;
    const groundTruth = run.groundTruth as any;

    if (!groundTruth) return 1;

    const text = output?.text || output?.content?.content || (typeof output?.content === 'string' ? output.content : '') || '';
    const toolCalls = output?.toolInvocations || output?.content?.toolInvocations || output?.toolCalls || [];
    
    let score = 1;

    if (groundTruth.should_call_tool) {
      const calledSpecificTool = toolCalls.some((tc: any) => tc.toolName === groundTruth.should_call_tool || tc.name === groundTruth.should_call_tool);
      if (!calledSpecificTool) score -= 0.5;
    }

    if (groundTruth.expected_answer_contains) {
      if (!text.includes(groundTruth.expected_answer_contains)) score -= 0.25;
    }

    if (groundTruth.forbidden_words && Array.isArray(groundTruth.forbidden_words)) {
      const hasForbidden = groundTruth.forbidden_words.some((word: string) => text.includes(word));
      if (hasForbidden) score -= 0.25;
    }

    return Math.max(0, score);
  });

  const mastra = new Mastra({
    agents: { financialAnalystAgent },
    storage: new PostgresStore({
      id: "eval-store",
      connectionString: process.env.DATABASE_URL!,
    }),
    scorers: {
      accuracy: accuracyScorer,
    },
  });

  console.log('🚀 Iniciando creación de Dataset...');

  // 1. Crear o recuperar el Dataset
  const dataset = await mastra.datasets.create({
    name: 'Evaluación de Alucinaciones Financieras',
    description: 'Casos críticos para verificar que el agente no invente datos cuando la DB está vacía',
  });

  // 2. Agregar los ítems (Casos de prueba)
  await dataset.addItems({
    items: [
      {
        input: '¿Cómo va la línea Snac en marzo?',
        // El groundTruth es lo que DEBE pasar para que el test sea exitoso
        groundTruth: {
          should_call_tool: 'getProfitabilityAnalysis',
          expected_answer_contains: 'No se registran movimientos',
          forbidden_words: ['$78.512', '69,8%'] // Si dice estos números inventados, falla
        }
      },
      {
        input: 'Dame el margen del vendedor Gomez',
        groundTruth: {
          should_call_tool: 'getProfitabilityAnalysis',
        }

      }
    ],
  });

  console.log('🧪 Ejecutando Experimento...');

  // 3. Ejecutar el Experimento
  // Esto hará que el agente "piense" cada pregunta y Mastra evalúe la respuesta
  const summary = await dataset.startExperiment({
    targetType: 'agent',
    targetId: 'financial-analyst-agent',
    scorers: ['accuracy'], // Mastra usará el evaluador de precisión por defecto
  });

  console.log('--- RESULTADOS DEL EXPERIMENTO ---');
  console.log(`✅ Exitosos: ${summary.succeededCount}`);
  console.log(`❌ Fallidos: ${summary.totalItems - summary.succeededCount}`);
  console.log(`📊 Total: ${summary.totalItems}`);
  
  // Puedes ver el detalle en Mastra Studio después de correr esto
  console.log('Revisa los detalles en Mastra Studio: http://localhost:4111');
}

runEvaluation().catch(console.error);