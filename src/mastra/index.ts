import { Mastra } from '@mastra/core/mastra';
import { PinoLogger } from '@mastra/loggers';
import { PostgresStore } from '@mastra/pg';
import { Observability, DefaultExporter, CloudExporter, SensitiveDataFilter } from '@mastra/observability';
import { financialAnalystAgent } from './agents/financial-agent';
import { registerApiRoute } from '@mastra/core/server';

const DEMO_RESOURCE_ID = "DEMO_USER_FINANCIERO_COTO";
const DEMO_THREAD_ID = "DEMO_THREAD_ESTRATEGICO_001";

export const mastra = new Mastra({
  workflows: {},
  agents: { financialAnalystAgent },
  scorers: {},
  storage: new PostgresStore({
    id: "mastra-storage",
    // Connecting to Supabase (or local Postgres via DATABASE_URL)
    connectionString: process.env.DATABASE_URL || "postgresql://postgres:postgres@localhost:5432/postgres",
  }),
  
  logger: new PinoLogger({
    name: 'Mastra',
    level: 'debug',
  }),
  observability: new Observability({
    configs: {
      default: {
        serviceName: 'mastra',
        exporters: [
          new DefaultExporter(), // Persists traces to storage for Mastra Studio
          new CloudExporter(), // Sends traces to Mastra Cloud (if MASTRA_CLOUD_ACCESS_TOKEN is set)
        ],
        spanOutputProcessors: [
          new SensitiveDataFilter(), // Redacts sensitive data like passwords, tokens, keys
        ],
      },
    },
  }),
  server:{
    host: '0.0.0.0',
    port: 4111,
    middleware: [
      async (c, next) => {
        // Reemplaza '*' por el dominio de tu Nuxt en producción para mayor seguridad
        c.res.headers.set('Access-Control-Allow-Origin', '*'); 
        c.res.headers.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        c.res.headers.set('Access-Control-Allow-Headers', 'Content-Type');
        
        // Responder a la solicitud "preflight" del navegador
        if (c.req.method === 'OPTIONS') {
          return new Response(null, { status: 204 });
        }
        
        await next();
      },
    ],
    apiRoutes: [
      registerApiRoute('/test', {
        method: 'POST',
        handler: async c => {
          // 1. Extraemos el mensaje que envía el usuario y el ID de su hilo si ya inició conversación
          const body = await c.req.json();
          const mensajeUsuario = body.message || "Hola";
          let threadId = body.threadId;
          const clientId = body.clientId; // Para no duplicar pacientes
          console.log('Body indicators request:', JSON.stringify(body, null, 2));
          // 2. Instanciamos a tu agente
          const mastra = c.get('mastra');
          const agent = mastra.getAgent('financialAnalystAgent');
          const memory = await agent.getMemory();

          if (memory) {
            try {
              // Verificamos si el hilo ya existe en la base de datos

                // Si no existe, lo creamos vinculado a nuestro recurso hardcodeado
                await memory.createThread({
                  resourceId: DEMO_RESOURCE_ID,
                  threadId: DEMO_THREAD_ID,
                  title: "Hilo Permanente de Demo"
                });
                console.log(`✅ Hilo de demo creado: ${DEMO_THREAD_ID}`);
            } catch (error) {
              // En demos, si falla el check pero el hilo existe, procedemos
              console.log("ℹ️ Info: El hilo ya está operativo.");
            }
          }

          // 4. Calculamos la hora de Argentina y el saludo dinámicamente
          const ahora = new Intl.DateTimeFormat('es-AR', {
            timeZone: 'America/Argentina/Buenos_Aires',
            dateStyle: 'full',
            timeStyle: 'medium',
            hour12: false
          }).format(new Date());
          const hora = new Date().toLocaleString('es-AR', { timeZone: 'America/Argentina/Buenos_Aires', hour: '2-digit', hour12: false });
          const horaNum = parseInt(hora);
          let momentoDia = "¡Buenas noches!";
          if (horaNum >= 5 && horaNum < 14) momentoDia = "¡Buen día!";
          else if (horaNum >= 14 && horaNum < 20) momentoDia = "¡Buenas tardes!";

          // 5. Inyectamos la instrucción estructurada de forma invisible usando el parámetro 'system'
          let instructionDinamica = `La fecha y hora actual en Argentina es ${ahora}. Si este es el primer mensaje de la conversación, es obligatorio que saludes al paciente diciendo exactamente "${momentoDia}"`;
          
          
          console.log('Body indicators request:', JSON.stringify(mensajeUsuario, null, 2));
          // 6. Generamos la respuesta con IA inyectándole el contexto por debajo
          const response = await agent.generate(mensajeUsuario, {
             memory: {
                 thread: threadId,
                 resource: body.resourceId || "financiero"
             },
             toolChoice: 'auto',
             instructions: instructionDinamica
          });

          
          // 7. Devolvemos el texto final Y TAMBIÉN el identificador del hilo actual, 
          // el frontend debe encargarse de conservar ese 'threadId' y enviarlo en el próximo POST!
          return c.json({
               respuesta: response.text, 
               threadId: threadId 
          });
        },
      }),
      registerApiRoute('/indicators', {
        method: 'POST',
        handler: async c => {
          const body = await c.req.json();
          console.log('Body indicators request:', JSON.stringify(body, null, 2));
          const mastra = c.get('mastra');
          const tool = mastra.getTool('consultarIndicadores');

          if (!tool) {
            return c.json({ error: 'Herramienta consultarIndicadores no encontrada' }, 500);
          }

          try {
            // @ts-ignore - Mastra tools are guaranteed to have execute when coming from getTool
            const results = await tool.execute({
                indicador: body.indicador,
                limite: body.limite || 10,
                orden: body.orden || 'DESC',
                fechaInicio: body.fechaInicio,
                fechaFin: body.fechaFin,
                marca: body.marca,
                marcasFoco: body.marcasFoco
            });
            return c.json(results);
          } catch (error: any) {
            return c.json({ error: error.message }, 500);
          }
        }
      }),
    ],
  }
});
