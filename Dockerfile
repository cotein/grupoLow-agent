ARG NODE_VERSION=24.0.0

# --- Etapa 1: Build ---
FROM node:${NODE_VERSION}-bookworm-slim as builder
WORKDIR /app

# Solo copiamos los archivos necesarios para instalar dependencias
COPY package*.json ./
RUN npm install

# Copiamos el código fuente
COPY src ./src
COPY tsconfig.json ./

# Copiar archivos de configuración si existen (opcional)
COPY .env.example .env.example* ./

RUN npm run build

# --- Etapa 2: Runner ---
FROM node:${NODE_VERSION}-bookworm-slim as runner
WORKDIR /app

ENV NODE_ENV=production

COPY package*.json ./
RUN npm install --only=production

# Copiar build de la etapa anterior
COPY --from=builder /app/.mastra ./.mastra

# El puerto por defecto de Mastra es 4111.
# El docker-compose mapea 4113:4111.
EXPOSE 4111

CMD ["npx", "mastra", "start"]
