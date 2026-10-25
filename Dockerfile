# P2Ply signaling server.
#   docker build -t p2ply-signaling .
#   docker run --rm -p 8787:8787 p2ply-signaling

FROM node:22-alpine AS build
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY tsconfig.json tsup.config.ts ./
COPY src ./src
RUN pnpm build

FROM node:22-alpine
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml .npmrc ./
RUN pnpm install --prod --frozen-lockfile --ignore-scripts
COPY --from=build /app/dist ./dist
ENV NODE_ENV=production P2PLY_HOST=0.0.0.0 P2PLY_PORT=8787
EXPOSE 8787
USER node
HEALTHCHECK CMD wget -qO- http://127.0.0.1:8787/health || exit 1
CMD ["node", "dist/server/cli.js"]
