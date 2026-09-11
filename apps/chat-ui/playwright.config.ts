import { defineConfig } from "@playwright/test";
const uiPort=Number(process.env.PLAYWRIGHT_UI_PORT??"3000");
const backendPort=Number(process.env.PLAYWRIGHT_BACKEND_PORT??"18080");
export default defineConfig({
  testDir: "./tests/e2e", fullyParallel: false,
  use: { baseURL: `http://127.0.0.1:${uiPort}`, headless: true, channel: process.env.PLAYWRIGHT_CHANNEL || undefined },
  webServer: [
    { command: "node tests/mock-backend.mjs", url: `http://127.0.0.1:${backendPort}/health`, reuseExistingServer: false, env:{MOCK_BACKEND_PORT:String(backendPort)} },
    { command: `pnpm exec next start --hostname 127.0.0.1 --port ${uiPort}`, url: `http://127.0.0.1:${uiPort}`, reuseExistingServer: false, env: { SELF_HEALTHY_KAFKA_CHAT_API_URL: `http://127.0.0.1:${backendPort}/api/v1/chat`, CHAT_API_TOKEN: "ui-e2e-server-only-secret", CHAT_API_TIMEOUT_MS: "10000" } },
  ],
});
