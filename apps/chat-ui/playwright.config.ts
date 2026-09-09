import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./tests/e2e", fullyParallel: false,
  use: { baseURL: "http://127.0.0.1:3000", headless: true, channel: process.env.PLAYWRIGHT_CHANNEL || undefined },
  webServer: [
    { command: "node tests/mock-backend.mjs", url: "http://127.0.0.1:18080/health", reuseExistingServer: false },
    { command: "pnpm start", url: "http://127.0.0.1:3000", reuseExistingServer: false, env: { SELF_HEALTHY_KAFKA_CHAT_API_URL: "http://127.0.0.1:18080/api/v1/chat", CHAT_API_TOKEN: "ui-e2e-server-only-secret", CHAT_API_TIMEOUT_MS: "10000" } },
  ],
});
