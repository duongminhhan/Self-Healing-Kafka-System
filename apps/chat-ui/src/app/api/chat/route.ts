import "server-only";
import { handleChat } from "@/lib/bff";
export const runtime="nodejs";
export async function POST(request:Request) {
  const configured=Number(process.env.CHAT_API_TIMEOUT_MS??60000);
  return handleChat(request,{
    url:process.env.SELF_HEALTHY_KAFKA_CHAT_API_URL,
    token:process.env.CHAT_API_TOKEN,
    timeoutMs:Number.isFinite(configured)?Math.max(1000,Math.min(configured,180000)):60000,
  },fetch,(entry)=>console.info(JSON.stringify(entry)));
}
