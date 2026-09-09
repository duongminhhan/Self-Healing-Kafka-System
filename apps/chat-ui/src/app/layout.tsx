import type { Metadata } from "next";
import "./globals.css";
export const metadata:Metadata={title:"Healing · Self Healthy Kafka",description:"Trợ lý phân tích sự cố và hướng dẫn Kafka Connect"};
export default function Layout({children}:Readonly<{children:React.ReactNode}>) {
  return <html lang="vi"><body>{children}</body></html>;
}
