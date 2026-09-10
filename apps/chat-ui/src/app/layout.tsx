import type { Metadata } from "next";
import "./globals.css";
export const metadata:Metadata={title:"Healing · Self Healthy Kafka",description:"Trợ lý phân tích sự cố và hướng dẫn Kafka Connect"};
// Applies the stored or system theme before first paint so dark users never see a light flash.
const themeScript=`try{var t=localStorage.getItem("healing-theme");if(t!=="light"&&t!=="dark"){t=window.matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light";}document.documentElement.classList.toggle("dark",t==="dark");}catch(e){}`;
export default function Layout({children}:Readonly<{children:React.ReactNode}>) {
  return <html lang="vi"><head><script dangerouslySetInnerHTML={{__html:themeScript}}/></head><body>{children}</body></html>;
}
