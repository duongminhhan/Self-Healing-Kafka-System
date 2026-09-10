"use client";
import { useCallback, useSyncExternalStore } from "react";

// The theme class is applied to <html> by the inline script in layout.tsx before
// first paint, so the DOM is the source of truth and React only subscribes to it.
const THEME_KEY="healing-theme";
const listeners=new Set<()=>void>();
const notify=()=>listeners.forEach(listener=>listener());
function stored() {
  try { const value=localStorage.getItem(THEME_KEY); return value==="dark"||value==="light"?value:null; }
  catch { return null; }
}
function subscribeTheme(listener:()=>void) {
  listeners.add(listener);
  const media=window.matchMedia("(prefers-color-scheme: dark)");
  // Follow the system only while the user has not chosen explicitly.
  const onSystemChange=()=>{ if(!stored()) { document.documentElement.classList.toggle("dark",media.matches); notify(); } };
  media.addEventListener("change",onSystemChange);
  return ()=>{ listeners.delete(listener); media.removeEventListener("change",onSystemChange); };
}
export function useDarkTheme() {
  return useSyncExternalStore(subscribeTheme,()=>document.documentElement.classList.contains("dark"),()=>false);
}
export function setDarkTheme(dark:boolean) {
  document.documentElement.classList.toggle("dark",dark);
  try { localStorage.setItem(THEME_KEY,dark?"dark":"light"); } catch { /* storage can be blocked */ }
  notify();
}
export function useMediaQuery(query:string) {
  const subscribe=useCallback((listener:()=>void)=>{
    const media=window.matchMedia(query);
    media.addEventListener("change",listener);
    return ()=>media.removeEventListener("change",listener);
  },[query]);
  return useSyncExternalStore(subscribe,()=>window.matchMedia(query).matches,()=>false);
}
