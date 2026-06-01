import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    open: false,
    port: 5173,
    // 1. Pozwala serwerowi nasłuchiwać na wszystkich adresach IP (0.0.0.0)
    // Bez tego w WSL/Dockerze nie wejdziesz na localhost:5173 z poziomu Windowsa!
    host: true, 
    
    // 2. Wymusza "ręczne" sprawdzanie plików (Polling)
    // Naprawia to częsty błąd, gdzie zmiany w kodzie nie odświeżają przeglądarki w środowiskach wirtualnych
    watch: {
      usePolling: true 
    }
  }
})