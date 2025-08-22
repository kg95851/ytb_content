import { defineConfig } from 'vite';
import { resolve } from 'path';

export default defineConfig({
  root: '.', // 프로젝트 루트
  server: {
    port: 5173
  },
  build: {
    outDir: 'dist', // 빌드 결과물이 저장될 폴더
    rollupOptions: {
      input: {
        dashboard: resolve(__dirname, 'dashboard.html'),
        main: resolve(__dirname, 'index.html'),
        admin: resolve(__dirname, 'admin.html'),
        details: resolve(__dirname, 'details.html'), // 'details.html' 추가
      },
    },
  },
});
