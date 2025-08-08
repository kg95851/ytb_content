import { defineConfig } from 'vite';

export default defineConfig({
  root: '.', // index.html이 루트에 있는 경우
  build: {
    outDir: 'dist'
  }
});
