import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    node: 'src/node.ts',
    'server/index': 'src/server/index.ts',
    'server/cli': 'src/server/cli.ts',
  },
  format: ['esm', 'cjs'],
  target: 'node20',
  platform: 'node',
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  treeshake: true,
  external: ['ws', '@roamhq/wrtc'],
});
