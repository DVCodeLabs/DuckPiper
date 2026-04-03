const esbuild = require("esbuild");

const production = process.argv.includes('--production');
const watch = process.argv.includes('--watch');

/**
 * @param {string} name
 * @returns {import('esbuild').Plugin}
 */
const createPlugin = (name) => ({
    name: 'esbuild-problem-matcher',
    setup(build) {
        build.onStart(() => {
            console.log(`[watch] ${name} build started`);
        });
        build.onEnd((result) => {
            result.errors.forEach(({ text, location }) => {
                console.error(`✘ [ERROR] ${name}: ${text}`);
                console.error(`    ${location.file}:${location.line}:${location.column}:`);
            });
            console.log(`[watch] ${name} build finished`);
        });
    },
});

async function main() {
    const ctxExtension = await esbuild.context({
        entryPoints: [
            { in: 'src/extension.ts', out: 'extension' }
        ],
        bundle: true,
        format: 'cjs',
        minify: production,
        sourcemap: !production,
        sourcesContent: false,
        platform: 'node',
        outdir: 'dist',
        external: ['vscode', 'duckdb', 'pg-native', 'cpu-features'],
        logLevel: 'silent',
        loader: {
            '.node': 'file',
        },
        plugins: [
            createPlugin('extension'),
        ],
    });

    const ctxWebview = await esbuild.context({
        entryPoints: [
            { in: 'src/results/webviewApp.tsx', out: 'webviewApp' },
            { in: 'src/ui/connectionForm/connectionFormApp.tsx', out: 'connectionFormApp' },
            { in: 'src/ui/welcome/welcomeApp.tsx', out: 'welcomeApp' },
            { in: 'src/ui/createTable/createTableApp.tsx', out: 'createTableApp' },
            { in: 'src/ui/backupSchema/backupSchemaApp.tsx', out: 'backupSchemaApp' },
            { in: 'src/markdown/markdownViewApp.tsx', out: 'markdownViewApp' },

        ],
        bundle: true,
        format: 'iife',
        minify: production,
        sourcemap: !production,
        sourcesContent: false,
        platform: 'browser',
        outdir: 'dist',
        logLevel: 'silent',
        plugins: [
            createPlugin('webview'),
        ],
        define: {
            'process.env.NODE_ENV': production ? '"production"' : '"development"'
        },
        loader: {
            '.ttf': 'dataurl',
            '.woff': 'dataurl',
            '.woff2': 'dataurl',
            '.eot': 'dataurl',
            '.svg': 'dataurl',
            '.png': 'dataurl',
            '.jpg': 'dataurl'
        }
    });

    if (watch) {
        await ctxExtension.watch();
        await ctxWebview.watch();
    } else {
        await ctxExtension.rebuild();
        await ctxWebview.rebuild();
        await ctxExtension.dispose();
        await ctxWebview.dispose();
    }
}

main().catch(e => {
    console.error(e);
    process.exit(1);
});
