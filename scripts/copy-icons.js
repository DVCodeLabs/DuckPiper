const fs = require('fs');
const path = require('path');

let iconsDir;

// Try to resolve via node resolution
try {
    // Try to find the package root
    const pkgPath = require.resolve('lucide-static/package.json');
    iconsDir = path.join(path.dirname(pkgPath), 'icons');
} catch (e) {
    // Fallback to standard node_modules location
    const fallbackPath = path.join(__dirname, '../node_modules/lucide-static/icons');
    if (fs.existsSync(fallbackPath)) {
        iconsDir = fallbackPath;
    } else {
        console.error('Could not locate lucide-static/icons directory.');
        console.error('Please ensure "lucide-static" is installed in devDependencies.');
        console.error('Debug Error:', e.message);
        process.exit(1);
    }
}

const destDirBase = path.join(__dirname, '../media/icons/lucide');
const destDirDark = path.join(destDirBase, 'dark');
const destDirLight = path.join(destDirBase, 'light');

[destDirDark, destDirLight].forEach(dir => {
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }
});

console.log(`Copying and theming icons from ${iconsDir} to ${destDirBase}...`);

const files = fs.readdirSync(iconsDir);
let count = 0;

const INCLUDED_ICONS = [
    'database-zap.svg',
    'database.svg',
    'upload.svg',
    'download.svg',
    'play.svg',
    'search-alert.svg',
    'file-plus.svg',
    'book-plus.svg',
    'book-text.svg',
    'trash.svg',
    'combine.svg',
    'rotate-cw.svg',
    'rotate-ccw.svg',
    'file-pen-line.svg',
    'file-stack.svg',
    'database-backup.svg',
    'clipboard-check.svg',
    'clipboard-plus.svg',
    'clipboard-x.svg',
    'clipboard-pen-line.svg'
];

const processedIcons = new Set();

files.forEach(file => {
    if (path.extname(file) === '.svg' && INCLUDED_ICONS.includes(file)) {
        processedIcons.add(file);
        const srcPath = path.join(iconsDir, file);
        let content = fs.readFileSync(srcPath, 'utf8');

        // Remove legacy flat copy if it exists to clean up
        const flatPath = path.join(destDirBase, file);
        if (fs.existsSync(flatPath)) {
            fs.unlinkSync(flatPath);
        }

        // 1. Dark Mode: White stroke (#FFFFFF)
        // Replace stroke="currentColor" with stroke="#FFFFFF"
        let darkContent = content.replace(/stroke="currentColor"/g, 'stroke="#FFFFFF"');
        darkContent = darkContent.replace('<svg', '<svg stroke-opacity="0.8"');
        fs.writeFileSync(path.join(destDirDark, file), darkContent);

        // 2. Light Mode: Black stroke with opacity
        // Replace stroke="currentColor" with stroke="#000000" and add opacity to svg
        let lightContent = content.replace(/stroke="currentColor"/g, 'stroke="#000000"');
        // Inject stroke-opacity attribute
        lightContent = lightContent.replace('<svg', '<svg stroke-opacity="0.75"');
        fs.writeFileSync(path.join(destDirLight, file), lightContent);

        if (file === 'play.svg') {
            const destDirBlue = path.join(destDirBase, 'blue');
            if (!fs.existsSync(destDirBlue)) {
                fs.mkdirSync(destDirBlue, { recursive: true });
            }
            // 3. Blue Variant: #3371B3 (Brand Color)
            // Replace stroke with brand color
            let blueContent = content.replace(/stroke="currentColor"/g, 'stroke="#3371B3"');
            // Replace fill="none" with fill="#3371B3"
            blueContent = blueContent.replace('fill="none"', 'fill="#3371B3"');
            fs.writeFileSync(path.join(destDirBlue, file), blueContent);

            // 4. Dark Filled Variant: White Fill
            const destDirDarkFilled = path.join(destDirBase, 'dark-filled');
            if (!fs.existsSync(destDirDarkFilled)) {
                fs.mkdirSync(destDirDarkFilled, { recursive: true });
            }
            let darkFilledContent = content.replace(/stroke="currentColor"/g, 'stroke="#FFFFFF"');
            darkFilledContent = darkFilledContent.replace('fill="none"', 'fill="#FFFFFF"');
            // Add 80% opacity to match other dark icons if desired, or keep solid. 
            // User asked for "black and white fill version". Assuming solid or same opacity logic.
            // Let's stick to standard solid fill for "filled" request unless specified otherwise.
            fs.writeFileSync(path.join(destDirDarkFilled, file), darkFilledContent);

            // 5. Light Filled Variant: Black Fill
            const destDirLightFilled = path.join(destDirBase, 'light-filled');
            if (!fs.existsSync(destDirLightFilled)) {
                fs.mkdirSync(destDirLightFilled, { recursive: true });
            }
            let lightFilledContent = content.replace(/stroke="currentColor"/g, 'stroke="#000000"');
            lightFilledContent = lightFilledContent.replace('fill="none"', 'fill="#000000"');
            fs.writeFileSync(path.join(destDirLightFilled, file), lightFilledContent);
        }

        count++;
    }
});

const missingIcons = INCLUDED_ICONS.filter(icon => !processedIcons.has(icon));

if (missingIcons.length > 0) {
    console.warn('⚠️  Warning: The following icons from the allowlist were NOT found in lucide-static:');
    missingIcons.forEach(icon => console.warn(`   - ${icon}`));
}

console.log(`Successfully proccessed ${count} icons into dark/light variants (out of ${INCLUDED_ICONS.length} requested).`);
