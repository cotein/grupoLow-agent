import * as ExcelJS_ from 'exceljs';
const ExcelJS = (ExcelJS_ as any).default || ExcelJS_;
console.log('--- Hybrid Import ExcelJS ---');
console.log('Is Workbook available?', !!ExcelJS.Workbook);
try {
    const wb = new ExcelJS.Workbook();
    console.log('Workbook instantiated successfully from hybrid import');
} catch (e) {
    console.error('Failed to instantiate Workbook:', e);
}

import * as pg_ from 'pg';
const pg = (pg_ as any).default || pg_;
const { Pool } = pg;
console.log('--- Hybrid Import pg ---');
console.log('Is Pool available?', !!Pool);
try {
    const p = new Pool();
    console.log('Pool instantiated successfully from hybrid import');
} catch (e) {
    console.error('Failed to instantiate Pool:', e);
}
