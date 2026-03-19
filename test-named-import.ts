import { Workbook } from 'exceljs';
console.log('--- Workbook from exceljs ---');
console.log(Workbook);
try {
    const wb = new Workbook();
    console.log('Workbook instantiated successfully');
} catch (e) {
    console.error('Failed to instantiate Workbook:', e);
}
