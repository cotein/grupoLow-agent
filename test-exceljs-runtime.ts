import * as ExcelJS from 'exceljs';
console.log('--- ExcelJS Namespace ---');
console.log(Object.keys(ExcelJS));
if ((ExcelJS as any).default) {
    console.log('--- ExcelJS.default Keys ---');
    console.log(Object.keys((ExcelJS as any).default));
}
