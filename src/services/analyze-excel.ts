import ExcelJS from 'exceljs';
import path from 'path';

async function analyzeExcel() {
    const filePath = '/home/coto/Github/Kaiahub.ar/grupoLow-demo/Copia de det comp total (1).xlsx';
    const workbook = new ExcelJS.Workbook();
    
    try {
        await workbook.xlsx.readFile(filePath);
        const worksheet = workbook.getWorksheet(1); // Get first sheet
        
        console.log('--- Excel Analysis ---');
        console.log(`Sheet Name: ${worksheet.name}`);
        console.log(`Total Rows: ${worksheet.rowCount}`);
        
        const headers = worksheet.getRow(1).values;
        console.log('\nHeaders:', JSON.stringify(headers));
        
        console.log('\nSample Row (Row 2):');
        console.log(JSON.stringify(worksheet.getRow(2).values));
        
        console.log('\nSample Row (Row 3):');
        console.log(JSON.stringify(worksheet.getRow(3).values));

    } catch (error) {
        console.error('Error reading Excel:', error);
    }
}

analyzeExcel();
