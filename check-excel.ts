import ExcelJS from 'exceljs';

const filePath = '/home/coto/Github/Kaiahub.ar/grupoLow-demo/det comp total.xlsx';

async function checkExcel() {
    console.log('Reading excel...');
    const workbook = new ExcelJS.Workbook();
    await workbook.xlsx.readFile(filePath);
    const worksheet = workbook.getWorksheet(1);
    
    let missingIdUnicaCount = 0;
    
    // Start from row 2 (headers are row 1)
    for (let i = 2; i <= worksheet.rowCount; i++) {
        const row = worksheet.getRow(i);
        const values = row.values as any[];
        
        const idunica = values[16];
        if (!idunica) {
            missingIdUnicaCount++;
            if (missingIdUnicaCount <= 5) {
                console.log(`Row ${i} is missing idunica. Values:`, JSON.stringify(values.slice(1, 17)));
            }
        }
    }
    console.log(`\nTotal rows: ${worksheet.rowCount - 1}`);
    console.log(`Rows missing idunica: ${missingIdUnicaCount}`);
    console.log(`Rows with idunica: ${(worksheet.rowCount - 1) - missingIdUnicaCount}`);
}

checkExcel();
