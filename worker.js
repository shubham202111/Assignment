const { parentPort, workerData } = require('worker_threads');
const xlsx = require('xlsx');
const csvParser = require('csv-parser');

function processXLSX(buffer) {
  const workbook = xlsx.read(buffer, { type: 'buffer' });
  const sheetName = workbook.SheetNames[0]; 
  const sheetData = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
  return sheetData;
}

function processCSV(buffer) {
  return new Promise((resolve, reject) => {  
    const results = [];
    const parser = csvParser();

    parser.on('data', (data) => {
      results.push(data);
    });

    parser.on('end', () => {
      resolve(results);
    });

    parser.on('error', reject);

    parser.write(buffer);
    parser.end();
  });
}

(async () => {
  try {
    const { fileBuffer, originalFilename } = workerData;

    const fileExtension = originalFilename.split('.').pop().toLowerCase();

    let processedData;

    if (fileExtension === 'xlsx') {
      processedData = processXLSX(fileBuffer);
    } else if (fileExtension === 'csv') {
      processedData = await processCSV(fileBuffer);
    } else {
      throw new Error('Unsupported file type');
    }  

    const agents = [];   
    const users = [];
    const accounts = [];
    const policyCategories = [];
    const policyCarriers = [];
    const policyInfos = [];

    processedData.forEach((data) => {
      if (data) {
        agents.push({ agentName: data.agent });

        policyInfos.push({
          policyNumber: data.policy_number,
          policyStartDate: new Date(data.policy_start_date),
          policyEndDate: new Date(data.policy_end_date),
          policyCategory: data.policy_category, 
          collectionId: data.collectionId,
          companyCollectionId: data.companyCollectionId, 
          userId: data.userId,
        });

        policyCarriers.push({ company_name: data.company_name });

        policyCategories.push({ category_name: data.category_name });

        accounts.push({ accountName: data.account_name });

        users.push({
          firstName: data.firstname,
          DOB: new Date(data.dob),
          address: data.address,
          phoneNumber: data.phone,
          state: data.state,   
          zipCode: data.zip,
          email: data.email,
          gender: data.gender,   
          userType: data.userType,
        });
      }
    });

    parentPort.postMessage({ agents, users, accounts, policyCategories, policyCarriers, policyInfos });
  } catch (err) {
    console.error('Error processing file in worker:', err);
        parentPort.postMessage({});
  }
})();
