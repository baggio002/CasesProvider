var schema;
var coduckSchema;

function getAllOpenCases_() {
  return Utils.getValues(ALL_CASES_SHEET, CASES_RANGE_START + Utils.getLastRow(ALL_CASES_SHEET));
}

function getAllOpenCasesForCoduck_() {
  return Utils.getValues(ALL_CASES_SHEET, RANGE_CODUCK + Utils.getLastRow(ALL_CASES_SHEET));
}

function getSchema_() {
  schema = Utils.getValues(ALL_CASES_SHEET, SCHEMA_RANGE);
}

function getCoduckSchema_() {
  coduckSchema = Utils.getValues(ALL_CASES_SHEET, SCHEMA_CODUCK_RANGE);
}

function exportCasesToShard() {
  let cases = getAllOpenCases_();
  if (cases.length == 0) {
    return;
  }
  getSchema_();
  getCoduckSchema_();
  exportDataCases_(cases);
  exportInfraCases_(cases);
  exportNetworkingCases_(cases);
  exportPlatfromCases_(cases);
  copyToCoduck_();
}

function copyToCoduck_() {
  cases = getAllOpenCasesForCoduck_();
  if (cases.length <= 2) {
    return;
  }
  Utils.clear(CODUCK_SHEET, RANGE_CODUCK + Utils.getLastRow(CODUCK_SHEET));
  cases.forEach(
    caseObj => {
      let last_modify = new Date(caseObj[38]);
      let trt = new Date(caseObj[40]);
      if (!Utils.isNull(caseObj[36])) {
        let last_bug_update = new Date(caseObj[36]);
        caseObj[36] = last_bug_update.toLocaleString();
        caseObj[37] = caseObj[37].toString();
      }
      caseObj[38] = last_modify.toLocaleString();
      caseObj[40] = trt.toLocaleString();
      if (caseObj[10] != 'shard') {
        caseObj[10] = caseObj[18];
      }
      // Logger.log(caseObj[37] + " " + caseObj[39] + " " + caseObj[41]);
      caseObj[39] = caseObj[39].toString();
      caseObj[41] = caseObj[41].toString();
    }
  );
  // cases = coduckSchema.concat(cases);
  Utils.exportRawDataToSheet(CODUCK_SHEET, RANGE_CODUCK + cases.length, cases);
  // Utils.exportRawDataToSheet(CODUCK_SHEET, 'A1:AN' + cases.length, cases);
}

function generateHead_(sheet) {
  // Utils.clear(sheet, SCHEMA_RANGE)
  Utils.exportRawDataToSheet(sheet, SCHEMA_RANGE, schema);
}

function exportDataCases_(cases) {
  generateHead_(DATA_SHARD)
  exportToSheet_(groupCasesByShard_(cases, DATA_SHARD), DATA_SHARD);
}

function exportInfraCases_(cases) {
  generateHead_(INFRA_SHARD)
  exportToSheet_(groupCasesByShard_(cases, INFRA_SHARD), INFRA_SHARD);
}

function exportNetworkingCases_(cases) {
  generateHead_(NETWORKING_SHARD)
  exportToSheet_(groupCasesByShard_(cases, NETWORKING_SHARD), NETWORKING_SHARD);
}

function exportPlatfromCases_(cases) {
  generateHead_(PLATFORM_SHARD)
  exportToSheet_(groupCasesByShard_(cases, PLATFORM_SHARD), PLATFORM_SHARD);
}

function groupCasesByShard_(cases, shard) {
  groupCasesRawData = [];
  cases.forEach(
    raw => {
      if (raw[18] == shard) {
        groupCasesRawData.push(raw);
      }
    }
  );
  // Logger.log("size = " + groupCasesRawData.length + " shard = " + shard) ;
  return groupCasesRawData;
}

function exportToSheet_(raws, shard) {
  Utils.clear(shard, CASES_RANGE_START + Utils.getLastRow(shard));
  if (raws.length > 0) {
    Utils.exportRawDataToSheet(shard, CASES_RANGE_START + (raws.length + 1), raws);
  }
}
