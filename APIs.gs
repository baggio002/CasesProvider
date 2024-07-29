/**
 * get all cases raw data filter by site and shard
 * return [][]
 */
function getAllCasesRaw(sites, shard) {
  let casesRaw = Utils.getRemoteValueWithNonLastRowRange(CASES_PROVIDER_SHEETS_ID, shard, CASES_RANGE_START).filter(
    // filter cases with site
    function(element, index, array) {
      return sites.toLowerCase().includes(formatSiteName_(element[34]));
    }
  );
  for (let i = 0; i < casesRaw.length; i++) {
    casesRaw[i][0] = casesRaw[i][0].toString();
  }
  return casesRaw;
}

/**
 * get all opened cases raw data filter by site and shard
 * return [][]
 */
function getOpenedCasesRawByShard(sites, shard) {
  let casesRaw = Utils.getRemoteValueWithNonLastRowRange(CASES_PROVIDER_SHEETS_ID, shard, CASES_RANGE_START).filter(
    function(element) {
      return element[3] != 'C';
    }
  ).filter(
    // filter cases with site
    function(element, index, array) {
      return sites.toLowerCase().includes(formatSiteName_(element[34]));
    }
  );
  for (let i = 0; i < casesRaw.length; i++) {
    casesRaw[i][0] = casesRaw[i][0].toString();
    // Logger.log('getOpenedCasesRawByShard = ' + i + " = " + casesRaw[i]);
  }
  return casesRaw;
}

/**
 * get all opened cases raw data filter by site and shard, dupliate by getOpenedCasesRawByShard(sites, shard)
 * return [][]
 */
function getAllCasesRawByShard(sites, shard) {
  let casesRaw = Utils.getRemoteValueWithNonLastRowRange(CASES_PROVIDER_SHEETS_ID, shard, CASES_RANGE_START).filter(
    // filter cases with site
    function(element, index, array) {
      return sites.toLowerCase().includes(formatSiteName_(element[34]));
    }
  );
  for (let i = 0; i < casesRaw.length; i++) {
    casesRaw[i][0] = casesRaw[i][0].toString();
  }
  return casesRaw;
}

function formatSiteName_(site) {
  return site.toLowerCase().replaceAll(" ", "");
}

/**
 * get all opened cases json objects fiter by site and shard
 * return [{jsonObj}...]
 */
function getCases(site, shard) {
  let cases = Utils.convertRawsToJsons(getOpenedCasesRawByShard(site, shard), getSchema());
  cases.forEach(
    caseObj => {

      // Logger.log('getCases = ' + JSON.stringify(caseObj));
      caseObj.open_bugs = [];
      caseObj.open_bugs = makeBugObjs_(caseObj);
    }
  );
  return cases;
}

/**
 * get all cases json object fiter by site and shard
 * return [{jsonObj}...]
 */
function getAllCases(site, shard) {
  let cases = Utils.convertRawsToJsons(getAllCasesRaw(site, shard), getSchema());
  cases.forEach(
    caseObj => {
      caseObj.open_bugs = [];
      caseObj.open_bugs = makeBugObjs_(caseObj);
    }
  );
  return cases;
}

/**
 * get all opened cases json object fiter by site and shard
 * return [{jsonObj}...]
 */
function getAllOPenedCases(site, shard) {
  return Utils.convertRawsToJsons(getOpenedCasesRawByShard(site, shard), getSchema());
}

/**
 * get all cases json object fiter by site, shard and min age <= case age <= max age
 * return [{jsonObj}...]
 */
function getCasesByAge(site, shard, minAge, maxAge) {
  return Utils.convertRawsToJsons(getOpenedCasesRawByShard(site, shard).filter(
    function(element, index, array) {
      return element[16] >= minAge && element[16] <= maxAge;
    }
  ), getSchema());
}

/**
 * get schema
 * return [string...]
 */
function getSchema() {
  // Do not use All cases sheet as schema since it might be empty when the data upload
  return Utils.getRemoteValueWithNonLastRowRange(CASES_PROVIDER_SHEETS_ID, DATA_SHARD, SCHEMA_RANGE)[0];
}

/**
 * get all ldap filter by sites and shard
 * return Set
 */
function getLdapSet(sites, shard) {
  let ldapSet = new Set();
  raws = getAllCasesRaw(sites, shard);
  raws.forEach(
    raw => {
      ldapSet.add(raw[17]);
    }
  );
  return ldapSet;
}

/**
 * get all cases filter by sites, shard and ldap
 * return [{jsonObj}...]
 */
function getCasesByLdap(sites, shard, ldap) {
  return getAllCases(sites, shard).filter(
    function (element) {
      return Utils.compareStrIgnoreCases(element.ldap, ldap)
    }
  );
}

/**
 * get all cases json objects fiter by site, shard and min trt <= trt hours <= max trt
 * return [{jsonObj}...]
 */
function getCasesByTrtHours(sites, shard, minTrt, maxTrt=0) {
  return getCases(sites, shard).filter(
    function (element) {
      return (minTrt <= element.trt_hours) && (element.trt_hours <= maxTrt)
    }
  );
}

/**
 * get opened cases json objects with open bugs fiter by site, shard
 * return [{jsonObj}...]
 */
function getOpenCasesWithOpenedBugs(site, shard) {
  return getCases(site, shard).filter(
    function (element) {
      // Logger.log(element.case_number + ' element.open_bugs.length = ' + element.open_bugs.length + " = " + JSON.stringify(element.open_bugs));
      return element.open_bugs.length > 0
    }
  );
}

function makeBugObjs_(caseObj) {
  bugs = [];
  caseObj.bugs_info.split(";").forEach(
    raw => {
      if (!Utils.isNull(raw)) {
        bugs.push(makeBugObj_(raw.split(",")));
      }
    }
  )
  return bugs;
}

function makeBugObj_(bugRaw) {
  return {
    bug_id: bugRaw[0],
    status: bugRaw[1],
    hours_util_now: bugRaw[2],
    last_update_timestamp: bugRaw[3],
    last_update_hours_until_now: bugRaw[4],
    assignee: bugRaw[5],
    create_timestamp: bugRaw[6]
  }
}

function makeConsultsObjs_(caseObj) {
  consults = [];
  caseObj.consults_info.split(";").forEach(
    raw => {
      if (!Utils.isNull(raw)) {
        consults.push(makeBugObj_(raw.split("&")));
      }
    }
  )
  return consults;
}

function makeConsultObj_(consultRaw) {
  return {
    consult_id: bugRaw[0],
    is_helpful_consult_response: bugRaw[1],
    consult_helpfulness: bugRaw[2],
    consult_channel: bugRaw[3],
    consult_age: bugRaw[4],
    create_timestamp: bugRaw[5],
    close_timestamp: bugRaw[6]
  }
}

function test() {
  Logger.log(Utils.convertRawsToJsons(getOpenedCasesRaw('tel-mnl', 'Data'), getSchema()).length);
  // Logger.log(getCasesByAge('tel-mon', 'Data', 0, 5).length);
}

