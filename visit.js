let utils = require('./utils');
let strValue = utils.stringValue;
let copyTableRecords = utils.copyTableRecords;

let beehive = global.beehive;

function prepareVisitTypeInsert(rows, nextId) {
  let insert = 'INSERT INTO visit_type(visit_type_id, name, description, '
        + 'creator, date_created, changed_by, date_changed, retired, retired_by,'
        + 'date_retired, retire_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if (toBeinserted.length > 1) {
          toBeinserted += ',';
      }

      let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
      let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

      beehive.visitTypeMap.set(row['visit_type_id'], nextId);

      toBeinserted += `(${nextId}, ${strValue(row['name'])}, `
          + `${strValue(row['description'])}, `
          + `${beehive.userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, `
          + `${strValue(utils.formatDate(row['date_changed']))}, `
          + `${row['retired']}, ${retiredBy}, `
          + `${strValue(utils.formatDate(row['date_retired']))}, `
          + `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

      nextId++;
  });

  let query = insert + toBeinserted;
  return [query, nextId];
}

function prepareVisitInsert(rows, nextId) {
  let insert = 'INSERT INTO visit(visit_id, patient_id, visit_type_id, '
        + 'date_started, date_stopped, indication_concept_id, location_id, '
        + 'creator, date_created, changed_by, date_changed, voided, voided_by, '
        + 'date_voided, void_reason, uuid) VALUES ';

  let toBeinserted = '';
  rows.forEach(row => {
      if(toBeinserted.length > 1) {
        toBeinserted += ',';
      }
      let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
      let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
      let location = row['location_id'] === null ? null : beehive.locationMap.get(row['location_id']);

      beehive.visitMap.set(row['visit_id'], nextId);

      toBeinserted += `(${nextId}, ${beehive.personMap.get(row['patient_id'])}, `
          + `${beehive.visitTypeMap.get(row['visit_type_id'])}, `
          + `${strValue(utils.formatDate(row['date_started']))}, `
          + `${strValue(utils.formatDate(row['date_stopped']))}, `
          + `${row['indication_concept_id']}, ${location}, `
          + `${beehive.userMap.get(row['creator'])}, `
          + `${strValue(utils.formatDate(row['date_created']))}, `
          + `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, `
          + `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, `
          + `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

      nextId++;
  });

  if(toBeinserted === '') {
    return [null, nextId];
  }
  let insertStatement = insert + toBeinserted;
  return [insertStatement, nextId];
}

async function copyVisitTypes(srcConn, destConn) {
  let condition = await utils.getExcludedIdsCondition(srcConn, 'visit_type',
            'visit_type_id', beehive.visitTypeMap);
    return await copyTableRecords(srcConn, destConn, 'visit_type',
            'visit_type_id', prepareVisitTypeInsert, condition);
}

async function copyVisits(srcConn, destConn) {
  let condition = await utils.getExcludedIdsCondition(srcConn, 'visit',
                    'visit_id', beehive.visitMap);
  return await copyTableRecords(srcConn, destConn, 'visit', 'visit_id',
                  prepareVisitInsert, condition);
}

async function main(srcConn, destConn) {
    utils.logInfo('Copying missing visit types...');
    await copyVisitTypes(srcConn, destConn);
    utils.logOk('Ok...');

    let srcVisitCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'visit');
    let initialDestCount = await utils.getCount(destConn, 'visit');

    utils.logInfo('Copying visits...');
    let copied = await copyVisits(srcConn, destConn);
    utils.logDebug('Number of visit records copied: ', copied);
    
    let finalDestCount = await utils.getCount(destConn, 'visit');
    let expectedFinalCount = initialDestCount + srcVisitCount;

    if(finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${copied}`);
    }
    else {
        let error = `Problem copying visits: the actual final count ` +
            `(${finalDestCount}) is not equal to the expected value ` +
            `(${expectedFinalCount})`;
        throw new Error(error);
    }
}

module.exports = main;
