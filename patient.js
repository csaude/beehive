const utils = require('./utils');
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const copyTableRecords = utils.copyTableRecords;

let beehive = global.beehive;

function preparePatientInsert(rows) {
    let insert = 'INSERT INTO patient(patient_id, creator, date_created, ' +
        'changed_by, date_changed, voided, voided_by, date_voided, ' +
        'void_reason';

    if(global.openmrsDataModelVersion === 2) {
        insert += ', allergy_status) VALUES ';
    } else {
        insert += ') VALUES ';
    }

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null :
                            beehive.userMap.get(row['voided_by']));
        let changedBy = (row['changed_by'] === null ? null :
                            beehive.userMap.get(row['changed_by']));

        toBeinserted += `(${beehive.personMap.get(row['patient_id'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))},` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))},` +
            `${row['voided']}, ${voidedBy},` +
            `${strValue(utils.formatDate(row['date_voided']))},` +
            `${strValue(row['void_reason'])}`;

        if(global.openmrsDataModelVersion === 2) {
            toBeinserted += `, ${strValue(row['allergy_status'])})`;
        } else {
            toBeinserted += ')';
        }
    });

    let query = insert + toBeinserted;
    return [query, -1];
}

function prepareIdentifierTypeInsert(rows, nextId) {
    let insert = 'INSERT INTO patient_identifier_type(patient_identifier_type_id, ' +
        'name, description, format, check_digit, creator, date_created, ' +
        'required, format_description, validator, retired, retired_by, ' +
        'date_retired, retire_reason, uuid, location_behavior, uniqueness_behavior';

    if(global.openmrsDataModelVersion === 2) {
        insert += ', changed_by, date_changed) VALUES ';
    } else {
        insert += ') VALUES ';
    }

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);

        beehive.identifierTypeMap.set(row['patient_identifier_type_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['format'])}, ` +
            `${row['check_digit']}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['required']}, ${strValue(row['format_description'])}, ` +
            `${strValue(row['validator'])}, ${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])}, ` +
            `${strValue(row['location_behavior'])}, ${strValue(row['uniqueness_behavior'])}`;

            if(global.openmrsDataModelVersion === 2) {
                let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
                toBeinserted += `, ${changedBy}, ${strValue(utils.formatDate(row['date_changed']))})`;
            } else {
                toBeinserted += ')';
            }
        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function preparePatientIdentifierInsert(rows, nextId) {
    let insert = 'INSERT INTO patient_identifier(patient_identifier_id, patient_id, ' +
        'identifier, identifier_type, preferred, location_id, creator, ' +
        'date_created, voided, voided_by, date_voided, void_reason, uuid, ' +
        'date_changed, changed_by) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = (row['voided_by'] === null ? null :
                            beehive.userMap.get(row['voided_by']));
        let changedBy = (row['changed_by'] === null ? null :
                            beehive.userMap.get(row['changed_by']));
        let locationId = (row['location_id'] === null ? null :
                            beehive.locationMap.get(row['location_id']));

        toBeinserted += `(${nextId}, ${beehive.personMap.get(row['patient_id'])}, ` +
            `${strValue(row['identifier'])}, ` +
            `${beehive.identifierTypeMap.get(row['identifier_type'])}, ` +
            `${row['preferred']},  ${locationId}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${row['voided']}, ` +
            `${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ${changedBy})`

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

async function copyPatientIdentifierTypes(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'patient_identifier_type',
            'patient_identifier_type_id', beehive.identifierTypeMap);
    return await copyTableRecords(srcConn, destConn, 'patient_identifier_type',
            'patient_identifier_type_id', prepareIdentifierTypeInsert, condition);
}

async function copyPatients(srcConn, destConn) {
    let condition = await patientCopyCondition(destConn);
    if(condition !== null) {
        return await copyTableRecords(srcConn, destConn, 'patient', 'patient_id', preparePatientInsert, condition);
    }
    return 0;
}

async function copyPatientIdentifiers(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'patient_identifier', 'patient_identifier_id');
    return await copyTableRecords(srcConn, destConn, 'patient_identifier',
        'patient_identifier_id', preparePatientIdentifierInsert, condition);
}

async function patientCopyCondition(destConn) {
    let personIdsCopied = [];
    let personMapDestIds = [];
    global.beehive.personMap.forEach(destPersonId => {
        personMapDestIds.push(destPersonId);
    });
    
    let query = `SELECT patient_id FROM patient WHERE patient_id IN (${personMapDestIds.join(',')})`;
    try {
        let [results] = await destConn.query(query);
        let correspondingPatientsAlreadyInDest = [];
        results.forEach(record => {
            correspondingPatientsAlreadyInDest.push(record['patient_id']);
        });

        global.beehive.personMap.forEach((destPersonId, srcPersonId) => {
            if(!global.excludedPersonIds.includes(srcPersonId) || !correspondingPatientsAlreadyInDest.includes(destPersonId)) {
                personIdsCopied.push(srcPersonId);
            }
        });

        if(personIdsCopied.length > 0) {
            return `patient_id IN (${personIdsCopied.join(',')})`;
        }
        return null;
    } catch(ex) {
        utils.logError('Error: While evaluating patient condition');
        if(query) {
            utils.logError('Query during error:');
            utils.logError(query);
        }
        throw ex;
    }
}

async function main(srcConn, destConn) {
    utils.logInfo('Copying patients...');
    let iDestPatientCount = await getCount(destConn, 'patient');

    let copied = await copyPatients(srcConn, destConn);

    let finalDestPatientCount = await getCount(destConn, 'patient');
    let expectedFinalCount = iDestPatientCount + copied;

    if (finalDestPatientCount === expectedFinalCount) {
        utils.logOk(`OK... ${copied} patients copied.`);

        utils.logInfo('Copying patient identifiers');
        await copyPatientIdentifierTypes(srcConn, destConn);
        copied = await copyPatientIdentifiers(srcConn, destConn);
        utils.logOk(`Ok... ${copied} patient identifiers copied.`);
    } else {
        let message = 'There is a problem in copying patients, the final expected ' +
            `count (${expectedFinalCount}) does not equal the actual final ` +
            `count (${finalDestPatientCount})`;
        throw new Error(message);
    }
}

module.exports = main;
