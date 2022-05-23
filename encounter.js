let utils = require('./utils');
let strValue = utils.stringValue;
let copyTableRecords = utils.copyTableRecords;

let beehive = global.beehive;

function prepareEncounterRoleInsert(rows, nextId) {
    let insert = 'INSERT INTO encounter_role(encounter_role_id, name, description, ' +
        'creator, date_created, changed_by, date_changed, retired, retired_by,' +
        'date_retired, retire_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.encounterRoleMap.set(row['encounter_role_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareEncounterProviderInsert(rows, nextId) {
    let insert = 'INSERT INTO encounter_provider(encounter_provider_id, ' +
        'encounter_id, provider_id, encounter_role_id, creator, date_created, ' +
        'changed_by, date_changed, voided, voided_by, date_voided, ' +
        'void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${nextId}, ${beehive.encounterMap.get(row['encounter_id'])}, ` +
            `${beehive.providerMap.get(row['provider_id'])}, ` +
            `${beehive.encounterRoleMap.get(row['encounter_role_id'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareEncounterTypeInsert(rows, nextId) {
    let insert = 'INSERT INTO encounter_type(encounter_type_id, name, ' +
        'description, creator, date_created, retired, retired_by, ' +
        'date_retired, retire_reason, uuid, view_privilege, edit_privilege ';
    
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

        beehive.encounterTypeMap.set(row['encounter_type_id'], nextId);

        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])}, ` + 
            `${strValue(row['view_privilege'])}, ${strValue(row['edit_privilege'])}`;
        
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

function prepareEncounterInsert(rows, nextId) {
    let insert = 'INSERT INTO encounter(encounter_id, encounter_type, patient_id, ' +
        'location_id, form_id, visit_id, encounter_datetime, creator, date_created, ' +
        'changed_by, date_changed, voided, voided_by, date_voided, ' +
        'void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if(!global.excludedEncounterIds.includes(row['encounter_id'])) {
            if (toBeinserted.length > 1) {
                toBeinserted += ',';
            }
            let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
            let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
            let visitId = row['visit_id'] === null ? null : beehive.visitMap.get(row['visit_id']);
	    let location_id = row['location_id'] === null ? null : beehive.locationMap.get(row['location_id']);

            beehive.encounterMap.set(row['encounter_id'], nextId);

            toBeinserted += `(${nextId}, ${beehive.encounterTypeMap.get(row['encounter_type'])}, ` +
                `${beehive.personMap.get(row['patient_id'])}, ` +
		`${location_id}, ${row['form_id']}, ` +
                `${visitId}, ${strValue(utils.formatDate(row['encounter_datetime']))}, ` +
                `${beehive.userMap.get(row['creator'])}, ` +
                `${strValue(utils.formatDate(row['date_created']))}, ` +
                `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
                `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
                `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`

            nextId++;
        }
    });

    if(toBeinserted === '') {
        return [null, nextId];
    }

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

async function consolidateEncounterTypes(srcConn, destConn) {
    let query = 'SELECT * FROM encounter_type';
    let [srcEncounterTypes] = await srcConn.query(query);
    let [destEncounterTypes] = await destConn.query(query);

    let missingInDest = [];
    srcEncounterTypes.forEach(srcEncounterType => {
        let match = destEncounterTypes.find(destEncounterType => {
            return srcEncounterType['name'] === destEncounterType['name'];
        });

        if (match !== undefined && match !== null) {
            beehive.encounterTypeMap.set(srcEncounterType['encounter_type_id'],
                match['encounter_type_id']);
        } else {
            missingInDest.push(srcEncounterType);
        }
    });

    if (missingInDest.length > 0) {
        let nextEncounterTypeId =
            await utils.getNextAutoIncrementId(destConn, 'encounter_type');

        let [sql] = prepareEncounterTypeInsert(missingInDest, nextEncounterTypeId);
        let [result] = await destConn.query(sql);
        return result.affectedRows;
    }
    return 0;
}

async function consolidateEncounterRoles(srcConn, destConn) {
    let query = 'SELECT * FROM encounter_role';
    let [srcEncounterRoles] = await srcConn.query(query);
    let [destEncounterRoles] = await destConn.query(query);

    let missingInDest = [];
    srcEncounterRoles.forEach(srcEncounterRole => {
        let match = destEncounterRoles.find(destEncounterRole => {
            return srcEncounterRole['name'] === destEncounterRole['name'];
        });

        if (match !== undefined && match !== null) {
            beehive.encounterRoleMap.set(srcEncounterRole['encounter_role_id'],
                match['encounter_role_id']);
        } else {
            missingInDest.push(srcEncounterRole);
        }
    });

    if (missingInDest.length > 0) {
        let nextEncounterRoleId =
            await utils.getNextAutoIncrementId(destConn, 'encounter_role');

        let [sql] = prepareEncounterRoleInsert(missingInDest, nextEncounterRoleId);
        let [result] = await destConn.query(sql);
        return result.affectedRows;
    }
    return 0;
}

async function copyEncounters(srcConn, destConn) {
    let condition = false;
    if(global.excludedEncounterIds.length > 0) {
        condition = `encounter_id NOT IN (${global.excludedEncounterIds.join(',')})`;
    } 
    return await copyTableRecords(srcConn, destConn, 'encounter',
        'encounter_id', prepareEncounterInsert, condition);
}

async function copyEncounterProviders(srcConn, destConn) {
    let excludedEncounterProviderIds = [];
    let condition = false;
    await utils.mapSameUuidsRecords(srcConn, 'encounter_provider', 'encounter_provider_id', excludedEncounterProviderIds);
    if(excludedEncounterProviderIds.length > 0) {
        let toExclude = '(' + excludedEncounterProviderIds.join(',') + ')';
        condition = `encounter_provider_id NOT IN ${toExclude}`;
    }
    return await copyTableRecords(srcConn, destConn, 'encounter_provider',
        'encounter_provider_id', prepareEncounterProviderInsert, condition);
}

async function main(srcConn, destConn) {
    utils.logInfo('Consolidating encounter types...');
    let copiedTypes = await consolidateEncounterTypes(srcConn, destConn);
    utils.logOk(`Ok... ${copiedTypes} encounter types copied.`);

    utils.logInfo('Consolidating encounter roles...');
    let copiedEncRoles = await consolidateEncounterRoles(srcConn, destConn);
    utils.logOk(`Ok... ${copiedEncRoles} encounter roles copied.`);

    utils.logInfo('Copying encounters...');
    let srcEncCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'encounter');
    let initialDestCount = await utils.getCount(destConn, 'encounter');
    let expectedFinalCount = initialDestCount + srcEncCount;

    let copied = await copyEncounters(srcConn, destConn);
    utils.logDebug(`Expected number of encounters to be copied is ${srcEncCount}`);
    utils.logDebug(`Actual number of copied encounters is ${copied}`);

    let finalDestCount = await utils.getCount(destConn, 'encounter');
    if (finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${copied} encounters copied.`);
    } else {
        let error = `Problem copying encounters: the actual final count ` +
            `(${finalDestCount}) is not equal to the expected value ` +
            `(${expectedFinalCount})`;
        throw new Error(error);
    }

    utils.logInfo('Copying encounter providers...');
    let srcCount = await utils.getCountIgnoringDestinationDuplicateUuids(srcConn, 'encounter_provider');
    initialDestCount = await utils.getCount(destConn, 'encounter_provider');
    expectedFinalCount = initialDestCount + srcCount;

    copied = await copyEncounterProviders(srcConn, destConn);

    finalDestCount = await utils.getCount(destConn, 'encounter_provider');
    if (finalDestCount === expectedFinalCount) {
        utils.logOk(`Ok... ${copied} encounter_provider records copied.`);
    } else {
        let error = `Problem copying encounter_providers: the actual final count ` +
            `(${expectedFinalCount}) is not equal to the expected value ` +
            `(${finalDestCount})`;
        throw new Error(error);
    }
}

module.exports = main;
