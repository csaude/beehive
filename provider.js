const config = require('./config');
const utils = require('./utils');
const strValue = utils.stringValue;
const copyTableRecords = utils.copyTableRecords;

const beehive = global.beehive;

function prepareProviderInsert(rows, nextId) {
    let insert = 'INSERT INTO provider(provider_id, person_id, name, identifier, ' +
        'creator, date_created, changed_by, date_changed, retired, retired_by, ' +
        'date_retired, retire_reason, uuid';

    if(global.openmrsDataModelVersion === 2) {
        insert += ', provider_role_id) VALUES ';
    } else {
        insert += ') VALUES ';
    }
    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let currentPersonId = row['person_id'] === null ? null : beehive.personMap.get(row['person_id']);
        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.providerMap.set(row['provider_id'], nextId);

        toBeinserted += `(${nextId}, ${currentPersonId}, ` +
            `${strValue(row['name'])}, ${strValue(row['identifier'])}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${utils.uuid(row['uuid'])}`;

        if(global.openmrsDataModelVersion === 2) {
            toBeinserted +=  `, ${row['provider_role_id']})`;
        } else {
            toBeinserted += ')';
        }
        nextId++;
    });

    let query = null;
    if(toBeinserted != '') query = insert + toBeinserted;
    return [query, nextId];
}

function prepareProviderAttributeTypeInsert(rows, nextId) {
    let insert = 'INSERT INTO provider_attribute_type(provider_attribute_type_id, ' +
        'name, description, datatype, datatype_config, preferred_handler, ' +
        'handler_config, min_occurs, max_occurs, creator, date_created, ' +
        'changed_by, date_changed, retired, retired_by, date_retired, ' +
        'retire_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.providerAttributeTypeMap.set(row['provider_attribute_type_id'], nextId);
        toBeinserted += `(${nextId}, ${strValue(row['name'])}, ` +
            `${strValue(row['description'])}, ${strValue(row['datatype'])}, ` +
            `${strValue(row['datatype_config'])}, ${strValue(row['preferred_handler'])}, ` +
            `${strValue(row['handler_config'])}, ${row['min_occurs']}, ${row['max_occurs']}` +
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

function prepareProviderAttributeInsert(rows, nextId) {
    let insert = 'INSERT INTO provider_attribute(provider_attribute_id, ' +
        'provider_id, attribute_type_id, value_reference, creator, ' +
        'date_created, changed_by, date_changed, voided, voided_by, ' +
        'date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${nextId}, ${beehive.providerMap.get(row['provider_id'])}, ` +
            `${beehive.providerAttributeTypeMap.get(row['attribute_type_id'])}, ` +
            `${strValue(row['value_reference'])}, ` +
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

async function copyProviderAttributeTypes(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'provider_attribute_type',
            'provider_attribute_type_id', beehive.providerAttributeTypeMap);
    return await copyTableRecords(srcConn, destConn, 'provider_attribute_type',
            'provider_attribute_type_id', prepareProviderAttributeTypeInsert, condition);
}

async function copyProviders(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'provider',
            'provider_id', beehive.providerMap);
    return await copyTableRecords(srcConn, destConn, 'provider',
            'provider_id', prepareProviderInsert, condition);
}

async function copyProviderAttributes(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'provider_attribute', 'provider_attribute_id');
    return await copyTableRecords(srcConn, destConn, 'provider_attribute',
            'provider_attribute_id', prepareProviderAttributeInsert, condition);
}

async function main(srcConn, destConn) {
    let initialDestCount = await utils.getCount(destConn, 'provider');

    utils.logInfo('Copying providers...');
    let copied = await copyProviders(srcConn, destConn);

    let finalDestCount = await utils.getCount(destConn, 'provider');
    let expectedFinalCount = initialDestCount + copied;
    if(expectedFinalCount === finalDestCount) {
        utils.logOk(`Ok... ${copied} providers copied.`);

        utils.logInfo('Copying provider attribute types...');
        await copyProviderAttributeTypes(srcConn, destConn);
        utils.logOk('Ok...');

        utils.logInfo('Copying provider attributes...');
        let copiedAttributes = await copyProviderAttributes(srcConn, destConn);
        utils.logOk(`Ok... ${copiedAttributes} provider attributes copied.`);
    }
    else {
        let error = `Problem copying providers: the actual final count ` +
            `of ${finalDestCount} is not equal to the expected value ` +
            `of ${expectedFinalCount}`;
        throw new Error(error);
    }
}

module.exports = main;
