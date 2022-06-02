const utils = require('./utils');
const logTime = utils.logTime;
const strValue = utils.stringValue;
const getCount = utils.getCount;
const copyTableRecords = utils.copyTableRecords;

let beehive = global.beehive;
beehive.programMap = new Map();
beehive.programWorkflowMap = new Map();
beehive.patientProgramMap = new Map();
beehive.programWorkflowStateMap = new Map();

function prepareProgramInsert(rows, nextId) {
    let insert = 'INSERT INTO program(program_id, concept_id, creator, ' +
            'date_created, changed_by, date_changed, retired, name, ' +
            'description, uuid, outcomes_concept_id) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        beehive.programMap.set(row['program_id'], nextId);

        toBeinserted += `(${nextId}, ${row['concept_id']}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))},` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))},` +
            `${row['retired']}, ${strValue(row['name'])},` +
            `${strValue(row['description'])}, ${utils.uuid(row['uuid'])},` +
            `${row['outcomes_concept_id']})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareProgramWorkflowInsert(rows, nextId) {
    let insert = 'INSERT INTO program_workflow(program_workflow_id, ' +
            'program_id, concept_id, creator, date_created, retired, ' +
            'changed_by, date_changed, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.programWorkflowMap.set(row['program_workflow_id'], nextId);

        toBeinserted += `(${nextId}, ${beehive.programMap.get(row['program_id'])}, ` +
            `${row['concept_id']}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['retired']}, ${changedBy}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${utils.uuid(row['uuid'])})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function prepareProgramWorkflowStateInsert(rows, nextId) {
    let insert = 'INSERT INTO program_workflow_state(program_workflow_state_id, ' +
        'program_workflow_id, ' +
        'concept_id, initial, terminal, creator, date_created, retired, ' +
        'changed_by, date_changed, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        beehive.programWorkflowStateMap.set(row['program_workflow_state_id'], nextId);

        toBeinserted += `(${nextId}, ` +
            `${beehive.programWorkflowMap.get(row['program_workflow_id'])}, ` +
            `${row['concept_id']},  ${row['initial']}, ${row['terminal']}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ${row['retired']}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${utils.uuid(row['uuid'])})`

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function preparePatientProgramInsert(rows, nextId) {
    let insert = 'INSERT INTO patient_program(patient_program_id, patient_id, ' +
        'program_id, ' +
        'date_enrolled, date_completed, creator, date_created, changed_by, ' +
        'date_changed, voided, voided_by, date_voided, void_reason, uuid, ' +
        'location_id, outcome_concept_id) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        let locationId = (row['location_id'] === null ? null :
                            beehive.locationMap.get(row['location_id']));

        // Do this for location_ids that no longer exists in the system.                    
        if(locationId === undefined) {
            locationId = null;
        }

        beehive.patientProgramMap.set(row['patient_program_id'], nextId);

        toBeinserted += `(${nextId}, ${beehive.personMap.get(row['patient_id'])}, ` +
            `${beehive.programMap.get(row['program_id'])}, ` +
            `${strValue(utils.formatDate(row['date_enrolled']))}, ` +
            `${strValue(utils.formatDate(row['date_completed']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])}, ` +
            `${locationId}, ${row['outcome_concept_id']})`;

        nextId++;
    });

    let query = insert + toBeinserted;
    return [query, nextId];
}

function preparePatientStateInsert(rows) {
    let insert = 'INSERT INTO patient_state(patient_program_id, state, ' +
            'start_date, end_date, creator, date_created, changed_by, ' +
            'date_changed, voided, voided_by, date_voided, void_reason, ' +
            'uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }

        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${beehive.patientProgramMap.get(row['patient_program_id'])}, ` +
            `${beehive.programWorkflowStateMap.get(row['state'])}, ` +
            `${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))}, ` +
            `${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${row['voided']}, ${voidedBy}, ` +
            `${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${utils.uuid(row['uuid'])})`;
    });

    let query = insert + toBeinserted;
    return [query, -1];
}

async function copyPrograms(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'program',
                     'program_id', beehive.programMap);
    return await copyTableRecords(srcConn, destConn, 'program', 'program_id',
                     prepareProgramInsert, condition);
}

async function copyProgramWorkflows(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'program_workflow',
            'program_workflow_id', beehive.programWorkflowMap);
    return await copyTableRecords(srcConn, destConn, 'program_workflow',
            'program_workflow_id', prepareProgramWorkflowInsert, condition);
}

async function copyProgramWorkflowStates(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'program_workflow_state',
            'program_workflow_state_id', beehive.programWorkflowStateMap);
    return await copyTableRecords(srcConn, destConn, 'program_workflow_state',
            'program_workflow_state_id', prepareProgramWorkflowStateInsert, condition);
}

async function copyPatientPrograms(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'patient_program',
                    'patient_program_id', beehive.patientProgramMap);
    return await copyTableRecords(srcConn, destConn, 'patient_program',
                    'patient_program_id', preparePatientProgramInsert, condition);
}

async function copyPatientStates(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'patient_state',
                    'patient_state_id');
    return await copyTableRecords(srcConn, destConn, 'patient_state',
                    'patient_state_id', preparePatientStateInsert, condition);
}

async function main(srcConn, destConn) {
    utils.logInfo('Copying programs...');
    let copied = await copyPrograms(srcConn, destConn);
    utils.logOk(`OK... ${copied} program copied to destination.`);

    utils.logInfo('Copying programs workflows...');
    copied = await copyProgramWorkflows(srcConn, destConn);
    utils.logOk(`OK... ${copied} program workflows copied to destination.`);

    utils.logInfo('Copying programs workflow states...');
    copied = await copyProgramWorkflowStates(srcConn, destConn);
    utils.logOk(`OK... ${copied} program work flow states copied to destination.`);

    utils.logInfo('Copying patients programs...');
    let iDestCount = await getCount(destConn, 'patient_program');

    copied = await copyPatientPrograms(srcConn, destConn);

    let fDestCount = await getCount(destConn, 'patient_program');
    let expectedFinalCount = iDestCount + copied;

    if (fDestCount === expectedFinalCount) {
        utils.logOk(`OK... ${copied} patient_program records copied.`);

        utils.logInfo('Copying patient_state records...');
        iDestCount = await getCount(destConn, 'patient_state');

        copied = await copyPatientStates(srcConn, destConn);

        fDestCount = await getCount(destConn, 'patient_state');
        expectedFinalCount = iDestCount + copied;
        if (fDestCount === expectedFinalCount) {
            utils.logOk(`OK... ${copied} patient_state records copied.`);
        } else {
            let message = 'There is a problem in copying patient_state records, ' +
                'the final expected ' +
                `count (${expectedFinalCount}) does not equal the actual final ` +
                `count (${fDestCount})`;
            throw new Error(message);
        }
    } else {
        let message = 'There is a problem in copying patient_program records, ' +
            'the final expected ' +
            `count (${expectedFinalCount}) does not equal the actual final ` +
            `count (${fDestCount})`;
        throw new Error(message);
    }
}

module.exports = main;
