'use strict';
const utils = require('./utils');
const strValue = utils.stringValue;
const uuid = utils.uuid;
const logTime = utils.logTime;
const copyTableRecords = utils.copyTableRecords;
const config = require('./config');

const BATCH_SIZE = config.batchSize || 200;
let beehive = global.beehive;

const copiedLaterPersonsMap = new Map();

function preparePersonInsert(rows, nextPersonId) {
    let insert = 'INSERT INTO person(person_id, gender, birthdate,' +
        'birthdate_estimated, dead, death_date, cause_of_death, creator,' +
        'date_created, date_changed, voided, ' +
        'date_voided, void_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        beehive.personMap.set(row['person_id'], nextPersonId);
        toBeinserted += `(${nextPersonId}, '${row['gender']}', ` +
            `${strValue(utils.formatDate(row['birthdate']))},` +
            `${strValue(row['birthdate_estimated'])}, ${row['dead']},` +
            `${strValue(utils.formatDate(row['deathdate']))}, ` +
            `${strValue(row['cause_of_death'])}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))},` +
            `${strValue(utils.formatDate(row['date_changed']))},` +
            `${row['voided']}, ` +
            `${strValue(utils.formatDate(row['date_voided']))},` +
            `${strValue(row['void_reason'])}, ${uuid(row['uuid'])})`;
        nextPersonId++;
    })

    let query = insert + toBeinserted;

    return [query, nextPersonId];
}

function preparePersonAuditInfoUpdateQuery(rows) {
    let update = 'INSERT INTO person(person_id, changed_by, voided_by) VALUES '
    let lastPart = ' ON DUPLICATE KEY UPDATE changed_by = VALUES(changed_by), ' +
                   'voided_by = VALUES(voided_by)';

    let values = '';
    let toUpdate = 0;
    rows.forEach(row => {
        let destPersonId = beehive.personMap.get(row['person_id']);
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        if(destPersonId) {
            toUpdate++;
            if(values.length > 1) {
                values += ',';
            }
            values += `(${destPersonId}, ${changedBy}, ${voidedBy})`;
        }
    });

    if(values === '') return [undefined, 0];
    return [update + values + lastPart, toUpdate];
}

function preparePersonNameInsert(rows, nextPersonNameId) {
    let insert = 'INSERT INTO person_name(person_name_id, preferred, person_id, ' +
        'prefix, given_name, middle_name, family_name_prefix, family_name,' +
        'family_name2, family_name_suffix, degree, creator, date_created,' +
        'voided, voided_by, date_voided, void_reason, changed_by, ' +
        'date_changed, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        let currentPersonId = beehive.personMap.get(row['person_id']);
        if (currentPersonId !== undefined) {
            if (toBeinserted.length > 1) {
                toBeinserted += ',';
            }
            let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
            let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

            toBeinserted += `(${nextPersonNameId}, ${row['preferred']}, ${currentPersonId}, ` +
                `${strValue(row['prefix'])}, ${strValue(row['given_name'])}, ` +
                `${strValue(row['middle_name'])}, ${strValue(row['family_name_prefix'])}, ` +
                `${strValue(row['family_name'])}, ${strValue(row['family_name2'])}, ` +
                `${strValue(row['family_name_suffix'])}, ${strValue(row['degree'])}, ` +
                `${beehive.userMap.get(row['creator'])}, ${strValue(utils.formatDate(row['date_created']))}, ` +
                `${row['voided']}, ${voidedBy}, ` +
                `${strValue(utils.formatDate(row['date_voided']))}, ${strValue(row['void_reason'])}, ` +
                `${changedBy}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
                `${uuid(row['uuid'])})`;
            nextPersonNameId++;
        }
    });
    let query = null;
    if (toBeinserted !== '') query = insert + toBeinserted;

    return [query, nextPersonNameId];
}

function preparePersonAddressInsert(rows, nextId) {
    let insert = 'INSERT INTO person_address (person_address_id, person_id, ' +
        'preferred, address1, address2, city_village, state_province, ' +
        'postal_code, country, latitude, longitude, creator, date_created, ' +
        'voided, voided_by, date_voided, void_reason, county_district, ' +
        'address3, address6, address5, address4, uuid, date_changed, ' +
        'changed_by, start_date, end_date';

    if(global.openmrsDataModelVersion === 2) {
        insert += ', address7, address8, address9, address10, ' +
        'address11, address12, address13, address14, address15) VALUES ';
    } else {
        insert += ') VALUES ';
    }

    let toBeinserted = '';
    rows.forEach(row => {
        let currentPersonId = beehive.personMap.get(row['person_id']);
        if (currentPersonId !== undefined) {
            if (toBeinserted.length > 1) {
                toBeinserted += ',';
            }
            let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
            let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

            toBeinserted += `(${nextId}, ${currentPersonId}, ` +
                `${row['preferred']}, ${strValue(row['address1'])}, ` +
                `${strValue(row['address2'])}, ${strValue(row['city_village'])}, ` +
                `${strValue(row['state_province'])}, ${strValue(row['postal_code'])}, ` +
                `${strValue(row['country'])}, ${strValue(row['latitude'])}, ` +
                `${strValue(row['longitude'])}, ${beehive.userMap.get(row['creator'])}, ` +
                `${strValue(utils.formatDate(row['date_created']))}, ${row['voided']}, ` +
                `${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
                `${strValue(row['void_reason'])}, ${strValue(row['county_district'])}, ` +
                `${strValue(row['address3'])}, ${strValue(row['address6'])}, ` +
                `${strValue(row['address5'])}, ${strValue(row['address4'])}, ` +
                `${uuid(row['uuid'])}, ${strValue(utils.formatDate(row['date_changed']))}, ` +
                `${changedBy}, ${strValue(utils.formatDate(row['start_date']))}, ` +
                `${strValue(utils.formatDate(row['end_date']))}`;

            if(global.openmrsDataModelVersion === 2) {
                toBeinserted +=  `, ${strValue(row['address7'])}, ${strValue(row['address8'])}, ${strValue(row['address9'])},` +
                `${strValue(row['address10'])}, ${strValue(row['address11'])}, ${strValue(row['address12'])},` +
                `${strValue(row['address13'])}, ${strValue(row['address14'])}, ${strValue(row['address15'])})`;
            } else {
                toBeinserted += ')';
            }
            nextId++;
        }
    });

    let insertStatement = null;
    if (toBeinserted !== '') insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareRelationshipTypeInsert(rows, nextId) {
    let insert = 'INSERT INTO relationship_type (relationship_type_id, a_is_to_b, ' +
        'b_is_to_a, preferred, weight, description, creator, date_created, ' +
        'uuid, retired, retired_by, date_retired, retire_reason';

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
        toBeinserted += `(${nextId}, ${strValue(row['a_is_to_b'])}, ` +
            `${strValue(row['b_is_to_a'])}, ${row['preferred']}, ${row['weight']}, ` +
            `${strValue(row['description'])}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${uuid(row['uuid'])}, ${row['retired']}, ${retiredBy}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}`;

        if(global.openmrsDataModelVersion === 2) {
            let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
            toBeinserted += `, ${changedBy}, ${strValue(utils.formatDate(row['date_changed']))})`;
        } else {
            toBeinserted += ')';
        }   
        //Update the map
        beehive.relationshipTypeMap.set(row['relationship_type_id'], nextId);
        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareRelationshipInsert(rows, nextId) {
    let insert = 'INSERT INTO relationship(relationship_id, person_a, relationship, ' +
        'person_b, creator, date_created, voided, voided_by, date_voided, ' +
        'void_reason, uuid, date_changed, changed_by, start_date, end_date)' +
        ' VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        let voidedBy = row['voided_by'] === null ? null : beehive.userMap.get(row['voided_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);

        toBeinserted += `(${nextId}, ${beehive.personMap.get(row['person_a'])}, ` +
            `${beehive.relationshipTypeMap.get(row['relationship'])}, ` +
            `${beehive.personMap.get(row['person_b'])}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${row['voided']}, ${voidedBy}, ${strValue(utils.formatDate(row['date_voided']))}, ` +
            `${strValue(row['void_reason'])}, ${uuid(row['uuid'])}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${changedBy}, ${strValue(utils.formatDate(row['start_date']))}, ` +
            `${strValue(utils.formatDate(row['end_date']))})`;

        nextId++;
    });

    let insertStatement = insert + toBeinserted;
    return [insertStatement, nextId];
}

function prepareUserInsert(rows, nextUserId) {
    let insert = 'INSERT INTO users(user_id, system_id, username, password, salt,' +
        'secret_question, secret_answer, creator, date_created, ' +
        'date_changed, person_id, retired, ' +
        'date_retired, retire_reason, uuid) VALUES ';

    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        beehive.userMap.set(row['user_id'], nextUserId);

        //Some users may be associated with persons that are not copied yet.
        let personId = beehive.personMap.get(row['person_id']);
        if(personId === undefined) {
            //This person is not yet copied.
            personId = 1;       // Place holder (to be updated later)
            copiedLaterPersonsMap.set(nextUserId, row['person_id']);
        }
        toBeinserted += `(${nextUserId}, '${row['system_id']}', ${strValue(row['username'])},` +
            `'${row['password']}', '${row['salt']}', ${strValue(row['secret_question'])}, ` +
            `${strValue(row['secret_answer'])}, ${beehive.userMap.get(row['creator'])}, ` +
            `${strValue(utils.formatDate(row['date_created']))}, ` +
            `${strValue(utils.formatDate(row['date_changed']))}, ` +
            `${personId}, ${row['retired']}, ` +
            `${strValue(utils.formatDate(row['date_retired']))}, ` +
            `${strValue(row['retire_reason'])}, ${uuid(row['uuid'])})`;

        nextUserId++;
    });

    let query = insert + toBeinserted;

    return [query, nextUserId];
}

function prepareUserAuditInfoUpdateQuery(rows) {
    let update = 'INSERT INTO users(user_id, changed_by, retired_by) VALUES '
    let lastPart = ' ON DUPLICATE KEY UPDATE changed_by = VALUES(changed_by), ' +
                   'retired_by = VALUES(retired_by)';

    let values = '';
    let toUpdate = 0;
    rows.forEach(row => {
        let destUserId = beehive.userMap.get(row['user_id']);
        let retiredBy = row['retired_by'] === null ? null : beehive.userMap.get(row['retired_by']);
        let changedBy = row['changed_by'] === null ? null : beehive.userMap.get(row['changed_by']);
        if(destUserId) {
            toUpdate++;
            if(values.length > 1) {
                values += ',';
            }
            values += `(${destUserId}, ${changedBy}, ${retiredBy})`;
        }
    });

    if(values === '') return [undefined, 0];
    return [update + values + lastPart, toUpdate];
}

function prepareRoleInsert(rows) {
    let insert = 'INSERT INTO role(role, description, uuid) VALUES ';
    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        toBeinserted += `(${strValue(row['role'])},` +
            `${strValue(row['description'])}, ` +
            `${uuid(row['uuid'])})`;
    });
    return insert + toBeinserted;
}

function preparePrivilegeInsert(rows) {
    let insert = 'INSERT INTO privilege(privilege, description, uuid) VALUES ';
    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        toBeinserted += `(${strValue(row['privilege'])},` +
            `${strValue(row['description'])}, ` +
            `${uuid(row['uuid'])})`;
    });
    return insert + toBeinserted;
}

function prepareRolePrivilegeInsert(rows) {
    let insert = 'INSERT IGNORE INTO role_privilege(role, privilege) VALUES ';
    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        toBeinserted += `(${strValue(row['role'])}, ${strValue(row['privilege'])})`;
    });
    return insert + toBeinserted;
}

function prepareRoleRoleInsert(rows) {
    let insert = 'INSERT IGNORE INTO role_role(parent_role, child_role) VALUES ';
    let toBeinserted = '';
    rows.forEach(row => {
        if (toBeinserted.length > 1) {
            toBeinserted += ',';
        }
        toBeinserted += `(${strValue(row['parent_role'])},` +
            `${strValue(row['child_role'])})`;
    });
    return insert + toBeinserted;
}

function prepareUserRoleInsert(rows) {
    let insert = 'INSERT IGNORE INTO user_role(user_id, role) VALUES ';
    let toBeinserted = '';
    rows.forEach(row => {
        let userId = beehive.userMap.get(row['user_id']);
        if (userId) {
            if (toBeinserted.length > 1) {
                toBeinserted += ',';
            }
            toBeinserted += `(${userId}, ${strValue(row['role'])})`;
        }
    });
    return insert + toBeinserted;
}

async function consolidateRolesAndPrivileges(srcConn, destConn) {
    let __addRolesNotAlreadyInDestination = async function() {
        let roleQuery = 'SELECT * FROM role';
        let [sRoles] = await srcConn.query(roleQuery);
        let [dRoles] = await destConn.query(roleQuery);

        let rolesToAdd = sRoles.filter(sRole => {
            return dRoles.every(dRole => {
                return sRole.role !== dRole.role;
            });
        });
        if (rolesToAdd.length > 0) {
            let insertStmt = prepareRoleInsert(rolesToAdd);
            utils.logDebug('Role insert statement:', insertStmt);

            let [result] = await destConn.query(insertStmt);
            return result.affectedRows;
        }
        return 0;
    };

    let __addPrivilegesNotAlreadyInDestination = async function() {
        let query = 'SELECT * FROM privilege';
        let [sPrivs] = await srcConn.query(query);
        let [dPrivs] = await destConn.query(query);

        let privToAdd = sPrivs.filter(sPriv => {
            return dPrivs.every(dPriv => {
                return sPriv.privilege.toLowerCase() !== dPriv.privilege.toLowerCase();
            });
        });
        if (privToAdd.length > 0) {
            utils.logDebug(`Privileges to be copied: ${privToAdd.length}`);
            let insertStmt = preparePrivilegeInsert(privToAdd);
            utils.logDebug('Privilege insert statement:', insertStmt);

            try {
                let [result] = await destConn.query(insertStmt);
                return result.affectedRows;
            }
            catch(ex) {
                utils.logError(`Statement during error: ${insertStmt}`);
                throw ex;
            }
        }
        return 0;
    };

    let copiedPrivileges = await __addPrivilegesNotAlreadyInDestination();
    let copiedRoles = await __addRolesNotAlreadyInDestination();

    utils.logDebug(`${copiedPrivileges} privileges & ${copiedRoles} roles copied`);
    //Insert role_privileges (insert ignore)
    let [rps] = await srcConn.query('SELECT * FROM role_privilege');
    if (rps.length > 0) {
        let stmt;
        try {
            stmt = prepareRolePrivilegeInsert(rps);
            await destConn.query(stmt);
        }
        catch(ex) {
            utils.logError(`An error occured when consolidating role_privilege table`);
            if(stmt) {
                utils.logError('Statement During error:');
                utils.logError(stmt);
            }
            throw ex;
        }
    }

    //Do the same sh*t for role_role
    let [rrs] = await srcConn.query('SELECT * FROM role_role');
    if (rrs.length > 0) {
        let stmt;
        try {
            stmt = prepareRoleRoleInsert(rrs);
            await destConn.query(stmt);
        }
        catch(ex) {
            utils.logError(`An error occured when consolidating role_role table`);
            if(stmt) {
                utils.logError('Statement During error:');
                utils.logError(stmt);
            }
            throw ex;
        }
    }
}

async function copyRelationshipTypes(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'relationship_type',
            'relationship_type_id', beehive.relationshipTypeMap);
    return await copyTableRecords(srcConn, destConn, 'relationship_type',
            'relationship_type_id', prepareRelationshipTypeInsert, condition);
}

async function copyRelationships(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'relationship', 'relationship_id');
    return await copyTableRecords(srcConn, destConn, 'relationship',
        'relationship_id', prepareRelationshipInsert, condition);
}

async function copyPersonAddresses(srcConn, destConn) {
    let condition = await utils.getExcludedIdsCondition(srcConn, 'person_address', 'person_address_id');
    return await copyTableRecords(srcConn, destConn, 'person_address',
        'person_address_id', preparePersonAddressInsert, condition);
}

async function updateCopiedUsersRoles(srcConn, destConn) {
    let excluded = '(' + global.excludedUsersIds.join(',') + ')';
    let query = `SELECT * FROM user_role WHERE user_id NOT IN ${excluded}`;
    let [rows] = await srcConn.query(query);
    if (rows.length > 0) {
        let insert;
        try {
            insert = prepareUserRoleInsert(rows);
            utils.logDebug('INSERT STATEMENT: ', insert);
            let [result] = await destConn.query(insert);
        }
        catch(ex) {
            utils.logError(`An error occured when updating copied user roles`);
            if(insert) {
                utils.logError('Statement During error:');
                utils.logError(insert);
            }
            throw ex;
        }
    }
}

async function getUsersCount(connection, condition) {
    let countQuery = 'SELECT count(*) as users_count FROM users';

    if (condition) {
        countQuery += ' WHERE ' + condition;
    }

    utils.logDebug(`Users count query: ${countQuery}`);
    try {
        let [results] = await connection.query(countQuery);
        return results[0]['users_count'];
    } catch (ex) {
        utils.logError('Error: while fetching users count');
        utils.logError('Query: ', countQuery);
        throw ex;
    }
}

async function getUsersCountIgnoreDuplicateUuids(connection, condition) {
    let countQuery = `SELECT count(*) as users_count FROM ${config.source.openmrsDb}.users`;

    if (condition) {
        countQuery += ' WHERE ' + condition+ ` AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.users)`;
    } else {
        countQuery += ` WHERE uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.users)`;
    }

    utils.logDebug(`Users count query: ${countQuery}`);
    try {
        let [results] = await connection.query(countQuery);
        return results[0]['users_count'];
    } catch (ex) {
        utils.logError('Error: while fetching users count');
        utils.logError('Query: ', countQuery);
        throw ex;
    }
}

async function getPersonsCount(connection, condition) {
    let personCountQuery = 'SELECT COUNT(*) as person_count from person';

    if (condition) {
        personCountQuery += ' WHERE ' + condition;
    }

    utils.logDebug(`Person count query: ${personCountQuery}`);
    try {
        let [results, metadata] = await connection.query(personCountQuery);
        return results[0]['person_count'];
    } catch (ex) {
        utils.logError('Error while fetching number of records in person table');
        utils.logError('Query: ', personCountQuery);
        throw ex;
    }
}

async function getPersonsCountIgnoreDuplicateUuids(connection, condition) {
    let personCountQuery = `SELECT COUNT(*) as person_count from ${config.source.openmrsDb}.person`;

    if (condition) {
        personCountQuery += ' WHERE ' + condition + ` AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.person)`;
    } else {
        personCountQuery += ` WHERE uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.person)`;
    }

    utils.logDebug(`Person count query: ${personCountQuery}`);
    try {
        let [results, metadata] = await connection.query(personCountQuery);
        return results[0]['person_count'];
    } catch (ex) {
        utils.logError('Error while fetching number of records in person table');
        utils.logError('Query: ', personCountQuery);
        throw ex;
    }
}

async function createUserTree(connection, rootUserId, tree) {
    let tempTree = null;
    if (tree === undefined || tree === null) {
        tempTree = {
            userId: rootUserId,
            children: []
        };
    } else {
        tempTree = tree;
    }
    try {
        let childrenQuery = 'SELECT user_id from users WHERE creator = ' + rootUserId;
        let [rows, fields] = await connection.query(childrenQuery);
        if (rows.length > 0) {
            rows.forEach(row => {
                if (row['user_id'] !== rootUserId) {
                    tempTree.children.push({
                        userId: row['user_id'],
                        children: []
                    });
                }
            });

            // Build tree for each child.
            tempTree.children.forEach(child => {
                createUserTree(connection, child.userId, child);
            });
        }
        return tempTree;
    } catch (ex) {
        utils.logError('Error occured while building user tree', ex);
        throw ex;
    }
}

async function copyPersonNames(srcConn, destConn) {
    let excludedPersonNamesIds = [];
    let condition = null;
    await utils.mapSameUuidsRecords(srcConn, 'person_name', 'person_name_id', excludedPersonNamesIds);
    if(excludedPersonNamesIds.length > 0) {
        let toExclude = '(' + excludedPersonNamesIds.join(',') + ')';
        condition = `person_name_id NOT IN ${toExclude}`;
    }
    return await copyTableRecords(srcConn, destConn, 'person_name',
        'person_name_id', preparePersonNameInsert, condition);
}

async function copyPersons(srcConn, destConn, srcUserId) {
    let [q, nextId] = [undefined, -1];
    try {
        // Get next person id in the destination
        let nextPersonId = await utils.getNextAutoIncrementId(destConn, 'person');
        let toExclude = '(' + global.excludedPersonIds.join(',') + ')';

        let countCondition = `creator = ${srcUserId} AND person_id NOT IN ${toExclude}`;
        let personsToCopyCount = await getPersonsCountIgnoreDuplicateUuids(srcConn, countCondition);

        // Get all person created by srcUserId in SRC database
        let startingRecord = 0;
        let personFetchQuery = `SELECT * FROM ${config.source.openmrsDb}.person WHERE creator = ${srcUserId}` +
            ` and person_id NOT IN ${toExclude} AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.person) order by date_created limit `;
        let temp = personsToCopyCount;
        let copied = 0;
        let queryLogged = false;
        while (temp % BATCH_SIZE > 0) {
            let query = personFetchQuery;
            if (Math.floor(temp / BATCH_SIZE) > 0) {
                copied = utils.addDecimalNumbers(copied, BATCH_SIZE);
                query += startingRecord + ', ' + BATCH_SIZE;
                temp = utils.subtractDecimalNumbers(temp, BATCH_SIZE);
            } else {
                copied = utils.addDecimalNumbers(copied, temp);
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord = utils.addDecimalNumbers(startingRecord, BATCH_SIZE);

            if (!queryLogged) {
                utils.logDebug('Person fetch query:', query);
            }

            let [r, f] = await srcConn.query(query);
            [q, nextId] = preparePersonInsert(r, nextPersonId);

            // Insert person records into destination machine.
            if (!queryLogged) {
                utils.logDebug('Person insert statement:\n',utils.shortenInsert(q));
                queryLogged = true;
            }

            await destConn.query(q);
            nextPersonId = nextId;
        }
        return copied;
    } catch (ex) {
        utils.logError('An error occured while moving persons...');
        if(q) {
            utils.logError('Insert statement during the error:');
            utils.logError(q);
        }
        throw ex;
    }
}

async function copyUsers(srcConn, destConn, creatorId) {
    let [insertStmt, nextId] = [undefined, -1];
    try {
        let toExclude = '(' + global.excludedUsersIds.join(',') + ')';
        let condition = `creator=${creatorId} AND user_id NOT IN ${toExclude}`;
        let nextUserId = await utils.getNextAutoIncrementId(destConn, 'users');
        let usersToCopyCount = await getUsersCountIgnoreDuplicateUuids(srcConn, condition);

        let startingRecord = 0;
        condition += ` AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.users)`;
        let userFetchQuery = `SELECT * FROM ${config.source.openmrsDb}.users WHERE ${condition} order by date_changed, date_created LIMIT `;

        let temp = usersToCopyCount;
        let copied = 0
        let logged = false;
        while (temp % BATCH_SIZE > 0) {
            let query = userFetchQuery;
            if (Math.floor(temp / BATCH_SIZE) > 0) {
                copied = utils.addDecimalNumbers(copied, BATCH_SIZE);
                query += startingRecord + ', ' + BATCH_SIZE;
                temp = utils.subtractDecimalNumbers(temp, BATCH_SIZE);
            } else {
                copied = utils.addDecimalNumbers(copied, temp);
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord = utils.addDecimalNumbers(startingRecord, BATCH_SIZE);

            if (!logged) {
                utils.logDebug('User fetch query:', query);
            }
            let [records, fields] = await srcConn.query(query);
            [insertStmt, nextId] = prepareUserInsert(records, nextUserId);

            // Insert data into destination
            await destConn.query(insertStmt);

            if(!logged) {
                utils.logDebug('User Insert Statement:');
                utils.logDebug(utils.shortenInsert(insertStmt));
                logged = true;
            }
            nextUserId = nextId;
        }
        return copied;
    } catch (ex) {
        utils.logError('Error while moving users...');
        if(insertStmt) {
            utils.logError('Insert Statement during the error:');
            utils.logError(insertStmt);
        }
        throw ex;
    }
}

async function traverseUserTree(tree, srcConn, destConn) {
    if (!tree || tree.userId === undefined) {
        throw new Error('Error! Incompatible tree passed', tree);
    }
    try {
        let copiedPersons = await copyPersons(srcConn, destConn, tree.userId);
        let copiedUsers = await copyUsers(srcConn, destConn, tree.userId);
        // For each child do the same.
        if (tree.children && tree.children.length > 0) {
            for (let i = 0; i < tree.children.length; i++) {
                let childCopied = await traverseUserTree(tree.children[i], srcConn, destConn);
                copiedPersons = utils.addDecimalNumbers(copiedPersons, childCopied.copiedPersonsCount);
                copiedUsers = utils.addDecimalNumbers(copiedUsers, childCopied.copiedUsersCount);
            }
        }

        return {
            copiedPersonsCount: copiedPersons,
            copiedUsersCount: copiedUsers
        };
    } catch (ex) {
        utils.logError('Error: while traversing tree ', JSON.stringify(tree,null,2));
        throw ex;
    }
}

async function updateUsersPersonIds(connection, idMap) {
    if(idMap.size === 0) return;

    let update = 'INSERT INTO users(user_id, person_id) VALUES '
    let lastPart = ' ON DUPLICATE KEY UPDATE person_id = VALUES(person_id)';

    let values = '';
    idMap.forEach((personId, userId) => {
        if(values.length > 1) {
            values += ',';
        }
        values += `(${userId}, ${beehive.personMap.get(personId)})`;
    });

    let statement = update + values + lastPart;
    try {
        let [r] = await connection.query(statement);
        return r.affectedRows;
    }
    catch(ex) {
        utils.logError('Error: while updating users person_id(s)');
        utils.logError('SQL statement during error');
        utils.logError(statement);
        throw ex;
    }
}

async function updateAuditInfoForPersons(srcConn, destConn) {
    //At this point the beehive.personMap is already populated
    let countQuery = `SELECT count(*) as 'count' FROM ${config.source.openmrsDb}.person WHERE (changed_by IS NOT ` +
                `NULL OR voided_by IS NOT NULL) AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.person)`;

    let queryParts = 'SELECT person_id, changed_by, voided_by FROM person ' +
                     'WHERE changed_by IS NOT NULL OR voided_by IS NOT NULL LIMIT ';

    let [count] = await srcConn.query(countQuery);
    let temp = count[0]['count'];
    let startingRecord = 0;
    let updated = 0;
    let queryLogged = false;
    let query = null;
    let [updateStmt, toUpdate] = [null, 0];
    try {
        while( temp % BATCH_SIZE) {
            //Do in batches
            query = queryParts;
            if (Math.floor(temp / BATCH_SIZE) > 0) {
                query += startingRecord + ', ' + BATCH_SIZE;
                temp = utils.subtractDecimalNumbers(temp, BATCH_SIZE);
            } else {
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord = utils.addDecimalNumbers(startingRecord, BATCH_SIZE);

            let [records] = await srcConn.query(query);
            [updateStmt, toUpdate] = preparePersonAuditInfoUpdateQuery(records);

            // Update audit info in destination
            if(toUpdate > 0) {
                if (!queryLogged) {
                    utils.logDebug('Person Audit Info fetch query:', query);
                    utils.logDebug('Person Audit Info Update statement:');
                    utils.logDebug(utils.shortenInsert(updateStmt));
                    queryLogged = true;
                }
                updated = utils.addDecimalNumbers(updated, toUpdate);
                await destConn.query(updateStmt);
            }
        }
        return updated;
    }
    catch(ex) {
        utils.logError('Error: While updating persons audit information');
        if(query) {
            utils.logError('Person Audit Info fetch query during the error:');
            utils.logError(query);
        }
        if(updateStmt) {
            utils.logDebug('Number of persons to be updated', toUpdate);
            utils.logError('Person Audit Info Update Statement during error:');
            utils.logError(updateStmt);

        }
        throw ex;
    }
}

async function updateAuditInfoForUsers(srcConn, destConn) {
    //At this point the beehive.personMap is already populated
    let toExclude = '(' + global.excludedUsersIds.join(',') + ')';
    let countQuery = `SELECT count(*) as 'count' FROM ${config.source.openmrsDb}.users ` +
                `WHERE (changed_by IS NOT NULL OR retired_by IS NOT NULL) ` +
                `AND user_id NOT IN ${toExclude} AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.users)`;

    let queryParts = 'SELECT user_id, changed_by, retired_by FROM users ' +
                'WHERE (changed_by IS NOT NULL OR retired_by IS NOT NULL) ' +
                `AND user_id NOT IN ${toExclude} LIMIT `;

    let [count] = await srcConn.query(countQuery);
    let temp = count[0]['count'];
    let startingRecord = 0;
    let updated = 0;
    let queryLogged = false;
    let query = null;
    let [updateStmt, toUpdate] = [null, 0];
    try {
        while( temp % BATCH_SIZE) {
            //Do in batches
            query = queryParts;
            if (Math.floor(temp / BATCH_SIZE) > 0) {
                query += startingRecord + ', ' + BATCH_SIZE;
                temp = utils.subtractDecimalNumbers(temp, BATCH_SIZE);
            } else {
                query += startingRecord + ', ' + temp;
                temp = 0;
            }
            startingRecord = utils.addDecimalNumbers(startingRecord, BATCH_SIZE);

            let [records] = await srcConn.query(query);
            let [updateStmt, toUpdate] = prepareUserAuditInfoUpdateQuery(records);

            // Update audit info in destination
            if(toUpdate > 0) {
                if (!queryLogged) {
                    utils.logDebug('User Audit Info fetch query:', query);
                    utils.logDebug('User Audit Info Update statement:');
                    utils.logDebug(utils.shortenInsert(updateStmt))
                    queryLogged = true;
                }
                updated = utils.addDecimalNumbers(updated, toUpdate);
                await destConn.query(updateStmt);
            }
        }
        return updated;
    }
    catch(ex) {
        utils.logError('Error: While updating users audit information');
        if(query) {
            utils.logError('User Audit Info fetch query during the error:');
            utils.logError(query);
        }
        if(updateStmt) {
            utils.logDebug('Number of users to be updated', toUpdate);
            utils.logError('User Audit Info Update Statement during error:');
            utils.logError(updateStmt);

        }
        throw ex;
    }
}

async function main(srcConn, destConn) {
    utils.logInfo('Fetching users count from source & destination...');
    const srcUsersCount = await getUsersCountIgnoreDuplicateUuids(srcConn);
    const initialDestUsersCount = await getUsersCount(destConn);

    let countCondition = 'person_id NOT IN (' + global.excludedPersonIds.join(',') + ')';
    const srcPersonCount = await getPersonsCountIgnoreDuplicateUuids(srcConn, countCondition);
    const initialDestPersonCount = await getPersonsCount(destConn);
    
    utils.logInfo(`${logTime()}: Starting to copy persons & users...`);
    utils.logInfo(`Number of persons in source db: ${srcPersonCount}`);
    utils.logInfo(`Number of users in source db: ${srcUsersCount}`);
    utils.logInfo(`Initial numnber of persons in destination: ${initialDestPersonCount}`);
    utils.logInfo(`Initial numnber of users in destination: ${initialDestUsersCount}`);

    // Get source's admin user. (This is usually user with user_id=1, user0)
    let srcAdminUserQuery = `SELECT * FROM users where system_id='admin'`;
    let [rows] = await srcConn.query(srcAdminUserQuery);

    let srcAdminUserId = rows[0]['user_id'];

    // Create the user tree.
    let tree = await createUserTree(srcConn, srcAdminUserId);
    utils.logDebug('tree:', JSON.stringify(tree, null, 2));

    // Traverse user tree performing the following for each user
    let count = await traverseUserTree(tree, srcConn, destConn);

    // Update users person ids for those users whose persons were not created by their
    // creators.
    await updateUsersPersonIds(destConn, copiedLaterPersonsMap);

    const finalDestUserCount = await getUsersCount(destConn);
    const finalDestPersonCount = await getPersonsCount(destConn);

    //Do some crude math verifications.
    let expectedFinalDestUserCount = initialDestUsersCount + count.copiedUsersCount;
    let expectedFinalDestPersonCount = initialDestPersonCount + count.copiedPersonsCount;

    if (expectedFinalDestPersonCount === finalDestPersonCount &&
        expectedFinalDestUserCount === finalDestUserCount) {
        utils.logOk(`Ok...${logTime()}: Hooraa! Persons & Users copied successfully!`);
        utils.logOk(`${count.copiedPersonsCount} persons copied. Destination's new total is ${finalDestPersonCount}`);
        utils.logOk(`${count.copiedUsersCount} users copied. Destination's new total is ${finalDestUserCount}`);

        utils.logInfo('Updating Auditing Information for person table...');
        count = await updateAuditInfoForPersons(srcConn, destConn);
        utils.logInfo(`Ok...${count} records updated`);

        utils.logInfo('Updating Auditing information for users table...');
        count = await updateAuditInfoForUsers(srcConn, destConn);
        utils.logInfo(`Ok...${count} records updated`);

        utils.logInfo('Copying person names...');
        count = await copyPersonNames(srcConn, destConn);
        utils.logOk(`Ok...${count} names copied`);

        utils.logInfo(`Consolidating & updating roles & privileges...`);
        await consolidateRolesAndPrivileges(srcConn, destConn);
        await updateCopiedUsersRoles(srcConn, destConn);
        utils.logOk('Ok...');

        utils.logInfo('Update copied persons relationships...');
        await copyRelationshipTypes(srcConn, destConn);
        await copyRelationships(srcConn, destConn);
        utils.logOk('Ok...');

        utils.logInfo('Copying person addresses...');
        await copyPersonAddresses(srcConn, destConn);
        utils.logOk('Ok...')
    } else {
        utils.logError('Expected & actual persons and/or users final numbers do not match!!');
        utils.logError(`Expected final person count in destination: ${expectedFinalDestPersonCount}`);
        utils.logError(`Actual final person count in destination: ${finalDestPersonCount}`);
        utils.logError(`Expected final users count in destination: ${expectedFinalDestUserCount}`);
        utils.logError(`Actual final users count in destination: ${finalDestUserCount}`);
        throw new Error();
    }
}

module.exports = main;
