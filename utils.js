'use strict';
const moment = require('moment');
const mysql = require('mysql2');
const config = require('./config');

if (config.batchSize === undefined) {
    config.batchSize = 500;
};

let getNextAutoIncrementId = async function(connection, table) {
    if (arguments.length < 2) {
        throw new Error('This utility function expects connection & table in that order');
    }

    let query = 'SELECT AUTO_INCREMENT as next_auto FROM information_schema.tables ' +
                'WHERE table_name=? and table_schema=database()';
    try {
        let [r, f] = await connection.execute(query, [table]);
        return r[0]['next_auto'];
    } catch (trouble) {
        console.error('An error occured while fetching next auto increment for table ' +
            table, ':\n', trouble);
        throw trouble;
    }
};

let getCount = async function(connection, table, condition) {
    let countQuery = `SELECT count(*) as table_count FROM ${table}`;
    if (condition) {
        countQuery += ' WHERE ' + condition;
    }

    let [results] = await connection.query(countQuery);
    return results[0]['table_count'];
}

let getCountIgnoringDestinationDuplicateUuids = async function(connection, table, condition) {
    let countQuery = `SELECT count(*) as table_count FROM ${config.source.openmrsDb}.${table}`;
    if (condition) {
        countQuery += ' WHERE ' + condition + ` AND uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.${table})`;
    } else {
        countQuery += ` WHERE uuid NOT IN (SELECT uuid FROM ${config.destination.openmrsDb}.${table})`;
    }

    let [results] = await connection.query(countQuery);
    return results[0]['table_count'];
}

let formatDate = function(d, format) {
    //some how undefined is parsed by moment!!!!
    if (d == undefined) return null;
    if (moment(d).isValid()) {
        return moment(d).format('YYYY-MM-DD HH:mm:ss');
    }
    return null;
};

let logTime = function() {
    return formatDate(Date.now());
}

let stringValue = function(value) {
    return mysql.escape(value);
}

function uuid(existing) {
    return `'${existing}'`;
}

/**
 * Utility function that copies all table records in config.batchSize batches
 * @param srcConn
 * @param destConn
 * @param tableName:String Name of table whose records are to be copied.
 * @param orderColumn:String Name of the column to order records with.
 * @param insertQueryPrepareFunction: function prepares the insert query
 * @param condition: String, an sql condition that is appended to count & fetch queries.
 * @return count of records copied. (or a promise that resolves to count)
 */
let copyTableRecords = async function(srcConn, destConn, tableName, orderColumn,
    insertQueryPrepareFunction, condition) {
    // Get the count to be pushed
    let countToCopy = await getCount(srcConn, tableName, condition);
    logDebug(`Table ${tableName}  number of records to be copied is ${countToCopy}`);
    let nextAutoIncr = await getNextAutoIncrementId(destConn, tableName);

    let fetchQuery = `SELECT * FROM ${tableName} `;
    if(condition) {
        fetchQuery += `WHERE  ${condition} `;
    }
    fetchQuery += `ORDER by ${orderColumn} LIMIT `;
    let start = 0;
    let temp = countToCopy;
    let copied = 0;
    let queryLogged = false;
    let query = null;
    let [q, nextId] = [null, -1];
    try {
        while (temp % config.batchSize > 0) {
            query = fetchQuery;
            if (Math.floor(temp / config.batchSize) > 0) {
                query += start + ', ' + config.batchSize;
                temp = subtractDecimalNumbers(temp, config.batchSize);
            } else {
                query += start + ', ' + temp;
                temp = 0;
            }
            start = addDecimalNumbers(start, config.batchSize);
            let [r] = await srcConn.query(query);
            [q, nextId] = insertQueryPrepareFunction.call(null, r, nextAutoIncr);

            if (!queryLogged) {
                logDebug(`${tableName} insert statement:\n`, shortenInsertStatement(q));
                queryLogged = true;
            }

            if(q) {
                [r] = await destConn.query(q);
                copied =  addDecimalNumbers(copied, r.affectedRows);
            }

            nextAutoIncr = nextId;
        }
        return copied;
    }
    catch(ex) {
        logError(`An error occured when moving ${tableName} records`);
        if(q) {
            logError('Select statement:', query);
            logError('Insert statement during error');
            logError(q);
        }
        throw ex;
    }
}

let logError = function(...args) {
    args.splice(0, 0, "\x1b[31m ERROR:");
    console.error.apply(null, args);
}

let logOk = function(...args) {
    args.splice(0, 0, "\x1b[32m OK:");
    console.log.apply(null, args);
}

let logDebug = function(...args) {
    args.splice(0, 0, "\x1b[33m DEBUG:");
    if (config.debug) {
        console.log.apply(null, args);
    }
}

let logInfo = function(...args) {
    args.splice(0, 0, "\x1b[37m INFO:");
    console.log.apply(null, args);
}

let shortenInsertStatement = function(statement) {
    let charcount = 700;
    if(statement === undefined || statement === null) return statement;
    let valuesIndex = statement.indexOf('VALUES');
    if(valuesIndex == -1)return statement
    if(statement.substring(valuesIndex).length <= charcount) return statement;

    let lastParenth = statement.lastIndexOf(')', valuesIndex + charcount);
    return statement.substring(0, lastParenth + 1) + '...';
}

let addDecimalNumbers = (n1, n2) => Number.parseInt(n1, 10) + Number.parseInt(n2, 10);
let subtractDecimalNumbers = (n1, n2) => Number.parseInt(n1, 10) - Number.parseInt(n2, 10);

async function personIdsToexclude(connection) {
    // Get the person associated with daemon user
    let exclude = `SELECT person_id from users WHERE system_id IN ('daemon', 'admin')`;
    let [ids] = await connection.query(exclude);
    return ids.map(id => id['person_id']);
}

/**
 * Utility function that creating the mapping records that share same UUIDs between source and destination
 * @param {MySQLConnectin} connection 
 * @param {String} table 
 * @param {String} column 
 * @param {Map} map 
 */
async function mapSameUuidsRecords(connection, table, column, arrayOfExcludedIds, map) {
    let query = `SELECT t1.${column} source_value, t2.${column} dest_value `
        + `FROM ${config.source.openmrsDb}.${table} t1 INNER JOIN ${config.destination.openmrsDb}.${table} t2 using(uuid)`;
    try {
        let [records] = await connection.query(query);
        if(map) {
            records.forEach(record => {
                map.set(record['source_value'], record['dest_value']);
                arrayOfExcludedIds.push(record['source_value']);
            });
        } else {
            records.forEach(record => {
                arrayOfExcludedIds.push(record['source_value']);
            });
        }
        
    } catch(trouble) {
        logError(`Error while mapping same uuids records for table ${table}`);
        logError(`Query during error: ${query}`);
        throw trouble;
    }
}

/**
 * Utility function that creates a condition to exclude record ids with same UUID as another record in destination.
 * @param {MysqlConnection} connection 
 * @param {String} table 
 * @param {String} idColumn 
 * @param {Map} idMap 
 * @returns {String} Condition to be added to where clause when fetching records to be copied
 */
async function getExcludedIdsCondition(connection, table, idColumn, idMap) {
    let arrayOfExcludedIds = [];
    await mapSameUuidsRecords(connection, table, idColumn, arrayOfExcludedIds, idMap);
    if(arrayOfExcludedIds.length > 0) {
        let toExclude = '(' + arrayOfExcludedIds.join(',') + ')';
        return `${idColumn} NOT IN ${toExclude}`;
    }
    return null;
}

module.exports = {
    getNextAutoIncrementId: getNextAutoIncrementId,
    getCount: getCount,
    getCountIgnoringDestinationDuplicateUuids: getCountIgnoringDestinationDuplicateUuids,
    stringValue: stringValue,
    copyTableRecords: copyTableRecords,
    formatDate: formatDate,
    logTime: logTime,
    logOk: logOk,
    logError: logError,
    logDebug: logDebug,
    logInfo: logInfo,
    uuid: uuid,
    shortenInsert: shortenInsertStatement,
    personIdsToexclude: personIdsToexclude,
    addDecimalNumbers: addDecimalNumbers,
    subtractDecimalNumbers: subtractDecimalNumbers,
    mapSameUuidsRecords: mapSameUuidsRecords,
    getExcludedIdsCondition: getExcludedIdsCondition
};
