(function() {
    global.beehive = {};
    global.excludedPersonIds = [];
    global.excludedUsersIds = [];
    const utils = require('./utils');
    const stringValue = utils.stringValue;

    // personMap also represents patientMap because person & patient
    // are one to one
    let beehiveMapNames = [
        'personMap',
        'personAttributeTypeMap',
        'relationshipTypeMap',
        'userMap',
        'identifierTypeMap',
        'locationMap',
        'encounterMap',
        'encounterRoleMap',
        'encounterTypeMap',
        'providerAttributeTypeMap',
        'providerMap',
        'visitTypeMap',
        'visitMap',
    ];

    beehiveMapNames.forEach(mapName => {
        global.beehive[mapName] = new Map();
    });

    // Use Object literal for obsMap to avoid the Map's number of entries hard limit of 16777216
    // Object literal has more than 100 million hard limit on the number of keys.
    // Also make the obsMap consintent with Map API
    global.beehive['obsMap'] = {
        set: function(key, value) {
            this[key] = value;
        },
        get: function(key) {
            return this[key];
        }
    };
    
    async function _sourceAlreadyExists(connection, source) {
        let query = 'SELECT source FROM beehive_merge_source where source = ' +
            `'${source}'`;
        [result] = await connection.query(query);
        if (result.length > 0) {
            return true;
        }
        return false;
    }

    async function prepareForNewSource(srcConn, destConn, config) {
        let source = config.source.location;
        let persist = config.persist || false;

        let check = `SHOW TABLES LIKE 'beehive_merge_source'`;
        let [result] = await destConn.query(check);
        if (result.length === 0) {
            //Not created yet.
            await _createSourceTable(destConn);
            if (persist) {
                await _createTables(destConn);
            }
            // await _insertSource(destConn, source);
        } else {
            // TODO: If we decide to do transaction in chunks this section will be relevant
            // check if source already exists.
            let sourceExists = await _sourceAlreadyExists(destConn, source);
            if (persist) {
                if (!sourceExists) {
                    // Initial run
                    await _insertSource(destConn, source);
                } else {
                    // Second or more run
                    // TODO: Populate the maps from persisted tables
                }
            }
            else {
                if(sourceExists){
                    let error = `Location ${source} already processed`;
                    throw new Error(error);
                }
            }
        }
        
        // Prepare exclude ids, map the excluded ids to their destination counterparts.
        await _prepareForDryRun(srcConn, destConn, config);
    }

    function _createMapTable(tableSuffix) {
        let stmt = 'CREATE TABLE IF NOT EXISTS beehive_merge_' + tableSuffix +
            '(source VARCHAR(50),' +
            'src_id INT(11) NOT NULL,' +
            'dest_id INT(11) NOT NULL,' +
            'UNIQUE(source, src_id)' +
            ')';
        return stmt;
    }

    async function _insertSource(connection, source) {
        let s = `insert into beehive_merge_source(source) values(${stringValue(source)})`;
        utils.logDebug(s);
        let [r] = await connection.query(s);
        return r.affectedRows;
    }

    async function _createSourceTable(connection) {
        let sourceTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_source(' +
            'source VARCHAR(50) PRIMARY KEY, merge_date datetime NULL DEFAULT CURRENT_TIMESTAMP' +
            ')';

        utils.logDebug(sourceTable);
        await connection.query(sourceTable);
    }

    async function _createTables(connection) {
        let progressTable = 'CREATE TABLE IF NOT EXISTS beehive_merge_progress(' +
            'id INT(11) AUTO_INCREMENT PRIMARY KEY,' +
            'source VARCHAR(50) NOT NULL,' +
            'atomi_step VARCHAR(50) NOT NULL,' +
            'passed TINYINT,' +
            'time_finished TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP' +
            ')';

        let tables = [
            progressTable
        ];

        //Create these tables.
        for (let i = 0; i < tables.length; i++) {
            utils.logDebug(tables[i]);
            await connection.query(tables[i]);
        };

        for (let i = 0; i < beehiveMapNames.length; i++) {
            let suffix = beehiveMapNames[i].toLowerCase();
            let mapTable = _createMapTable(suffix);
            if (i === 0) utils.logDebug('MapTables Statement', mapTable);
            await connection.query(mapTable);
        }
    }

    async function _usersAndAssociatedPersonsToExclude(srcConn, destConn) {
        let q = `SELECT * FROM users`;
        let [srcUsers] = await srcConn.query(q);
        let [destUsers] = await destConn.query(q);

        srcUsers.forEach(su => {
            let match = destUsers.find(du => {
                return ((su['system_id'] === du['system_id'] &&
                            su['username'] === du['username'])
                            || su['uuid'] === du['uuid']);
            });

            if(match) {
                global.excludedUsersIds.push(su['user_id']);
                global.beehive.userMap.set(su['user_id'], match['user_id']);
            }
        });
    }

    async function _prepareForDryRun(srcConn, destConn, config) {
        // prepare the excluded person_ids
        utils.logInfo('Determining users records already existing in destination');
        await _usersAndAssociatedPersonsToExclude(srcConn, destConn);

        utils.logInfo('Determining person records already copied (criterion is similar UUID)...');
        await utils.mapSameUuidsRecords(srcConn, 'person', 'person_id', global.excludedPersonIds, global.beehive.personMap);
    }

    module.exports = {
        prepare: prepareForNewSource,
        insertSource: _insertSource,
        prepareForDryRun: _prepareForDryRun,
    };
})();
