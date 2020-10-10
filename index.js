var { v4: uuid } = require('uuid');
var operators = require('./operators');
var KEY_SELECT = '__key__';
var KEY_EXISTS = Symbol('KEY_EXISTS');

function filter(query, field, operator, value){
    query.filters.push([field, operator, value]);
    return createQuery(query);
}

function limit(query, limit){
    query.limit = limit;
    return createQuery(query);
}

function select(query, fields){
    query.select = [].concat(fields);

    if(query.select.length > 1 && query.select.includes(KEY_SELECT)){
        throw new Error('Cannot mix __key__ select with other fields');
    }

    return createQuery(query);
}

function createQuery(query){
    return {
        filter: filter.bind(null, query),
        limit: limit.bind(null, query),
        select: select.bind(null, query),
        query
    };
}

function pick(collection, selectFields, key){
    if(selectFields == null) {
        return { ...collection[key] };
    }

    if(selectFields.includes(KEY_SELECT)){
        return key;
    }

    return selectFields.reduce((result, field) => {
        if(field === '__key__') {
            return key
        }

        result[field] = collection[key][field];
        return result;
    }, {});
}

async function runQuery(data, query){
    var collection = data[query.query.collection];
    var filters = query.query.filters;
    var limit = query.query.limit;
    var selectFields = query.query.select;
    var collectionPick = pick.bind(null, collection, selectFields);

    return [Object.keys(collection || {}).filter(key => 
        filters.every(filter => 
            operators[filter[1]](
                collection[key][filter[0]],
                filter[2]
            )
        )
    )
    .map(collectionPick)
    .slice(0, limit || Infinity)];
}

async function get(data, keys){
    return [keys].flat().map(key => {
        var { collection, id } = key

        if(!(collection in data)){
            throw `Collection ${collection} does not exist`;
        }

        if(!(id in data[collection])){
            throw `Key ${JSON.stringify(key)} does not exist`;
        }
        
        return data[collection][id];
    })
}

async function set(data, collection, id, record){
    data[collection] = data[collection] || {};

    data[collection][id] = record;
}

async function insertOne(data, config) {
    var key = config.key;
    var record = config.data;
    var { collection, id } = key;

    if(!id){
        id = uuid()
    } else if(await get(data, key).catch(() => null)){
        if(id in data[collection]){
            var error = new Error(`Key ${JSON.stringify(key)} exists`);
            error[KEY_EXISTS] = true;
            throw error;
        }
    }

    return set(data, collection, id, record);
}

async function insert(data, configs){
    await Promise.all([configs].flat().map(config => insertOne(data, config)));
}

async function updateOne(data, config) {
    var key = config.key;
    var updatedRecord = config.data;
    var { collection, id } = key;

    await get(data, key);
    await set(data, collection, id, updatedRecord);
}

async function update(data, configs){
    return Promise.all([configs].flat().map(config => updateOne(data, config)));
}

async function upsert(data, configs){
    await Promise.all([configs].flat().map(async function(config) {
        try {
            return await insertOne(data, config);
        } catch(error) {
            if(!error[KEY_EXISTS]){
                throw error;
            }

            return updateOne(data, config);
        }
    }));
}

function createKey(value){
    var collection;
    var id;

    if(Array.isArray(value)){
        collection = value[0];
        id = value[1];
    } else {
        collection = value;
    }

    return { collection, id };
}

function Datastore(){
    var data = {};

    return {
        insert: insert.bind(null, data),
        update: update.bind(null, data),
        upsert: upsert.bind(null, data),
        get: get.bind(null, data),
        key: createKey,
        runQuery: (query) => runQuery(data, query),
        createQuery: function(collection){
            return createQuery({
                collection,
                filters: []
            })
        }
    };
};

module.exports = Datastore;