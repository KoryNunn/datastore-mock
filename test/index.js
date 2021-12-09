var test = require('tape');
var Datastore = require('../');

test('insert and get a record', async function(t){
    t.plan(1);

    var db = new Datastore();

    await db.insert({
        key: db.key(['things', 1]),
        data: {
            abc: 123
        }
    });

    var record = await db.get(db.key(['things', 1]));

    t.deepEqual(record, [{
        abc: 123
    }]);
});

test('insert and get multiple records', async function(t){
    t.plan(2);

    var db = new Datastore();

    var record = await db.insert([
        {
            key: db.key(['things', 1]),
            data: {
                abc: 123
            }
        },
        {
            key: db.key(['things', 2]),
            data: {
                abc: 456
            }
        }
    ]);

    var record1 = await db.get(db.key(['things', 1]));

    t.deepEqual(record1, [{
        abc: 123
    }]);

    var record2 = await db.get(db.key(['things', 2]));

    t.deepEqual(record2, [{
        abc: 456
    }]);
});

test('upsert', async function(t){
    t.plan(2);

    var db = new Datastore();

    await db.insert({
        key: db.key(['things', 1]),
        data: {
            abc: 123
        }
    });

    await db.upsert({
        key: db.key(['things', 1]),
        data: {
            abc: 456
        }
    });

    await db.upsert({
        key: db.key(['things', 2]),
        data: {
            abc: 789
        }
    });

    var record1 = await db.get(db.key(['things', 1]));
    var record2 = await db.get(db.key(['things', 2]));

    t.deepEqual(record1, [{
        abc: 456
    }]);

    t.deepEqual(record2, [{
        abc: 789
    }]);
});

test('save', async function(t){
    t.plan(2);

    var db = new Datastore();

    await db.insert({
        key: db.key(['things', 1]),
        data: {
            abc: 123
        }
    });

    await db.save({
        key: db.key(['things', 1]),
        data: {
            abc: 456
        }
    });

    await db.save({
        key: db.key(['things', 2]),
        data: {
            abc: 789
        }
    });

    var record1 = await db.get(db.key(['things', 1]));
    var record2 = await db.get(db.key(['things', 2]));

    t.deepEqual(record1, [{
        abc: 456
    }]);

    t.deepEqual(record2, [{
        abc: 789
    }]);
});

test('query records', async function(t){
    t.plan(1);
    
    var db = new Datastore();

    await db.insert([
        {
            key: db.key(['things', 1]),
            data: {
                abc: 123,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 2]),
            data: {
                abc: 234,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 3]),
            data: {
                abc: 345,
                foo: 'baz'
            }
        },
        {
            key: db.key(['things', 4]),
            data: {
                abc: 456,
                foo: 'bar'
            }
        }
    ]);

    var records = await db.runQuery(
        db.createQuery('things')
        .filter('abc', '>', 123)
        .filter('foo', '=', 'bar')
    );

    t.deepEqual(records, [[
        { abc: 234, foo: 'bar' },
        { abc: 456, foo: 'bar' }
    ]]);
});

test('query records limit', async function(t){
    t.plan(1);
    
    var db = new Datastore();

    await db.insert([
        {
            key: db.key(['things', 1]),
            data: {
                abc: 123,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 2]),
            data: {
                abc: 234,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 3]),
            data: {
                abc: 345,
                foo: 'baz'
            }
        },
        {
            key: db.key(['things', 4]),
            data: {
                abc: 456,
                foo: 'bar'
            }
        }
    ]);

    var records = await db.runQuery(
        db.createQuery('things')
        .filter('abc', '>', 123)
        .filter('foo', '=', 'bar')
        .limit(1)
    );

    t.deepEqual(records, [[
        { abc: 234, foo: 'bar' }
    ]]);
});

test('select', async function(t){
    t.plan(1);
    
    var db = new Datastore();

    await db.insert([
        {
            key: db.key(['things', 1]),
            data: {
                abc: 123,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 2]),
            data: {
                abc: 234,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 3]),
            data: {
                abc: 345,
                foo: 'baz'
            }
        },
        {
            key: db.key(['things', 4]),
            data: {
                abc: 456,
                foo: 'bar'
            }
        }
    ]);

    var records = await db.runQuery(
        db.createQuery('things')
        .select('foo')
        .filter('abc', '>', 123)
        .filter('foo', '=', 'bar')
        .limit(1)
    );

    t.deepEqual(records, [[
        { foo: 'bar' }
    ]]);
});

test('select __key__', async function(t){
    t.plan(1);
    
    var db = new Datastore();

    await db.insert([
        {
            key: db.key(['things', 1]),
            data: {
                abc: 123,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 2]),
            data: {
                abc: 234,
                foo: 'bar'
            }
        },
        {
            key: db.key(['things', 3]),
            data: {
                abc: 345,
                foo: 'baz'
            }
        },
        {
            key: db.key(['things', 4]),
            data: {
                abc: 456,
                foo: 'bar'
            }
        }
    ]);

    var records = await db.runQuery(
        db.createQuery('things')
        .select('__key__')
        .filter('abc', '>', 123)
        .filter('foo', '=', 'bar')
        .limit(1)
    );

    t.deepEqual(records, [[
        '2'
    ]]);
});

test('select __key__ and other fields throws', async function(t){
    t.plan(1);
    
    var db = new Datastore();

    t.throws(function(){
        db.createQuery('things')
            .select(['__key__', 'other'])
    })
});