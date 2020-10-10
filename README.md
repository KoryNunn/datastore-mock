# datastore-mock

A google datastore mock that runs in your node process for easy integration testing.

# install

`npm i datastore-mock`

# Usage

```js
var Datastore = require('datastore-mock');

var db = new Datastore();

// insert and get a record
await db.insert({
    key: db.key(['things', 1]),
    data: {
        abc: 123
    }
});

var records = await db.get(db.key(['things', 1]));

records -> [
    {
        abc: 123
    }
]

// Run a query
var records = await db.runQuery(
    db.createQuery('things')
    .filter('abc', '>', 123)
    .filter('foo', '=', 'bar')
    .limit(1)
);
```

# Limitations

The following API's are implemented:

 - insert
 - update
 - upsert
 - get
 - key
 - runQuery
 - createQuery
     - filter
     - limit
     - select

More are trivially implementable, log an issue or send me a PR.