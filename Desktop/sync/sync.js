/**
* This exercise has you implement a synchronize() method that will
* copy all records from the sourceDb into the targetDb() then start
* polling for changes. Places where you need to add code have been
* mostly marked and the goal is to get the runTest() to complete
* successfully.
*
*
* Requirements:
*
* Try to solve the following pieces for a production system. Note,
* doing one well is better than doing all poorly. If you are unable to
*  complete certain requirements, please comment your approach to the
* solution. Comments on the "why" you chose to implement the code the
* way you did is highly preferred.
*
* 1. syncAllNoLimit(): Make sure what is in the source database is
*    in our target database for all data in one sync. Think of a naive
*    solution.
* 2. syncAllSafely(): Make sure what is in the source database is in
*    our target database for all data in batches during multiple
*    syncs. Think of a pagination solution.
* 3. syncNewChanges(): Make sure updates in the source database is in
*    our target database without syncing all data for all time. Think
*    of a delta changes solution.
* 4. synchronize(): Create polling for the sync. A cadence where we
*    check the source database for updates to sync.
*
* Feel free to use any libraries that you are comfortable with and
* change the parameters and returns to solve for the requirements.
*
*
* You will need to reference the following API documentation:
*
* Required: https://www.npmjs.com/package/nedb
* Required: https://github.com/bajankristof/nedb-promises
* Recommended: https://lodash.com/docs/4.17.15
* Recommended: https://www.npmjs.com/package/await-the#api-reference
*/

const Datastore = require('nedb-promises');
const _ = require('lodash.chunk');
const the = require('await-the');
const { isArray } = require('lodash');

// The source database to sync updates from.
const sourceDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
});

// The target database that sendEvents() will write too.
const targetDb = new Datastore({
    inMemoryOnly: true,
    timestampData: true
});

let TOTAL_RECORDS;
let EVENTS_SENT = 0;


const load = async () => {
    // Add some documents to the collection.
    // TODO: Maybe dynamically do this? `faker` might be a good library here.
    await sourceDb.insert({ name : 'GE', owner: 'test', amount: 1000000 });
    await the.wait(300);
    await sourceDb.insert({ name : 'Exxon', owner: 'test2', amount: 5000000 });
    await the.wait(300);
    await sourceDb.insert({ name : 'Google', owner: 'test3', amount: 5000001 });

    TOTAL_RECORDS = 3;
}


/**
* API to send each document to in order to sync.
*/
const sendEvent = data => {
    EVENTS_SENT += 1;
    console.log('event being sent: ');
    console.log(data);
    targetDb.insert(data)

    // TODO: Write data to targetDb
    // await targetDb.insert(data);
};


// Find and update an existing document.
const touch = async name => {
    await sourceDb.update({ name }, { $set: { owner: 'test4' } });
};
const targetTouch = async (dataName, dataOwner) => {
    const target = await targetDb.findOne({dataName})
    console.log(target)
    await targetDb.update({ dataName }, { $set: { owner: dataOwner } });
    // EVENTS_SENT += 1
}


/**
* Utility to log one record to the console for debugging.
*/
const read = async name => {
    const record = await sourceDb.findOne( { name });
    console.log("the record",record);
};
const reader = async name => {
    const record2 = await targetDb.findOne( { name });
    console.log("the target record",record2)
}

/**
* Get all records out of the database and send them using
* 'sendEvent()'.
*/
const syncAllNoLimit = async () => {
    // TODO
    //Find all records in the source database
    //Puts all elements into an array to be more easily accessed

    const target = await sourceDb.find({owner: /t/}, function(err,docs){});

    //Called a forEach to more easily insert each document in the targetDb
    //Easier as well to call sendEvent
    target.forEach(element => {
        sendEvent(element)
    });
}


/**
* Sync up to the provided limit of records. Data returned from
* this function will be provided on the next call as the data
* argument.
*/
 const syncWithLimit = async (limit, data) => {
    // TODO
    // Create a while loop queue to make batches
    // used the length of the data array in order and shitfed them
    // off each time to allow the loop to end
    while(data.length > 0){

        let queue = []
        // For loop used in order to push docs into the queue so that the
        // queue's length never exceeds the batch limit
        for(let i = 0; i < limit; i++){

            // Placed this edge case in the loop to make sure that no
            // undefined entries are inserted into the targetDb
            if(data[i] === undefined){
                break
            }

            queue.push(data[i]);

            data.shift();

        }


        queue.forEach(async element => {
            // sendEvent used here as it already inserts info into the targetDb
            // above and incremented EVENTS_SEND.
            sendEvent(element)

        });

        console.log("End of batch")

        // Breaks the loop if EVENTS_SENT equals total records.
        if(EVENTS_SENT === TOTAL_RECORDS){
            break
        }


    }

    // Sets lastResultSize to 0 to break out of the loop in syncAllSafely
    return data.lastResultSize = 0;
}


/**
* Synchronize in batches.
*/
const syncAllSafely = async (batchSize, data) => {
    // FIXME: Example implementation.
    if (!data) {
        data = await sourceDb.find({owner: /t/}, function(err,docs){});
    }

    data.lastResultSize = -1;
    await the.while(
        () => data.lastResultSize != 0,
        async () => await syncWithLimit(batchSize, data)
    );

    return data;
}


const sync = async (int, data, target) => {
    for(let i = 0; i < data.length; i++){
        if(data[1].updatedAt === target[1].updatedAt){
            continue
        } else {
            let names = data[i].name
            let owners = data[i].owner
            targetTouch(names,owners)
            EVENTS_SENT += 1
        }
    }
    return data
}

/**
 * Sync changes since the last time the function was called with
* with the passed in data.
*/
const syncNewChanges = async (int, data) => {
    // TODO
    // Put docs in sourceDb and targetDb in to arrays
    // This allows each entry in them to be compared.
    target = await targetDb.find({}, function(err,docs){});
    if (!data) {
        data = await sourceDb.find({}, function(err,docs){});
    }


    for(let i = 0; i < data.length; i++){
        if(data[i].updatedAt === target[i].updatedAt){
            continue
        } else {
            let names = data[i].name
            console.log("Changed name", names)
            let owners = data[i].owner
            console.log("Changed owner", owners);
            targetTouch(names,owners)
            EVENTS_SENT += 1
        }
    }

    return data;
}


/**
* Implement function to fully sync of the database and then
 * keep polling for changes.
*/
const synchronize = async () => {
     // TODO
}


/**
 * Simple test construct to use while building up the functions
 * that will be needed for synchronize().
*/
 const runTest = async () => {

    await load();

    // Check what the saved data looks like.
    await read('GE');

    EVENTS_SENT = 0;
    const data = await syncAllNoLimit();

    // TODO: Maybe use something other than logs to validate use cases?
    // Something like `assert` or `chai` might be good libraries here.
    if (EVENTS_SENT === TOTAL_RECORDS) {
        console.log('1. synchronized correct number of events')
    }

    EVENTS_SENT = 0;
    // const data = await syncAllSafely(1)

    // await reader('Google')
    // await reader('Exxon')
    // await reader('GE')

    if (EVENTS_SENT === TOTAL_RECORDS) {
        console.log('2. synchronized correct number of events')
    }

    // Make some updates and then sync just the changed files.
    EVENTS_SENT = 0;
    await the.wait(300);
    await touch('GE');
    await syncNewChanges(1, data);

    if (EVENTS_SENT === 1) {
        console.log('3. synchronized correct number of events')
    }

}

// TODO:
// Call synchronize() instead of runTest() when you have synchronize working
// or add it to runTest().
runTest();
// synchronize();
