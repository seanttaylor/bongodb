import express from 'express';
import bodyParser from 'body-parser';
import morgan from 'morgan';
import figlet from 'figlet';

import crypto from 'node:crypto';
import randomPetName from 'node-petname';
import { promisify } from 'util';

import { ObjectId } from 'mongodb';
import { MongoClient } from 'mongodb';

const APP_NAME = process.env.APP_NAME || 'bongodb';
const APP_VERSION = process.env.APP_VERSION || '0.0.1';
const PORT = process.env.PORT || 3000;
const CONNECTED_CLIENTS = {}; // Store clients per database for prototype simplicity

const figletize = promisify(figlet);
const banner = await figletize(`${APP_NAME} v${APP_VERSION}`);
const app = express();
let server;

app.use(express.json());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(morgan('tiny'));

/**
 * 
 * @param {Object} obj 
 * @returns {Object}
 */
function removeComputedProperties(obj) {
    return Object.fromEntries(
      Object.entries(obj).filter(([key]) => !key.startsWith('_'))
    );
}
  

// DatabaseService
app.post('/databases/:databaseName', async (req, res) => {
    const { databaseName } = req.params;
    const { uri } = req.query;

    if (!databaseName || !uri) {
        res.status(400).json({
            status: 'NOT_CONNECTED', 
            message: 'Missing required fields: databaseName or uri', 
            timestamp: new Date().toISOString()
        });
        return;
    }

    try {
        const client = new MongoClient(decodeURIComponent(uri)); 
        const connectionName = randomPetName(2, '-');

        await client.connect();
        await client.db(databaseName).command({ ping: 1 });

        CONNECTED_CLIENTS[databaseName] = { client, connectionName }; // Store client in memory for now

        res.json({ 
            name: connectionName,
            status: 'CONNECTED', 
            message: `Connected to database: (${databaseName})`, 
            timestamp: new Date().toISOString()
        });
    } catch (ex) {
        console.error(`Exception encountered while connecting to database: (${databaseName}) See details -> ${ex.message}`);
        res.status(500).json({ 
            status: 'NOT_CONNECTED', 
            message: `Exception while connecting to database: (${databaseName})`, 
            timestamp: new Date().toISOString()
        });
    }
});

app.delete('/databases/:databaseName', async (req, res) => {
    const { databaseName } = req.params;

    if (!databaseName) {
        return res.status(400).json({ 
            status: 'CONNECTED', 
            message: 'Missing required field: databaseName', 
            timestamp: new Date().toISOString()
        });
    }

    const { client, connectionName } = CONNECTED_CLIENTS[databaseName];

    if (!client) {
        return res.status(404).json({ 
            status: 'UNKNOWN', 
            message: `No connection found for database: (${databaseName})`,
            timestamp: new Date().toISOString() 
        });
    }

    try {
        await client.close();
        delete CONNECTED_CLIENTS[databaseName];

        res.json({ 
            name: connectionName,
            status: 'DISCONNECTED', 
            message: `Disconnected from database: (${databaseName})`,
            timestamp: new Date().toISOString()
        });
    } catch (ex) {
        console.error(`INTERNAL_ERROR: Exception encountered while disconnecting (${databaseName}). See details -> ${ex.message}`);
        res.status(500).json({ 
            status: 'UNKNOWN', 
            message: `Exception while disconnecting from (${databaseName})`,
            timestamp: new Date().toISOString() 
        });
    }
});

//QueryService
app.put('/databases/:databaseName/collections/:collectionName', async (req, res) => {
    const { databaseName, collectionName } = req.params;
    const { client, connectionName } = CONNECTED_CLIENTS[databaseName];

    try {
        const resourceId = req.body._id ? new ObjectId(req.body._id) : undefined;
        const ifMatch = req.headers['if-match'];

        if (!client) {
            res.status(404).json({ 
                name: connectionName, 
                message: `No existing connection found for database: (${databaseName})`,
                timestamp: new Date().toISOString() 
            });
            return;
        }

        const query = { _id: resourceId };
        // we have to remove non-computed properties to ensure the generated ETag can match
        // objects sent by the client
        const existingResource = await client.db(databaseName).collection(collectionName).findOne(query);

        if (existingResource) {
            const { _lastModified, _createdAt } = existingResource;
            const nonComputedResource = removeComputedProperties(existingResource);
            const currentETag = crypto.createHash('sha256').update(JSON.stringify(nonComputedResource)).digest('base64');
            
            if (!ifMatch) {
                // Request intent unclear, return 409 Conflict with ETag and Last-Modified headers
                res.setHeader('ETag', currentETag);
                res.setHeader('Last-Modified', new Date(_lastModified || _createdAt).toISOString());
                res.status(409).json({
                    message: 'CONFLICT: Resource exists; missing required header. Request *MUST* include If-Match header set to ETag of resource',
                    timestamp: new Date().toISOString()
                });
                return;
            }

            if (ifMatch !== currentETag) {
                // Precondition Failed with ETag and Last-Modified headers
                res.setHeader('ETag', currentETag);
                res.setHeader('Last-Modified', new Date(_lastModified || _createdAt).toISOString());
                res.status(412).json({
                  message: 'PRECONDITION_FAILED: Request ETag header value *MUST* match that of the existing resource',
                  timestamp: new Date().toISOString()
                });
                return;
            }

            // Exclude computed properties from request body before merging
            const nonComputedBody = removeComputedProperties(req.body);
 
            // Update the specified resource
             const updatedDocument = {
                ...nonComputedResource,
                ...nonComputedBody,
                _id: resourceId, // Preserve MongoDB ObjectId
                _lastModified: new Date().toISOString(),
                _createdAt
              };

            await client.db(databaseName).collection(collectionName).replaceOne({ _id: resourceId }, updatedDocument);

            const newETag = crypto.createHash('sha256').update(JSON.stringify(nonComputedBody)).digest('base64');
            const newLastModified = new Date(updatedDocument._lastModified).toISOString();

            res.setHeader('ETag', newETag);
            res.setHeader('Last-Modified', newLastModified);
            res.status(204).json();

        } else {
            // Create the resource if it doesn't exist
            const _createdAt = new Date().toISOString();
            const nonComputedBody = removeComputedProperties(req.body);
            const newResource = { ...nonComputedBody, _createdAt, _lastModified: null };

            const { insertedId } = await client.db(databaseName).collection(collectionName).insertOne(newResource);
            const newETag = crypto.createHash('sha256').update(JSON.stringify(req.body)).digest('base64');            

            res.setHeader('ETag', newETag);
            res.setHeader('Last-Modified', _createdAt);
            res.status(201).json({ _id: insertedId });
        } 
    } catch (ex) {
        console.error(`INTERNAL_ERROR (QueryService): Exception encountered while inserting into collection (${databaseName}/${collectionName}) See details -> ${ex.message}`);
        res.status(500).json({
            name: connectionName,
            message: `INTERNAL_ERROR: could not insert document into collection (${collectionName})`,
            timestamp: new Date().toISOString() 
        });
    }
});

app.get('/databases/:databaseName/collections/:collectionName', async (req, res) => {
    const { databaseName, collectionName } = req.params;
    const { filter, limit, skip } = req.query;
    const { client, connectionName } = CONNECTED_CLIENTS[databaseName];

    try {
        const config = filter ? JSON.parse(decodeURIComponent(filter)) : { query: {}, projection: {}, options: {} };

        if (!client) {
            res.status(404).json({ 
                status: 'UNKNOWN', 
                message: `No existing connection found for database: (${databaseName})`,
                timestamp: new Date().toISOString() 
            });
            return;
        }
        
        config.options.limit = parseInt(limit, 10) || 0;
        config.options.skip = parseInt(skip, 10) || 0;
    
        const results = await client
            .db(databaseName)
            .collection(collectionName)
            .find(config.query, config.projection, config.options)
            .toArray();
  
      res.status(200).json({
        items: results,
        count: results.length
      });
    } catch (ex) {
        console.error(`INTERNAL_ERROR (QueryService): Exception encountered fetching from collection (${databaseName}/${collectionName}) See details -> ${ex.message}`);
        res.status(500).json({ 
            name: connectionName,
            status: 'UNKNOWN', 
            message: `Exception during fetch on collection (${databaseName}/${collectionName})`,
            timestamp: new Date().toISOString()
        });
    }
});

app.get('/databases/:databaseName/collections/:collectionName/:mongoId', async (req, res) => {
    const { databaseName, collectionName, mongoId } = req.params;
    const { client, connectionName } = CONNECTED_CLIENTS[databaseName];

    try {
        if (!client) {
            res.status(404).json({
                name: connectionName, 
                message: `No existing connection found for database: (${databaseName})`,
                timestamp: new Date().toISOString() 
            });
            return;
        }

        if (!ObjectId.isValid(mongoId)) {
            return res.status(400).json({ 
                name: connectionName,
                message: `The Mongo objectId (${mongoId}) is invalid` ,
                timestamp: new Date().toISOString()
            });
        }

        const result = await client
            .db(databaseName)
            .collection(collectionName)
            .findOne({ _id: new ObjectId(mongoId) });

        if (result) {
            res.status(200).json({
                items: [result],
                count: 1 
            });
        } else {
            res.status(404).json({ 
                items: [],
                count: 0  
            });
        }
    } catch (ex) {
        console.error(`INTERNAL_ERROR (QueryService): Exception encountered during document lookup (${collectionName}/${mongoId}) See details -> ${ex.message}`);
        res.status(500).json({ 
            name: connectionName,
            message: `Exception encountered during lookup (${collectionName}/${mongoId})`,
            timestamp: new Date().toISOString() 
        });
    }
});


app.use((req, res) => {
    res.status(404).send({ status: 404, message: 'Not Found' });
});
  
app.use((err, req, res, next) => {
    const status = err.status || 500;
    console.error(err);
    res.status(status).send({ status, message: 'There was an error.' });
});
  
server = app.listen(PORT, () => {
    console.log(banner);
    console.log(`\nApp listening on http://localhost:${PORT}`);
});
  
process.on('SIGTERM', () => {
    console.warn('Warn: SIGTERM signal received: shutting down...');
  
    server.close(() => {
      console.warn(`Warn: App offline`);
    });
})
