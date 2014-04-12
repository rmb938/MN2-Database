package com.rmb938.database;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.logging.Logger;

public class MongoDatabase extends Database {

    private static Logger logger = Logger.getLogger(MongoDatabase.class.getName());

    private final String database;
    private final String address;
    private final int port;

    public MongoDatabase(String database, String address, int port) {
        this.database = database;
        this.address = address;
        this.port = port;
    }

    @Override
    public void setupDatabase() {

    }

    private DB getDatabase(MongoClient mongoClient) {
        return mongoClient.getDB(database);
    }

    private MongoClient getClient() throws UnknownHostException {
        return new MongoClient(address, port);
    }

    public void returnClient(MongoClient mongoClient) {
        mongoClient.close();
    }

    private DBCollection getCollection(MongoClient mongoClient, String collectionName) {
        DB database = getDatabase(mongoClient);
        return database.getCollection(collectionName);
    }

    public boolean collectionExists(String collectionName) {
        MongoClient client;
        boolean exists = false;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return false;
        }
        if (client != null) {
            DB database = getDatabase(client);
            exists = database.collectionExists(collectionName);
            returnClient(client);
        }
        return exists;
    }

    public void createCollection(String collectionName) {
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        if (client != null) {
            DB database = getDatabase(client);
            database.createCollection(collectionName, new BasicDBObject("capped", false));
        }
    }

    public DBObject findOne(String collection, DBObject query) {
        DBObject dbObject = null;
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);

            dbObject = dbCollection.findOne(query);

            returnClient(client);
        }
        return dbObject;
    }

    public void remove(String collection, DBObject query) {
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);

            dbCollection.remove(query);

            returnClient(client);
        }
    }

    public Map.Entry<DBCursor, MongoClient> findMany(String collection, DBObject query) {
        DBCursor dbCursor = null;
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);

            dbCursor = dbCollection.find(query);
        }
        return new AbstractMap.SimpleEntry<DBCursor, MongoClient>(dbCursor, client);
    }

    public Map.Entry<DBCursor, MongoClient> findMany(String collection) {
        DBCursor dbCursor = null;
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);

            dbCursor = dbCollection.find();
        }
        return new AbstractMap.SimpleEntry<DBCursor, MongoClient>(dbCursor, client);
    }

    public void insert(String collection, DBObject object) {
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);
            dbCollection.insert(object);
        }
    }

    public void updateDocument(String collection, DBObject query, DBObject document) {
        MongoClient client;
        try {
            client = getClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return;
        }
        if (client != null) {
            DBCollection dbCollection = getCollection(client, collection);

            dbCollection.update(query, document);

            returnClient(client);
        }
    }

}
