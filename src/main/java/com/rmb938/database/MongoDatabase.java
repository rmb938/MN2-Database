package com.rmb938.database;

import com.mongodb.*;

import java.net.UnknownHostException;
import java.util.logging.Logger;

public class MongoDatabase extends Database {

    private static Logger logger = Logger.getLogger(MongoDatabase.class.getName());

    private final String database;
    private final String address;
    private final int port;
    private MongoClient mongoClient;
    private DB db;

    public MongoDatabase(String database, String address, int port) {
        this.database = database;
        this.address = address;
        this.port = port;
        setupDatabase();
    }

    @Override
    public void setupDatabase() {
        try {
            mongoClient = new MongoClient(address, port);
            db = mongoClient.getDB(database);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private DB getDatabase() {
        db.requestEnsureConnection();
        return db;
    }

    private MongoClient getClient() throws UnknownHostException {
        return mongoClient;
    }

    private DBCollection getCollection(String collectionName) {
        DB database = getDatabase();
        return database.getCollection(collectionName);
    }

    public boolean collectionExists(String collectionName) {
        DB database = getDatabase();
        return database.collectionExists(collectionName);
    }

    public void createCollection(String collectionName) {
        DB database = getDatabase();
        database.createCollection(collectionName, new BasicDBObject("capped", false));
    }

    public DBObject findOne(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        return dbCollection.findOne(query);
    }

    public void remove(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.remove(query);
    }

    public DBCursor findMany(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        return dbCollection.find(query);
    }

    public DBCursor findMany(String collection) {
        DBCollection dbCollection = getCollection(collection);
        return dbCollection.find();
    }

    public void insert(String collection, DBObject object) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.insert(object);
    }

    public void delete(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.remove(query);
    }

    public void updateDocument(String collection, DBObject query, DBObject document) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.update(query, document);
    }

}
