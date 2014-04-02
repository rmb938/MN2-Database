package com.rmb938.database;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
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

    public MongoClient getClient() throws UnknownHostException {
        return new MongoClient(address, port);
    }

    public void returnClient(MongoClient mongoClient) {
        mongoClient.close();
    }

    public DBCollection getCollection(MongoClient mongoClient, String collectionName) {
        DB database = getDatabase(mongoClient);
        return database.getCollection(collectionName);
    }

}
