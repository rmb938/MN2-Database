package com.rmb938.database;

public class DatabaseAPI {

    private static MongoDatabase mongoDatabase;
    private static MySQLDatabase mySQLDatabase;

    public static void initializeMongo(String database, String address, int port) {
        mongoDatabase = new MongoDatabase(database, address, port);
    }

    public static void initializeMySQL(String userName, String password, String database, String address, int port) {
        mySQLDatabase = new MySQLDatabase(userName, password, database, address, port);
    }

    public static MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    public static MySQLDatabase getMySQLDatabase() {
        return mySQLDatabase;
    }
}
