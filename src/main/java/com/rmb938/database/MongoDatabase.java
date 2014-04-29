package com.rmb938.database;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.rmb938.jedis.JedisManager;
import org.json.JSONException;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.net.UnknownHostException;
import java.util.Set;
import java.util.logging.Logger;

public class MongoDatabase extends Database {

    private static Logger logger = Logger.getLogger(MongoDatabase.class.getName());

    private final String database;
    private final String address;
    private final int port;
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
            MongoClient mongoClient = new MongoClient(address, port);
            db = mongoClient.getDB(database);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private DB getDatabase() {
        db.requestEnsureConnection();
        return db;
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
        Jedis jedis = JedisManager.getJedis();

        Set<String> expiredKeys = jedis.zrangeByScore("expire", 0, System.currentTimeMillis());
        if (expiredKeys.isEmpty() == false) {
            String[] keyArray = new String[expiredKeys.size()];
            expiredKeys.toArray(keyArray);
            jedis.del(keyArray);
            jedis.zrem("expire", keyArray);
        }

        JSONObject jsonObject = null;
        if (jedis.hexists(collection, query.toString())) {
            String data = jedis.hget(collection, query.toString());
            if (data != null) {
                try {
                    jsonObject = new JSONObject(data);
                } catch (JSONException e) {
                    e.printStackTrace();
                }
            }
        }
        DBObject dbObject;
        if (jsonObject == null) {
            DBCollection dbCollection = getCollection(collection);
            DBCursor dbCursor = dbCollection.find(query).limit(1);
            if (dbCursor.hasNext() == false) {
                return null;
            }
            dbObject = dbCursor.next();
            dbCursor.close();

            jedis.hset(collection, query.toString(), dbObject.toString());
            jedis.zadd("expire", System.currentTimeMillis() + 300000, collection + "_" + query);
        } else {
            dbObject = (DBObject) JSON.parse(jsonObject.toString());
            jedis.hset(collection, query.toString(), dbObject.toString());
            jedis.zadd("expire", System.currentTimeMillis() + 300000, collection + "_" + query);
        }

        JedisManager.returnJedis(jedis);
        return dbObject;
    }

    public DBObject findOneNoCache(String collection, DBObject query) {
        DBObject dbObject;
        DBCollection dbCollection = getCollection(collection);
        DBCursor dbCursor = dbCollection.find(query).limit(1);
        if (dbCursor.hasNext() == false) {
            return null;
        }
        dbObject = dbCursor.next();
        dbCursor.close();

        return dbObject;
    }

    public void remove(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.remove(query);
        Jedis jedis = JedisManager.getJedis();
        jedis.zrem("expire", query.toString());
        jedis.hdel(collection, query.toString());
        JedisManager.returnJedis(jedis);
    }

    /*public DBCursor findMany(String collection, DBObject query) {
        DBCollection dbCollection = getCollection(collection);
        return dbCollection.find(query);
    }*/

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
        Jedis jedis = JedisManager.getJedis();
        if (jedis.hexists(collection, query.toString())) {
            jedis.hdel(collection, query.toString(), findOneNoCache(collection, query).toString());
            jedis.zrem("expire", collection + "_" + query);
        }
        JedisManager.returnJedis(jedis);
    }

    public void updateDocument(String collection, DBObject query, DBObject document) {
        DBCollection dbCollection = getCollection(collection);
        dbCollection.update(query, document);
        Jedis jedis = JedisManager.getJedis();
        if (jedis.hexists(collection, query.toString())) {
            jedis.hset(collection, query.toString(), findOneNoCache(collection, query).toString());
            jedis.zadd("expire", System.currentTimeMillis() + 300000, collection + "_" + query);
        }
        JedisManager.returnJedis(jedis);
    }

}
