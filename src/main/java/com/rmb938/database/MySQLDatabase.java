package com.rmb938.database;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MySQLDatabase extends Database {

    private static Logger logger = Logger.getLogger(MySQLDatabase.class.getName());

    private final String userName;
    private final String password;
    private final String database;
    private final String address;
    private final int port;

    private BasicDataSource dataSource;

    public MySQLDatabase(String userName, String password, String database, String address, int port) {
        this.userName = userName;
        this.password = password;
        this.database = database;
        this.address = address;
        this.port = port;
        setupDatabase();
    }

    @Override
    public void setupDatabase() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUrl("jdbc:mysql://" + address + ":" + port + "/" + database);
        ds.setUsername(userName);
        ds.setPassword(password);
        ds.setMaxActive(100);
        ds.setMaxIdle(10);
        ds.setMinEvictableIdleTimeMillis(1800000);
        ds.setNumTestsPerEvictionRun(3);
        ds.setTimeBetweenEvictionRunsMillis(1800000);
        ds.setTestOnBorrow(false);
        ds.setTestWhileIdle(true);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void closeConnection(Connection connection) {
        DbUtils.closeQuietly(connection);
    }

    public boolean isTable(String tableName) {
        boolean exists = false;
        Connection connection = null;
        try {
            connection = getConnection();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        try {
            DatabaseMetaData meta = connection.getMetaData();
            ResultSet rs = meta.getTables(null, null, tableName, null);
            exists = rs.next();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            closeConnection(connection);
        }
        return exists;
    }

    public void createTable(String sql) {
        if (sql.startsWith("CREATE ") == false || sql.startsWith("create") == false) {
            logger.severe("Can only be used to create tables!");
            return;
        }
        Connection connection = null;
        try {
            connection = getConnection();
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        } finally {
            closeConnection(connection);
        }
    }

    public ArrayList<Object> getBeansInfo(String sql, ResultSetHandler resultSetHandler) {
        ArrayList<Object> beansInfo = new ArrayList<>();
        try {
            QueryRunner run = new QueryRunner(dataSource);
            List beans = (List) run.query(sql, resultSetHandler);
            for (int i = 0; i < beans.size(); i++) {
                beansInfo.add(beans.get(i));
            }
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return beansInfo;
    }

    public void updateQueryPS(String s, Object... params) {
        try {
            QueryRunner run = new QueryRunner(dataSource);
            run.update(s, params);
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

    }

    public void updateQuery(String s) {
        try {
            QueryRunner run = new QueryRunner(dataSource);
            if (s.startsWith("SELECT") == false || s.startsWith("select") == false) {
                run.update(s);
            } else {
                logger.severe("UpdateQuery can not be used to select!");
            }
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }

}
