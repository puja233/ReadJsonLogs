package com.cs.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

/**
 * This class creates database connection
 */
public class DbConnection implements Cloneable{
    static Logger logger = Logger.getLogger(String.valueOf(DbConnection.class));
    private static Connection connection;
    private static String url = "jdbc:hsqldb:file:W:\\Projects\\IntellijProject\\ReadJsonLogs\\src\\main\\java\\com\\cs\\db/-dbname.0 EventDb";
    private static String username = "SA";
    private static String password = "";

    private DbConnection() {

    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    /**
     * This method instantiates database connection
     *
     * @return Connection instance
     */
    private Connection instantiateConnection(){
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            connection = DriverManager.getConnection(url, username, password);
            logger.info("Connection Created");
        } catch (ClassNotFoundException | SQLException ex) {
            logger.severe("Error occurred while connecting to database : " + ex.getMessage());
            ex.printStackTrace();
        }
        return this.connection;
    }

    /**
     * This method returns database connection object instance
     *
     * @return connection object
     * @throws SQLException
     */
    static synchronized public Connection getDatabaseConnectionInstance() throws SQLException {
        if (connection == null) {
            connection = new DbConnection().instantiateConnection();
        } else if (connection.isClosed()) {
            connection = new DbConnection().instantiateConnection();
        }
        logger.info("Connection Instance : "+connection);
        return connection;
    }

    /**
     * This method closes database connection
     */
    public static void closeDbConnection(){
        try {
            connection.close();
            logger.info("DB Connection closed");
        } catch (SQLException e) {
            e.printStackTrace();
            logger.severe("Error occurred while closing Db :"+e.getMessage());
        }
    }
}
