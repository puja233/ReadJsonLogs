package com.cs.util;

import java.sql.*;
import java.util.logging.Logger;

public class DbConnectionUtil {
    static Logger logger = Logger.getLogger(String.valueOf(DbConnectionUtil.class));
    public static Connection getDbConnection() {
        Connection connection = null;
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            //connection = DriverManager.getConnection("jdbc:hsqldb:file:E:\\Eclipse Projects\\HSQLDB\\db/-dbname.0 Try", "SA", "");
            connection = DriverManager.getConnection("jdbc:hsqldb:file:W:\\Projects\\IntellijProject\\ReadJsonLogs\\src\\main\\java\\com\\cs\\db/-dbname.0 EventDb", "SA", "");
            logger.info("Connection Created");
            /*Statement stmt = null;
            stmt = connection.createStatement();*/


            /*logger.info("In DatabaseQueriesDAO.createTable() : ");
            Statement stmt = null;
            stmt = connection.createStatement();
            String c="create table eventDetails(" +
                    "id int, " +
                    "eventId varchar(20), " +
                    "eventDuration bigint, " +
                    "eventType varchar(20), " +
                    "eventHost varchar(20), " +
                    "alert boolean)" ;
            boolean s=stmt.execute(c);
            connection.commit();
            // connection.close();
            logger.info("Table created : "+s);


            logger.info("In DatabaseQueriesDAO.insertData() : ");
           // Connection connection = DbConnectionUtil.getDbConnection();
            String insertQuery = "insert into eventDetails values (1 , 'test1',5,'Type','Host',true)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);
            int i =  preparedStatement.executeUpdate();

            connection.commit();
            logger.info("Insertted data :"+i);


            logger.info("In DatabaseQueriesDAO.selectData() : ");
            //Connection connection = DbConnectionUtil.getDbConnection();
            String selectQuery = "select * from eventDetails";
            PreparedStatement  preparedStatement1 = connection.prepareStatement(selectQuery);
            ResultSet rs =  preparedStatement1.executeQuery();
            while (rs.next()) {
                int id = rs.getInt("id");
                logger.info("id data :"+id);
                String eventId = rs.getString("eventId");
                logger.info("eventId data :"+eventId);
            }

            logger.info("select data :");*/
        } catch (Exception e) {
            System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
            e.printStackTrace();

        }
        return connection;
    }
}
