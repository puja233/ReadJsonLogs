package com.cs.dao;

import com.cs.model.EventDetails;
import com.cs.util.DbConnection;
import com.cs.util.DbConnectionUtil;

import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

public class DatabaseQueriesDAO {
    static Logger logger = Logger.getLogger(String.valueOf(DatabaseQueriesDAO.class));
    public void createTable() throws SQLException {
        logger.info("In DatabaseQueriesDAO.createTable() : ");
        Statement stmt = null;
        Connection connection = DbConnection.getDatabaseConnectionInstance();
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
    }

    public void insertData(EventDetails eventDetails) throws SQLException {
        logger.info("In DatabaseQueriesDAO.insertData() : ");
        Connection connection = DbConnection.getDatabaseConnectionInstance();
        DatabaseQueriesDAO databaseQueriesDAO = new DatabaseQueriesDAO();
        String insertQuery = "insert into eventDetails (id,eventId,eventDuration,eventType,eventHost,alert) " +
                "values (? , ?,?,?,?,?)";
        PreparedStatement  preparedStatement = connection.prepareStatement(insertQuery);
        preparedStatement.setInt(1,databaseQueriesDAO.selectMaxRowCount()+1);
        preparedStatement.setString(2,eventDetails.getEventId());
        preparedStatement.setInt(3,eventDetails.getEventDuration());
        preparedStatement.setString(4,eventDetails.getEventType());
        preparedStatement.setString(5,eventDetails.getEventHost());
        preparedStatement.setBoolean(6,eventDetails.isAlert());
       int i =  preparedStatement.executeUpdate();

        connection.commit();
       logger.info("Insertted data");
    }

    public void selectData() throws SQLException {
        logger.info("In DatabaseQueriesDAO.selectData() : ");
        Connection connection = DbConnection.getDatabaseConnectionInstance();
        String selectQuery = "select * from eventDetails";
        PreparedStatement  preparedStatement = connection.prepareStatement(selectQuery);
        ResultSet rs =  preparedStatement.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String eventId = rs.getString("eventId");
            int eventDuration = rs.getInt("eventDuration");
            String eventType = rs.getString("eventType");
            String eventHost = rs.getString("eventHost");
            boolean alert = rs.getBoolean("alert");
            logger.info("*********************");
            logger.info("id: "+id+" eventId data :"+eventId+" eventDuration : "+eventDuration+" eventType : "+eventType+" eventHost : "+eventHost+" alert : "+alert);
            System.out.println("id: "+id+" eventId data :"+eventId+" eventDuration : "+eventDuration+" eventType : "+eventType+" eventHost : "+eventHost+" alert : "+alert);
        }
    }

    private int selectMaxRowCount() throws SQLException {
        logger.info("In DatabaseQueriesDAO.selectData() : ");
        Connection connection = DbConnection.getDatabaseConnectionInstance();
        String selectQuery = "select max(id) as countId from eventDetails";
        PreparedStatement  preparedStatement = connection.prepareStatement(selectQuery);
        ResultSet rs =  preparedStatement.executeQuery();
        int countId = 0;
        while (rs.next()) {
            countId = rs.getInt("countId");
            logger.info("countId data :"+countId);
        }
        logger.info("select data :");
        return countId;

    }

    public void deleteData(int id) throws SQLException {
        logger.info("In DatabaseQueriesDAO.insertData() : ");
        Connection connection = DbConnection.getDatabaseConnectionInstance();
        DatabaseQueriesDAO databaseQueriesDAO = new DatabaseQueriesDAO();
        String insertQuery = "delete from eventDetails where id = (?) " ;
        PreparedStatement  preparedStatement = connection.prepareStatement(insertQuery);
        preparedStatement.setInt(1,id);
        int i =  preparedStatement.executeUpdate();

        connection.commit();
        logger.info("Insertted data :"+i);
    }
}
