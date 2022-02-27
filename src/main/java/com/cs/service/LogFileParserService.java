package com.cs.service;

import com.cs.dao.DatabaseQueriesDAO;
import com.cs.model.EventDetails;
import com.cs.util.Constants;
import com.cs.util.DbConnection;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This is service class for parsing input log json file
 */
public class LogFileParserService {
    static Logger logger = Logger.getLogger(String.valueOf(LogFileParserService.class));
    DatabaseQueriesDAO databaseQueriesDAO = new DatabaseQueriesDAO();

    /**
     * This method reads the file from the path and parses it.
     *
     * @param inputFilePath String input file path
     * @return result string
     */
    public synchronized String parseLogFile(String inputFilePath){
        String result;
            try {
                URL filePath = LogFileParserService.class.getResource(inputFilePath);
                File file = new File(filePath.getFile());

                ArrayList<JsonNode> listOfJson = readJSON(file,"UTF-8");
                logger.info("******************** logger implemented *******************");

                getTimestamps(listOfJson);
                databaseQueriesDAO.selectData();
                result = "Successfully Parsed Log File and updated Event Details in Database";
            }catch (Exception e){
                logger.severe("Error Occurred while parsing log file : "+e.getMessage());
                result = "Error Occurred while parsing log file";
        }finally {
                logger.info("Closing DB Connection");
                DbConnection.closeDbConnection();
        }
        return result;
    }

    /**
     * This method calculates the difference between the timestamps for each id
     *
     * @param listOfJson objects read from the file
     * @throws SQLException
     */
    private void getTimestamps(ArrayList<JsonNode> listOfJson) throws SQLException {
        Map<Object, List<JsonNode>> mapGroupBy = listOfJson.stream().collect(Collectors.groupingBy(s->s.get("id").textValue()));

        for (Object key : mapGroupBy.keySet()) {
            AtomicReference<String> startedTS = new AtomicReference<>();
            AtomicReference<String> finishedTS = new AtomicReference<>();
            EventDetails eventDetails = new EventDetails();
            long diffTS;
            mapGroupBy.get(key).stream().forEach(
                    l -> {
                        String stateStarted = l.get(Constants.STATE).textValue();
                        if(null != l.get(Constants.TYPE)) {
                            eventDetails.setEventType(l.get(Constants.TYPE).textValue());
                        }if(null != l.get(Constants.HOST)) {
                            eventDetails.setEventHost(l.get(Constants.HOST).textValue());
                        }
                        logger.info("==============================================");
                        logger.info("id : " + l.get(Constants.ID).textValue());
                        if (stateStarted.equals(Constants.STARTED)) {
                            startedTS.set(l.get(Constants.TIMESTAMP).textValue());
                        } else if (stateStarted.equals("FINISHED")) {
                            finishedTS.set(l.get(Constants.TIMESTAMP).textValue());
                        }
                    });
            logger.info("startedTS : " + startedTS);
            logger.info("finishedTS : " + finishedTS);
            if(null != startedTS && null != startedTS) {
                diffTS = Long.parseLong(String.valueOf(finishedTS)) - Long.parseLong(String.valueOf(startedTS));
                eventDetails.setEventDuration((int) diffTS);
                eventDetails.setEventId(key.toString());
                eventDetails.setAlert(diffTS > 4);
                databaseQueriesDAO.insertData(eventDetails);
                logger.info("Time diff for id : " + key + " is : " + diffTS);
            }
        }
    }

    /**
     * This method reads the file into array line by line
     *
     * @param MyFile File object to be parsed
     * @param Encoding utf-8 by default
     * @return Arraylist of JsonNodes
     * @throws IOException
     * @throws org.json.simple.parser.ParseException
     */
    public synchronized ArrayList<JsonNode> readJSON(File MyFile, String Encoding) throws IOException, org.json.simple.parser.ParseException {
        Scanner scn=new Scanner(MyFile,Encoding);
        ArrayList<JsonNode> json= new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        //Reading and Parsing Strings to Json
        while(scn.hasNext()){
            JSONObject obj= (JSONObject) new JSONParser().parse(scn.nextLine());
            json.add(mapper.readTree(obj.toJSONString()));
        }
        return json;
    }
}
