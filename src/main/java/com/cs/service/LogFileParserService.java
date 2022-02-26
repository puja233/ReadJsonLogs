package com.cs.service;

import com.cs.TestFileRead;
import com.cs.dao.DatabaseQueriesDAO;
import com.cs.model.EventDetails;
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

public class LogFileParserService {
    static Logger logger = Logger.getLogger(String.valueOf(LogFileParserService.class));
    DatabaseQueriesDAO databaseQueriesDAO = new DatabaseQueriesDAO();
    public synchronized String parseLogFile(String inputFilePath){
        String result = null;

            try {
                ObjectMapper mapper = new ObjectMapper();
                URL fontURL = TestFileRead.class.getResource("/test1.txt");
                File file = new File(fontURL.getFile());

                ArrayList<JsonNode> listOfJson = TestFileRead.readJSON(file,"UTF-8");
                System.out.println("listOfJson========>"+listOfJson);
               logger.info("******************** logger implemented *******************");

                getTimestamps(listOfJson);
                //databaseQueriesDAO.deleteData(4);
                //databaseQueriesDAO.selectData();
                //DbConnection.closeDbConnection();
            }catch (Exception e){

        }finally {

        }
        return result;
    }

    private void getTimestamps(ArrayList<JsonNode> listOfJson) throws SQLException {
        Map<Object, List<JsonNode>> mapGroupBy = listOfJson.stream().collect(Collectors.groupingBy(s->s.get("id").textValue()));

        for (Object key : mapGroupBy.keySet()) {
            AtomicReference<String> startedTS = new AtomicReference<>();
            AtomicReference<String> finishedTS = new AtomicReference<>();
            EventDetails eventDetails = new EventDetails();
            long diffTS = 0L;
            mapGroupBy.get(key).stream().forEach(
                    l -> {
                        String stateStarted = l.get("state").textValue();
                        if(null != l.get("type")) {
                            eventDetails.setEventType(l.get("type").textValue());
                        }if(null != l.get("host")) {
                            eventDetails.setEventHost(l.get("host").textValue());
                        }
                        logger.info("==============================================");
                        logger.info("id : " + l.get("id").textValue());
                        if (stateStarted.equals("STARTED")) {
                            startedTS.set(l.get("timestamp").textValue());
                            System.out.println("stateStarted ===>" + stateStarted);

                        } else if (stateStarted.equals("FINISHED")) {
                            finishedTS.set(l.get("timestamp").textValue());
                            System.out.println("stateStopped ===>" + stateStarted);

                        }
                    });
            logger.info("startedTS : " + startedTS);
            logger.info("finishedTS : " + finishedTS);
            if(null != startedTS && null != startedTS) {
                diffTS = Long.parseLong(String.valueOf(finishedTS)) - Long.parseLong(String.valueOf(startedTS));
                eventDetails.setEventDuration((int) diffTS);
                eventDetails.setEventId(key.toString());
                if(diffTS > 4){
                    eventDetails.setAlert(true);
                }else{
                    eventDetails.setAlert(false);
                }
                databaseQueriesDAO.insertData(eventDetails);
                logger.info("Time diff for id : " + key.toString() + " is : " + diffTS);
            }
        }
    }

    public synchronized ArrayList<JsonNode> readJSON(File MyFile, String Encoding) throws IOException, org.json.simple.parser.ParseException {
        Scanner scn=new Scanner(MyFile,Encoding);
        ArrayList<JsonNode> json=new ArrayList<JsonNode>();
        ObjectMapper mapper = new ObjectMapper();
        //Reading and Parsing Strings to Json
        while(scn.hasNext()){
            JSONObject obj= (JSONObject) new JSONParser().parse(scn.nextLine());
            json.add(mapper.readTree(obj.toJSONString()));
        }
        return json;
    }
}
