package com.cs;

import com.cs.dao.DatabaseQueriesDAO;
import com.cs.model.EventDetails;
import com.cs.util.DbConnection;
import com.cs.util.DbConnectionUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TestFileRead {
    static Logger logger = Logger.getLogger(String.valueOf(TestFileRead.class));

   /* public static void main(String[] args) {
        DatabaseQueriesDAO databaseQueriesDAO = new DatabaseQueriesDAO();
        try {
            ObjectMapper mapper = new ObjectMapper();
            URL fontURL = TestFileRead.class.getResource("/test1.txt");
            File file = new File(fontURL.getFile());

            ArrayList<JsonNode> listOfJson = TestFileRead.readJSON(file,"UTF-8");
            System.out.println("listOfJson========>"+listOfJson);
            logger.info("******************** logger implemented *******************");
            logger.info("1========>"+DbConnection.getDatabaseConnectionInstance());
            logger.info("2========>"+DbConnection.getDatabaseConnectionInstance());
            //DbConnectionUtil.getDbConnection();
            //databaseQueriesDAO.createTable();
            *//*EventDetails eventDetails = new EventDetails();
            eventDetails.setEventId("test2");
            eventDetails.setEventDuration(3);
            eventDetails.setEventHost(null);
            eventDetails.setEventType(null);
            eventDetails.setAlert(false);
            databaseQueriesDAO.insertData(eventDetails);
            databaseQueriesDAO.selectData();*//*
            //databaseQueriesDAO.selectMaxRowCount();
            getTimestamps(listOfJson);

        }catch (Exception e){
            e.printStackTrace();
        }


    }*/

    private static void getTimestamps(ArrayList<JsonNode> listOfJson) {

        /*listOfJson.fiorEach(it-> {
            String strtdId = it.get("id").textValue();
            HashMap<String,>

        };*/
        Map<Object, List<JsonNode>> mapGroupBy = listOfJson.stream().collect(Collectors.groupingBy(s->s.get("id").textValue()));

        for (Object key : mapGroupBy.keySet()) {
            AtomicReference<String> startedTS = new AtomicReference<>();
            AtomicReference<String> finishedTS = new AtomicReference<>();
            long diffTS = 0L;
            mapGroupBy.get(key).stream().forEach(
                    l -> {
                        String stateStarted = l.get("state").textValue();
                        System.out.println("==============================================");
                        System.out.println("id======>" + l.get("id").textValue());
                        if (stateStarted.equals("STARTED")) {
                            startedTS.set(l.get("timestamp").textValue());
                            System.out.println("stateStarted ===>" + stateStarted);

                        } else if (stateStarted.equals("FINISHED")) {
                            finishedTS.set(l.get("timestamp").textValue());
                            System.out.println("stateStopped ===>" + stateStarted);

                        }
                    });
            System.out.println("startedTS ===>" + startedTS);
            System.out.println("finishedTS ===>" + finishedTS);
            if(null != startedTS && null != startedTS) {
                diffTS = Long.parseLong(String.valueOf(finishedTS)) - Long.parseLong(String.valueOf(startedTS));
                System.out.println("Time diff for id : " + key.toString() + " is : " + diffTS);
            }
        }


        /*mapGroupBy.keySet().forEach(key-> mapGroupBy.get(key).stream().forEach(
                l -> {
                        String stateStarted = l.get("state").textValue();
                        String startedTS=null;
                        String finishedTS=null;
                    System.out.println("==============================================");
                    System.out.println("id======>"+l.get("id").textValue());
                        if(stateStarted.equals("STARTED")) {
                            startedTS = l.get("timestamp").textValue();
                            System.out.println("stateStarted ===>" + stateStarted);
                            System.out.println("startedTS ===>" + startedTS);
                        }else if(stateStarted.equals("FINISHED")){
                            finishedTS = l.get("timestamp").textValue();
                            System.out.println("stateStopped ===>" + stateStarted);
                            System.out.println("finishedTS ===>" + finishedTS);
                        }
                }
        ));*/
        System.out.println("group by============>"+mapGroupBy.keySet());
    }

    public static synchronized ArrayList<JsonNode> readJSON(File MyFile, String Encoding) throws IOException, org.json.simple.parser.ParseException {
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
