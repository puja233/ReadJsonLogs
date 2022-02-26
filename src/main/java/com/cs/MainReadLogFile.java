package com.cs;

import com.cs.service.LogFileParserService;

public class MainReadLogFile {

    public static void main(String[] args) {
        LogFileParserService logFileParserService = new LogFileParserService();
        logFileParserService.parseLogFile("/test1.txt");
    }
}
