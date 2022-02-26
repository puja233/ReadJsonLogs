package com.cs.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LogFileParserServiceTest {

    @InjectMocks
    private LogFileParserService logFileParserService;

    @Test
    public void test1(){
        System.out.println("============>"+logFileParserService.parseLogFile("/test1.txt"));
    }
}
