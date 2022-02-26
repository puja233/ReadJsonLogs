package com.cs;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class TestModel {
    List<JsonNode> listOfJsons;

    public List<JsonNode> getListOfJsons() {
        return listOfJsons;
    }

    public void setListOfJsons(List<JsonNode> listOfJsons) {
        this.listOfJsons = listOfJsons;
    }



}
