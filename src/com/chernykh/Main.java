package com.chernykh;

import com.chernykh.db.DBManager;
import com.chernykh.db.entity.Pipeline;

import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) throws SQLException {
        DBManager dbManager = DBManager.getInstance();

        dbManager.createTablePipelines();
        List<Pipeline> pipelines = IOUtils.readInputFile("input.csv");
        dbManager.insertAllPipelines(pipelines);
        IOUtils.createOutputFile(dbManager, dbManager.findAllPipelines(), "output.csv");
        dbManager.uploadAllFilesToDB();
    }

}
