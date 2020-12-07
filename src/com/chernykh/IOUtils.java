package com.chernykh;

import com.chernykh.db.DBManager;
import com.chernykh.db.entity.Pipeline;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IOUtils {

    public static boolean createInputFile(List<Pipeline> pipelines, String path) {
        try {
            FileWriter csvWriter = new FileWriter(path);
            csvWriter.append("idX");
            csvWriter.append(";");
            csvWriter.append("idY");
            csvWriter.append(";");
            csvWriter.append("LENGTH");
            csvWriter.append("\n");

            for (Pipeline temp : pipelines) {
                csvWriter.append(temp.toString());
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to write to file");
            e.printStackTrace();
        }
        return false;
    }

    public static boolean createRouteFile(DBManager dbManager, List<Pipeline> pipelines, String path) {
        try {
            FileWriter csvWriter = new FileWriter(path);
            csvWriter.append("ROUTE EXISTS");
            csvWriter.append(";");
            csvWriter.append("MIN LENGTH");
            csvWriter.append("\n");

            ArrayList<int[]> routes = IOUtils.readRouteFile("routes.csv");

            for (int[] route :
                    routes) {
                int routeLength = dbManager.getMinRouteLength(route[0], route[1]);
                if(routeLength == -1) {
                    csvWriter.append("FALSE;");
                } else {
                    csvWriter.append("TRUE;");
                    csvWriter.append(Integer.toString(routeLength));
                }
                csvWriter.append("\n");
            }

            csvWriter.flush();
            csvWriter.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed to write to file");
            e.printStackTrace();
        }
        return false;
    }

    public static List<Pipeline> readInputFile(String path) {
        try {
            File csvFile = new File(path);
            if (csvFile.isFile()) {
                BufferedReader csvReader = new BufferedReader(new FileReader(csvFile));
                String row;
                csvReader.readLine();
                List<Pipeline> pipelines = new ArrayList<>();
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(";");
                    pipelines.add(new Pipeline(Integer.parseInt(data[0]),
                            Integer.parseInt(data[1]),
                            Integer.parseInt(data[2])));
                }
                csvReader.close();
                return pipelines;
            }
        } catch (IOException e) {
            System.err.println("Failed to read from file");
            e.printStackTrace();
        }
        return null;
    }

    public static ArrayList<int[]> readRouteFile(String path) {
        try {
            File csvFile = new File(path);
            if (csvFile.isFile()) {
                BufferedReader csvReader = new BufferedReader(new FileReader(csvFile));
                String row;
                csvReader.readLine();
                ArrayList<int[]> routes = new ArrayList<>();
                while ((row = csvReader.readLine()) != null) {
                    String[] data = row.split(";");
                    int[] tempArr = new int[2];
                    tempArr[0] = Integer.parseInt(data[0]);
                    tempArr[1] = Integer.parseInt(data[1]);
                    routes.add(tempArr);
                }
                csvReader.close();
                return routes;
            }
        } catch (IOException e) {
            System.err.println("Failed to read from file");
            e.printStackTrace();
        }
        return null;
    }
}
