package com.chernykh.db;

import com.chernykh.db.entity.Pipeline;
import org.h2.tools.Csv;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DBManager {

    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:mem:waterpipelines";

    static final String USER = "water";
    static final String PASS = "system";

    private static final String SQL_INSERT_PIPELINE =
            "INSERT INTO pipelines (startPointId, endPointId, length) VALUES (?, ?, ?)";

    private static final String SQL_FIND_PIPELINE_BY_COORDINATES =
            "SELECT * FROM pipelines WHERE startPointId = ? and endPointId = ?";

    private static final String SQL_FIND_PIPELINE_BY_ID =
            "SELECT * FROM pipelines WHERE id = ?";

    private static final String SQL_UPDATE_PIPELINE_BY_COORDINATES =
            "UPDATE pipelines SET startPointId = ?, endPointId = ?, length = ?" +
                    " WHERE startPointId = ? and endPointId = ?";

    private static final String SQL_UPDATE_PIPELINE_BY_ID =
            "UPDATE pipelines SET startPointId = ?, endPointId = ?, length = ?" +
                    " WHERE id = ?";

    private static final String SQL_FIND_ALL_PIPELINES =
            "SELECT * FROM pipelines ORDER BY id";

    private static final String SQL_FIND_ALL_PIPELINES_BY_START =
            "SELECT * FROM pipelines WHERE startPointId = ? ORDER BY id";

    private static final String SQL_CREATE_TABLE_PIPELINES = "CREATE TABLE PIPELINES " +
            "(id INT NOT NULL AUTO_INCREMENT, " +
            " startPointId INTEGER, " +
            " endPointId INTEGER, " +
            " length INTEGER, " +
            " PRIMARY KEY ( id ), " +
            " CHECK ( endPointId > startPointId )," +
            " UNIQUE (startPointId, endPointId))";

    private static final String SQL_CREATE_INPUT_TABLE_CSV =
            "CREATE TABLE INPUT AS SELECT * FROM CSVREAD('input.csv', null,'fieldSeparator=;');";
    private static final String SQL_CREATE_ROUTES_TABLE_CSV =
            "CREATE TABLE ROUTES AS SELECT * FROM CSVREAD('routes.csv', null,'fieldSeparator=;');";
    private static final String SQL_CREATE_OUTPUT_TABLE_CSV =
            "CREATE TABLE OUTPUT AS SELECT * FROM CSVREAD('output.csv', null,'fieldSeparator=;');";

    private static final int VALUE_INDEX = 1;

    private Connection con = getConnection();
    private static DBManager instance;

    private DBManager() {
    }

    public static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(DB_URL, USER, PASS);
        } catch (SQLException e) {
            System.out.println("JDBC exception");
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.out.println("Class.forName exception");
            e.printStackTrace();
        }
        return con;
    }

    public static synchronized DBManager getInstance() {
        if (instance == null) {
            instance = new DBManager();
        }
        return instance;
    }

    public boolean createTablePipelines() throws SQLException {
        return createTableSQL(SQL_CREATE_TABLE_PIPELINES);
    }

    private boolean createTableSQL(String sql) throws SQLException {
        Statement stmt = null;
        try {
            System.out.println("Creating table in given database...");
            stmt = con.createStatement();
            stmt.executeUpdate(sql);
            System.out.println("Created table in given database...");
            stmt.close();
            return true;
        } catch (SQLException e) {
            System.err.println("Error creating table");
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                DBUtils.close(stmt);
            }
        }
        return false;
    }

    public boolean insertAllPipelines(List<Pipeline> pipelines) {
        try {
            for (Pipeline temp :
                    pipelines) {
                insertPipeline(temp);
            }
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean insertPipeline(Pipeline pipeline) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = con.prepareStatement(SQL_INSERT_PIPELINE, Statement.RETURN_GENERATED_KEYS);
            pstmt.setInt(VALUE_INDEX, pipeline.getStartPointId());
            pstmt.setInt(VALUE_INDEX + 1, pipeline.getEndPointId());
            pstmt.setInt(VALUE_INDEX + 2, pipeline.getLength());

            if (pstmt.executeUpdate() > 0) {
                rs = pstmt.getGeneratedKeys();
                if (rs.next()) {
                    int id = rs.getInt(VALUE_INDEX);
                    pipeline.setId(id);
                }
                return true;
            }
        } catch (SQLException ex) {
            System.err.println("Cannot insert a new pipeline");
        } finally {
            if (rs != null) {
                DBUtils.close(rs);
            }
            if (pstmt != null) {
                DBUtils.close(pstmt);
            }
        }
        return false;
    }

    public Pipeline getPipeline(int idX, int idY) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = con.prepareStatement(SQL_FIND_PIPELINE_BY_COORDINATES);
            pstmt.setInt(VALUE_INDEX, idX);
            pstmt.setInt(VALUE_INDEX + 1, idY);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return extractPipeline(rs);
            }
        } finally {
            if (rs != null) {
                DBUtils.close(rs);
            }
            if (pstmt != null) {
                DBUtils.close(pstmt);
            }
        }
        return null;
    }

    public Pipeline getPipelineById(int id) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = con.prepareStatement(SQL_FIND_PIPELINE_BY_ID);
            pstmt.setInt(VALUE_INDEX, id);
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return extractPipeline(rs);
            }
        } finally {
            if (rs != null) {
                DBUtils.close(rs);
            }
            if (pstmt != null) {
                DBUtils.close(pstmt);
            }
        }
        return null;
    }

    public Pipeline updatePipeline(int idX, int idY, Pipeline newPipeline) throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement(SQL_UPDATE_PIPELINE_BY_COORDINATES);

            pstmt.setInt(VALUE_INDEX, newPipeline.getStartPointId());
            pstmt.setInt(VALUE_INDEX + 1, newPipeline.getEndPointId());
            pstmt.setInt(VALUE_INDEX + 2, newPipeline.getLength());
            pstmt.setInt(VALUE_INDEX + 3, idX);
            pstmt.setInt(VALUE_INDEX + 4, idY);

            pstmt.executeUpdate();
        } finally {
            if (pstmt != null) {
                DBUtils.close(pstmt);
            }
        }
        return null;
    }

    public Pipeline updatePipelineById(int id, Pipeline newPipeline) throws SQLException {
        PreparedStatement pstmt = null;
        try {
            pstmt = con.prepareStatement(SQL_UPDATE_PIPELINE_BY_ID);

            pstmt.setInt(VALUE_INDEX, newPipeline.getStartPointId());
            pstmt.setInt(VALUE_INDEX + 1, newPipeline.getEndPointId());
            pstmt.setInt(VALUE_INDEX + 2, newPipeline.getLength());
            pstmt.setInt(VALUE_INDEX + 3, id);

            pstmt.executeUpdate();
        } finally {
            if (pstmt != null) {
                DBUtils.close(pstmt);
            }
        }
        return null;
    }

    public List<Pipeline> findAllPipelines() throws SQLException {
        List<Pipeline> pipelines = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = con.createStatement();
            rs = stmt.executeQuery(SQL_FIND_ALL_PIPELINES);
            while (rs.next()) {
                pipelines.add(extractPipeline(rs));
            }
        } catch (SQLException ex) {
            System.err.println("Cannot find all pipelines");

            throw new SQLException("Cannot find all pipelines", ex);
        } finally {
            if (rs != null) {
                DBUtils.close(rs);
            }
            if (stmt != null) {
                DBUtils.close(stmt);
            }
        }
        return pipelines;
    }

    private Pipeline extractPipeline(ResultSet rs) throws SQLException {
        Pipeline pipeline = new Pipeline();
        pipeline.setId(rs.getInt("id"));
        pipeline.setStartPointId(rs.getInt("startPointId"));
        pipeline.setEndPointId(rs.getInt("endPointId"));
        pipeline.setLength(rs.getInt("length"));
        return pipeline;
    }

    public int getMinRouteLength(int idX, int idY) {
        if (idX == idY) {
            return -1;
        }
        if (idX > idY) {
            return getMinRouteLength(idY, idX);
        }
        try {
            int result = checkRoute(idX, idY);
            if (result > 0) {
                return result;
            }

            List<Pipeline> routePipelines = getPipelinesByStartId(idX);
            if (routePipelines.isEmpty()) {
                return -1;
            }

            for (Pipeline temp :
                    routePipelines) {
                int restOfTheRoute = getMinRouteLength(temp.getEndPointId(), idY);
                if (restOfTheRoute != -1) {
                    if (result == -1) {
                        result = temp.getLength() + restOfTheRoute;
                    } else {
                        result = Math.min(result, temp.getLength() + restOfTheRoute);
                    }
                }
            }

            return result;
        } catch (SQLException e) {
            System.err.println("Cannot calculate route");
            e.printStackTrace();
        }
        return -1;
    }

    private int checkRoute(int idX, int idY) throws SQLException {
        Pipeline temp = getPipeline(idX, idY);
        if (temp != null) {
            return temp.getLength();
        }
        return -1;
    }

    private List<Pipeline> getPipelinesByStartId(int idX) throws SQLException {
        return findAllPipelines()
                .stream()
                .filter(p -> p.getStartPointId() == idX)
                .collect(Collectors.toList());
    }

    public boolean uploadAllFilesToDB() {
        try {
            createTableSQL(SQL_CREATE_INPUT_TABLE_CSV);
            createTableSQL(SQL_CREATE_ROUTES_TABLE_CSV);
            createTableSQL(SQL_CREATE_OUTPUT_TABLE_CSV);
            addCSVElements("input.csv");
            addCSVElements("routes.csv");
            addCSVElements("output.csv");
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public void addCSVElements(String path) throws SQLException {
        Statement stmt = null;
        String sql = "INSERT INTO " + path.substring(0, path.length() - 4) + "(`";

        ResultSet rs = new Csv().read(path, null, null);
        ResultSetMetaData meta = rs.getMetaData();
        int noOfCols = meta.getColumnCount();
        for (int i = 0; i < noOfCols; i++) {
            System.out.print(meta.getColumnLabel(i + 1));
            sql += meta.getColumnLabel(i + 1).replaceAll(";", "`,`");
        }
        sql += "`) VALUES (";
        while (rs.next()) {
            System.out.println();
            String tempSql = sql;
            for (int i = 0; i < noOfCols; i++) {
                System.out.print(rs.getString(i + 1));
                tempSql += rs.getString(i + 1) + ")";
            }
            tempSql = tempSql.replaceAll(";", ",");
            stmt = con.createStatement();
            stmt.executeUpdate(tempSql);
        }
        System.out.println();
    }
}
