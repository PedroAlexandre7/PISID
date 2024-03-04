package StructClasses;

import javax.swing.*;
import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CloudToMySQL {

    public static Connection connTo;
    public static String cloud_database_connection = "";
    public static String cloud_database_password = "";
    public static String cloud_database_user = "";

    private CloudToMySQL() {
    }

    public static void setup() throws SQLException {
        loadIni();
        connTo = DriverManager.getConnection(cloud_database_connection, cloud_database_password, cloud_database_user);
    }

    private static void loadIni() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("ProjetoJar/data.ini"));
            cloud_database_connection = p.getProperty("cloud_database_connection");
            cloud_database_password = p.getProperty("cloud_database_password");
            cloud_database_user = p.getProperty("cloud_database_user");
        } catch (Exception e) {
            System.err.println("Error reading data.ini file " + e);
            JOptionPane.showMessageDialog(null, "The data.ini file wasn't found.", "Ini file", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static List<Corridor> getCorridors() {
        if (connTo == null)
            throw new IllegalStateException("Cloud MySQL Server yet not connected.");
        try {
            List<Corridor> corridors = new ArrayList<>();
            Statement statement = connTo.createStatement();
            ResultSet rs = statement.executeQuery("SELECT salaentrada, salasaida FROM corredor");
            while (rs.next())
                corridors.add(new Corridor(rs.getInt("salaentrada"), rs.getInt("salasaida")));
            return corridors;

        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }

    }

    public static float getProgrammedTemp() {
        if (connTo == null)
            throw new IllegalStateException("Cloud MySQL yet not connected.");
        try {
            Statement statement = connTo.createStatement();
            ResultSet rs = statement.executeQuery("SELECT temperaturaprogramada FROM configuraçãolabirinto LIMIT 1");
            rs.next();
            return rs.getFloat("temperaturaprogramada");
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }

    }

    public static int getNumRooms() {
        if (connTo == null)
            throw new IllegalStateException("Cloud MySQL yet not connected.");
        try {
            Statement statement = connTo.createStatement();
            ResultSet rs = statement.executeQuery("SELECT numerosalas FROM configuraçãolabirinto LIMIT 1");
            rs.next();
            return rs.getInt("numerosalas");
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }

    }
}