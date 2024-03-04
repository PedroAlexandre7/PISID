package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.*;

public class MySQLWriter {

    private MySQLWriter() {
    }

    public static void fillMazeRooms() {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call fillMazeRooms(?,?,?)}");
            cs.setInt(1, MySQLReceiver.experimentID);
            cs.setInt(2, ProjectUtils.INITIAL_MAZE_ROOM);
            cs.setTimestamp(3, new Timestamp(System.currentTimeMillis() - (long) MySQLReceiver.maxCurrentMessageDelaySec * 1000));
            cs.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public static void initExperimentSession() {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call initExperimentSession(?,?)}");
            cs.setInt(1, MySQLReceiver.experimentID);
            cs.setInt(2, MySQLReceiver.mongoSessionID);
            cs.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }

    }

    public static void insertFinalMeasurements() {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call insertOneFinalMeasurement(?,?,?)}");
            for (MazeRoom room : MySQLReceiver.expValidator.getMazeRooms()) {
                cs.setInt(1, MySQLReceiver.experimentID);
                cs.setInt(2, room.roomNumber);
                cs.setInt(3, room.getNumRats());
                cs.execute();
            }
        } catch (SQLException e) {
            if (e instanceof SQLIntegrityConstraintViolationException) {
                String alertText = "Não foi possível registar as medições da experiência pois já foi dada como concluída anteriormente, crie uma nova experiência.";
                generateAlert(AlertType.EXPERIMENT_ALREADY_CONCLUDED, alertText, null, null, null, null);
            } else
                throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public static void insertTemp(float temperature, int sensorID, Timestamp hour) {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call insertTemp(?,?,?,?)}");
            cs.setTimestamp(1, hour);
            cs.setFloat(2, temperature);
            cs.setInt(3, sensorID);
            cs.setInt(4, MySQLReceiver.experimentID);
            cs.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public static void insertMov(int roomFrom, int roomTo, Timestamp hour) {
        int numRatsFrom = MySQLReceiver.expValidator.getRatsInRoom(roomFrom);
        int numRatsTo = MySQLReceiver.expValidator.getRatsInRoom(roomTo);
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call registerMov(?,?,?,?,?,?)}");
            cs.setInt(1, roomFrom);
            cs.setInt(2, numRatsFrom);
            cs.setInt(3, roomTo);
            cs.setInt(4, numRatsTo);
            cs.setTimestamp(5, hour);
            cs.setInt(6, MySQLReceiver.experimentID);
            cs.execute();
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public static void generateAlert(AlertType alertType, String alertMessage, Integer numMazeRoom, Timestamp hour, Integer sensorID, Float temperature) {
        if (AlertType.shouldAlert(alertType)) {
            try {
                System.out.println(ProjectUtils.ANSI_YELLOW + alertType.getTypeString() + ": " + alertMessage + ProjectUtils.ANSI_RESET);
                CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call insertAlert(?,?,?,?,?,?,?)}");
                cs.setInt(1, MySQLReceiver.experimentID);
                cs.setString(2, alertType.getTypeString());
                cs.setString(3, alertMessage);
                if (numMazeRoom == null)
                    cs.setNull(4, Types.INTEGER);
                else
                    cs.setInt(4, numMazeRoom);
                if (hour == null)
                    cs.setNull(5, Types.TIMESTAMP);
                else
                    cs.setTimestamp(5, hour);
                if (sensorID == null)
                    cs.setNull(6, Types.INTEGER);
                else
                    cs.setInt(6, sensorID);
                if (temperature == null)
                    cs.setNull(7, Types.INTEGER);
                else
                    cs.setFloat(7, temperature);
                cs.execute();

            } catch (SQLException e) {
                throw new RuntimeException("Error executing SQL statement.\n" + e);
            }

        }
    }

}


