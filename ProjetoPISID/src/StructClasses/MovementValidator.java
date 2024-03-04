package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class MovementValidator {
    private final List<Corridor> corridors;
    private final List<MazeRoom> mazeRooms;
    private final int maxRatsInRoom;
    private Timestamp lastValidMovTime;

    public MovementValidator(List<Corridor> corridors, int numRooms, int totalRats, int maxRatsInRoom) {
        this.corridors = corridors;
        this.maxRatsInRoom = maxRatsInRoom;
        mazeRooms = new ArrayList<>(numRooms);
        for (int i = 1; i <= numRooms; i++)
            mazeRooms.add(new MazeRoom(i, totalRats));
        loadLastValidMovTime();
    }

    public Timestamp getLastValidMovTime() {
        return lastValidMovTime;
    }

    public int getRatsInRoom(int roomNumber) {
        return mazeRooms.get(roomNumber - 1).getNumRats();
    }

    public List<MazeRoom> getMazeRooms() {
        return mazeRooms;
    }

    private void loadLastValidMovTime() {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call getLastValidMovMeasure(?,?)}");
            cs.setInt(1, MySQLReceiver.experimentID);
            cs.registerOutParameter(2, Types.TIMESTAMP);
            cs.execute();
            Timestamp timestamp = cs.getTimestamp("horas");
            if (cs.wasNull())
                return;
            if (ProjectUtils.secondsBetween(timestamp, new Timestamp(System.currentTimeMillis())) > MySQLReceiver.maxDisconnectedMessageDelaySec)
                MySQLWriter.generateAlert(AlertType.EXPERIMENT_ALREADY_CONCLUDED, "Por já ter passado mais que " + MySQLReceiver.maxDisconnectedMessageDelaySec + " segundos desde a última vez que se correu esta esperiência, foi dada como concluída, crie uma nova experiência.", null, null, null, null);
            lastValidMovTime = timestamp;
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public ValidationStatus validateMov(int from, int to, Timestamp hour) {
        if (!corridors.contains(new Corridor(from, to)))
            return ValidationStatus.NONEXISTENT_CORRIDOR;
        if (mazeRooms.get(from - 1).decrement() < 0) {
            if (ProjectUtils.IGNORE_NEGATIVE_RATS)
                lastValidMovTime = hour;
            mazeRooms.get(to - 1).increment();
            return ValidationStatus.NEGATIVE_RATS;
        }
        lastValidMovTime = hour;
        if (mazeRooms.get(to - 1).increment() >= maxRatsInRoom)
            return ValidationStatus.OVERCROWDED_ROOM;
        return ValidationStatus.OK;

    }

}
