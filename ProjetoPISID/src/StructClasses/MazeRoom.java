package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Types;

public class MazeRoom {
    public final int roomNumber;
    private Integer numRats;

    public MazeRoom(int number, int totalRats) {
        this.roomNumber = number;
        loadLastRoomRatCount(totalRats);

    }

    public Integer getNumRats() {
        return numRats;
    }

    private void loadLastRoomRatCount(int totalRats) {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call getLastNumRatsInRoom(?,?,?)}");
            cs.setInt(1, roomNumber);
            cs.setInt(2, MySQLReceiver.experimentID);
            cs.registerOutParameter(3, Types.INTEGER);
            cs.execute();
            int numRats = cs.getInt("num_ratos");
            if (cs.wasNull())
                if (roomNumber == ProjectUtils.INITIAL_MAZE_ROOM) {
                    setNumRats(totalRats);
                    MySQLWriter.fillMazeRooms();
                } else
                    setNumRats(0);
            else
                setNumRats(numRats);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    public int increment() {
        return ++numRats;
    }

    public int decrement() {
        return --numRats;
    }

    public void setNumRats(int numRats) {
        if (this.numRats != null)
            throw new IllegalStateException("Number of rats already initialized.");
        this.numRats = numRats;
    }

}
