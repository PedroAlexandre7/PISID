package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class ExperimentValidator {

    private static final int CORRECTION_TEMP_MILLISECONDS = 1000;
    private static final int CORRECTION_MOV_MILLISECONDS = 1;
    public final List<Float> lowTempThreshholds;
    public final List<Float> highTempThreshholds;
    public final float programmedTemp;
    public final float maxTempVariance;
    public final int maxConsecutiveTempErrors;
    private final List<TemperatureSensor> temperatureSensors;
    private final MovementValidator movementValidator;

    public ExperimentValidator(List<Float> lowTempThreshholds, List<Float> highTempThreshholds, float programmedTemp, float maxTempVariance, int maxConsecutiveTempErrors, List<Corridor> corridors, int numRooms, int totalRats, int maxRatsInRoom) {
        temperatureSensors = new LinkedList<>();
        this.lowTempThreshholds = lowTempThreshholds;
        this.highTempThreshholds = highTempThreshholds;
        this.programmedTemp = programmedTemp;
        this.maxTempVariance = maxTempVariance;
        this.maxConsecutiveTempErrors = maxConsecutiveTempErrors;
        movementValidator = new MovementValidator(corridors, numRooms, totalRats, maxRatsInRoom);
    }

    public Timestamp getLastValidMovTime() {
        return movementValidator.getLastValidMovTime();
    }

    public int getRatsInRoom(int roomNumber) {
        return movementValidator.getRatsInRoom(roomNumber);
    }

    public List<MazeRoom> getMazeRooms() {
        return movementValidator.getMazeRooms();

    }

    private TemperatureSensor getTemperatureSensor(int sensorID) {
        if (sensorID < 1)
            return null;
        if (temperatureSensors.size() <= sensorID)
            temperatureSensors.add(new TemperatureSensor(sensorID, this));
        for(TemperatureSensor temperatureSensor : temperatureSensors)
            if (temperatureSensor.sensorID == sensorID)
                return temperatureSensor;
        throw new RuntimeException("Erro inesperado.");
    }

    public Timestamp processTempHour(int sensorID, Timestamp messageHour, Timestamp mongoSenderHour) {
        TemperatureSensor temperatureSensor = getTemperatureSensor(sensorID);
        return temperatureSensor == null ? null : processHour(messageHour, mongoSenderHour, temperatureSensor.getLastValidTempTime(), CORRECTION_TEMP_MILLISECONDS);
    }

    public Timestamp processMovHour(Timestamp messageHour, Timestamp mongoSenderHour) {
        return processHour(messageHour, mongoSenderHour, movementValidator.getLastValidMovTime(), CORRECTION_MOV_MILLISECONDS);
    }

    // Devolve a hora e ser colocada na BD se posteriormente se verificar que o registo é válido
    public Timestamp processHour(Timestamp messageHour, Timestamp mongoSenderHour, Timestamp lastValidTime, int correctionIncrement) {
        if (lastValidTime == null) {
            double timestampsDelaySec = ProjectUtils.secondsBetween(messageHour, mongoSenderHour);
            if (timestampsDelaySec < MySQLReceiver.maxCurrentMessageDelaySec && timestampsDelaySec > 0)
                return messageHour;
            else
                return new Timestamp(mongoSenderHour.getTime() - (MySQLReceiver.maxCurrentMessageDelaySec * (long) 1000));
        } else {
            double timestampsDelaySec = ProjectUtils.secondsBetween(lastValidTime, messageHour);
            if (timestampsDelaySec < MySQLReceiver.maxCurrentMessageDelaySec && timestampsDelaySec > 0)
                return messageHour;
            else
                return new Timestamp(lastValidTime.getTime() + correctionIncrement);
        }
    }

    // Para uma dada mensagem, deve ser corrido após o processTempHour, o hour deste método será o hour obtido do processTempHour.
    // Se a temperatura for considerada válida (não errada), atualiza o valor do lastValidTempTime na classe TemperatureSensorValidator
    public ValidationStatus validateTemp(int sensorID, float temp, Timestamp hour) {
        TemperatureSensor temperatureSensor = getTemperatureSensor(sensorID);
        return temperatureSensor == null ? ValidationStatus.INVALID_TEMP_SENSOR : temperatureSensor.validateTemp(temp, hour);

    }

    // Para uma dada mensagem, deve ser corrido após o processMovHour, o hour deste método será o hour obtido do processMovHour.
    // Se o movimento for considerado possível, atualiza o valor do lastValidMovTime
    public ValidationStatus validateMov(int from, int to, Timestamp hour) {
        return movementValidator.validateMov(from, to, hour);
    }

}
