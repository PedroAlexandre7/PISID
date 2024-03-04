package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.*;

public class TemperatureSensor {

    public static final float MAX_PROGRAMMED_TEMP_DIFFERENCE = 30;
    // A primeiríssima temperatura de um dado sensor numa experiência
    // será aceite se não se afastar este valor da programmedTemp
    public final int sensorID;
    private final ExperimentValidator expValidator;
    private float lastValidTemp;
    private Timestamp lastValidTempTime;
    private int consecutiveTempErrors;

    public TemperatureSensor(int sensorID, ExperimentValidator expValidator) {
        this.sensorID = sensorID;
        this.expValidator = expValidator;
        consecutiveTempErrors = 0;
        loadSQLInfo();
    }


    public Timestamp getLastValidTempTime() {
        return lastValidTempTime;
    }

    private void loadSQLInfo() {
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call getLastValidTempMeasure(?,?,?,?)}");
            cs.setInt(1, MySQLReceiver.experimentID);
            cs.setInt(2, sensorID);
            cs.registerOutParameter(3, Types.TIMESTAMP);
            cs.registerOutParameter(4, Types.DECIMAL);
            cs.execute();
            cs.getTimestamp("horas");
            if (cs.wasNull())
                return;
            lastValidTempTime = cs.getTimestamp("horas");
            lastValidTemp = cs.getFloat("temperatura");

        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }

    }

    public ValidationStatus validateTemp(float temp, Timestamp hour) {
        if (lastValidTempTime == null)
            if (temp >= expValidator.programmedTemp + MAX_PROGRAMMED_TEMP_DIFFERENCE || temp <= expValidator.programmedTemp - MAX_PROGRAMMED_TEMP_DIFFERENCE)
                return ValidationStatus.ABSURD_TEMP;
            else
                return checkThresholds(temp, hour);
        if (consecutiveTempErrors == expValidator.maxConsecutiveTempErrors)
            return checkThresholds(temp, hour);
        if (calculateTempVariance(temp, hour) > expValidator.maxTempVariance) {
            consecutiveTempErrors++;
            return ValidationStatus.ABSURD_TEMP;
        }
        return checkThresholds(temp, hour);
    }

    private ValidationStatus checkThresholds(float temp, Timestamp hour) {
        consecutiveTempErrors = 0;
        this.lastValidTemp = temp;
        this.lastValidTempTime = hour;
        if (expValidator.lowTempThreshholds.removeIf(element -> (temp <= element)))
            return expValidator.lowTempThreshholds.isEmpty() ? ValidationStatus.LOW_TEMP_CRITICAL : ValidationStatus.LOW_TEMP_WARNING;
        if (expValidator.highTempThreshholds.removeIf(element -> (temp >= element)))
            return expValidator.highTempThreshholds.isEmpty() ? ValidationStatus.HIGH_TEMP_CRITICAL : ValidationStatus.HIGH_TEMP_WARNING;
        return ValidationStatus.OK;

    }

    private float calculateTempVariance(float temp, Timestamp hour) {
        float tempDiff = temp - lastValidTemp;
        double timeDiffSec = ProjectUtils.secondsBetween(lastValidTempTime, hour);
        if (timeDiffSec <= 0)
            throw new RuntimeException("Something went wrong with hour validation, movement validation compromised.");
        return (float) (tempDiff / timeDiffSec);
    }

}
