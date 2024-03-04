package StructClasses;

import MainPrograms.MySQLReceiver;

import java.sql.Timestamp;

public enum AlertType {
    CLOUDTOMONGO_DISCONNECT("CloudToMongo Desconectado"),
    CLOUDTOMONGO_SESSION_CHANGE("Sessão CloudToMongo Alterada"),
    MONGOSENDER_DISCONNECT("MongoSender Desconectado"),
    MQTT_DISCONNECT("MQTT Broker Desconectado"),
    MISFORMATTED_MOVEMENT_MESSAGE("Mensagem De Movimento Desformatada"),
    NONEXISTENT_CORRIDOR("Corredor Não Existe"),
    NEGATIVE_RATS("Ratos Negativos"),
    OVERCROWDED_ROOM("Limite Ratos Sala"),
    RATS_ARE_HAPPY("Ratos Contentes"),
    LOW_TEMP_WARNING("Temperatura Baixa Aviso"),
    LOW_TEMP_CRITICAL("Temperatura Baixa Crítico"),
    HIGH_TEMP_WARNING("Temperatura Alta Aviso"),
    HIGH_TEMP_CRITICAL("Temperatura Alta Crítico"),
    EXPERIMENT_ALREADY_CONCLUDED("Experiência Já Foi Concluída");

    private static boolean alertMongoDisconnect = true;
    private static boolean alertMongoSessionChange = true;
    private static boolean alertImpossibleMovement = true;
    private static boolean alertMongoSenderDisconnect = true;
    private static boolean alertRatsAreHappy = true;
    private static boolean alertMultipleExperimentFinalization = true;
    private static Timestamp lastOverCrowdedAlert = null;
    private final String typeString;

    AlertType(String typeString) {
        this.typeString = typeString;
    }

    public static boolean shouldAlert(AlertType alertType) {
        switch (alertType) {
            case CLOUDTOMONGO_DISCONNECT -> {
                if (!alertMongoDisconnect)
                    return false;
                alertMongoDisconnect = false;
                alertMongoSessionChange = true;
                alertRatsAreHappy = false;
            }
            case CLOUDTOMONGO_SESSION_CHANGE -> {
                if (!alertMongoSessionChange)
                    return false;
                alertMongoSessionChange = false;
                alertMongoDisconnect = true;
                alertRatsAreHappy = false;
            }
            case MONGOSENDER_DISCONNECT -> {
                if (!alertMongoSenderDisconnect)
                    return false;
                alertMongoSenderDisconnect = false;
            }
            case MISFORMATTED_MOVEMENT_MESSAGE, NONEXISTENT_CORRIDOR, NEGATIVE_RATS -> {
                if (!alertImpossibleMovement)
                    return false;
                alertImpossibleMovement = false;
                alertRatsAreHappy = false;
            }
            case OVERCROWDED_ROOM -> {
                if (lastOverCrowdedAlert == null || ProjectUtils.secondsBetween(lastOverCrowdedAlert, new Timestamp(System.currentTimeMillis())) > MySQLReceiver.maxSecNoMovement) {
                    lastOverCrowdedAlert = new Timestamp(System.currentTimeMillis());
                    return true;
                }
                return false;
            }
            case EXPERIMENT_ALREADY_CONCLUDED -> {
                if (!alertMultipleExperimentFinalization)
                    return false;
                alertMultipleExperimentFinalization = false;
            }
            case RATS_ARE_HAPPY -> {
                return alertRatsAreHappy;
            }
        }
        return true;
    }

    public String getTypeString() {
        return typeString;
    }

}
