package MainPrograms;

import StructClasses.*;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import javax.swing.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.sql.*;
import java.util.*;

public class MySQLReceiver implements MqttCallback {

    public static MqttClient mqttclient;
    public static String mqtt_server = "";
    public static String mqtt_temp_topic = "";
    public static String mqtt_mov_topic = "";
    public static String mqtt_status_topic = "";
    public static MqttDefaultFilePersistence mqtt_buffer = null;
    public static int tcp_server_port;
    public static Socket tcp_socket;
    public static ObjectInputStream tcp_in;
    public static boolean experimentRunning = true;
    public static JTextArea documentLabelMongoSender = new JTextArea("\n");
    public static JTextArea documentLabelMySQL = new JTextArea("\n");
    public static Connection connTo;
    public static String sql_database_connection = "";
    public static String sql_database_password = "";
    public static String sql_database_user = "";
    public static String sql_experiment_table = "";
    public static String sql_alert_threshholds_table = "";
    public static int experimentID;
    public static Integer mongoSessionID;
    public static int maxCurrentMessageDelaySec;
    public static int maxDisconnectedMessageDelaySec;
    public static int maxSecNoMovement;
    public static ExperimentValidator expValidator;
    public static Timestamp lastStatusMessageTime = null;
    public static RatHappinessChecker ratHappinessChecker = new RatHappinessChecker();

    public static void main(String[] args) {
        if (ProjectUtils.USE_MENU)
            openMenu();
        else
            startExperiment();
    }


    private static Scanner scanner;

    private static void openMenu() {
        experimentRunning = false;
        boolean running = true;
        Thread mySQLReceiver = new Receiver();
        initMenu();
        while (running) {
            showPrompt();
            String option = getUserOption();
            if (option.equals("0") || option.equals("2")) {
                running = false;
                if (experimentRunning) {
                    endExperiment();
                    try {
                        mySQLReceiver.join();
                    } catch (InterruptedException ignored) {
                    }
                    System.out.println("Experiência terminada com sucesso.");
                }
                scanner.close();
            } else if (option.equals("1")) {
                experimentRunning = true;
                mySQLReceiver.start();
            }
        }
    }

    private static void showPrompt() {
        System.out.println("\n\n\n\n\n\n\n\n\n\n\n");
        System.out.println("O que deseja fazer:");
        System.out.println("1. Iniciar registo da experiência");
        System.out.println("2. Terminar experiência");
        System.out.println("0. Sair");
    }

    private static String getUserOption() {
        return scanner.nextLine();
    }

    private static void initMenu() {
        scanner = new Scanner(System.in);
    }

    private static class Receiver extends Thread {

        @Override
        public void run() {
            startExperiment();
        }
    }

    public static int getExpID() {
        int expID = getExperimentId();
        try {
            while (expID < 0) {
                expID = getExperimentId();
                Thread.sleep(500);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return expID;
    }


    public static int getExperimentId() {
        int expID;
        try {
            CallableStatement cs = MySQLReceiver.connTo.prepareCall("{call GetLastExperienceId(?)}");
            cs.registerOutParameter(1, Types.INTEGER);
            cs.execute();
            cs.getInt(1);
            if (cs.wasNull())
                return -1;
            expID = cs.getInt("last_id");
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
        return expID;
    }

    private static void startExperiment() {
        ProjectUtils.createWindow("Receive from MQTT", documentLabelMongoSender, "Data from broker: ", "Stop the experiment", true);
//      ProjectUtils.createWindow("Data Bridge", documentLabelMySQL, "Data: ", "Stop the program");
        loadIni();
        connectCloud();
        connectMySQL();
        int expID = getExpID();
        getExperimentInfo(expID);
        ratHappinessChecker.start();
        if (ProjectUtils.USE_MQTT)
            new MySQLReceiver().connectMQTT();
        else {
            connectTCP();
            receiveTCPMessages();
        }
    }

    private static void loadIni() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("ProjetoJar/data.ini"));
            mqtt_server = p.getProperty("mqtt_server");
            mqtt_temp_topic = p.getProperty("mqtt_temp_topic");
            mqtt_mov_topic = p.getProperty("mqtt_mov_topic");
            mqtt_status_topic = p.getProperty("mqtt_status_topic");
            mqtt_buffer = new MqttDefaultFilePersistence(p.getProperty("mqtt_buffer_directory"));
            tcp_server_port = Integer.parseInt(p.getProperty("tcp_server_port"));
            sql_experiment_table = p.getProperty("sql_experiment_table");
            sql_alert_threshholds_table = p.getProperty("sql_alert_threshholds_table");
            sql_database_connection = p.getProperty("sql_database_connection");
            sql_database_password = p.getProperty("sql_database_password");
            sql_database_user = p.getProperty("sql_database_user");
        } catch (Exception e) {
            System.err.println("Error reading data.ini file " + e);
            JOptionPane.showMessageDialog(null, "The data.ini file wasn't found.", "Ini file", JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void connectMySQL() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo = DriverManager.getConnection(sql_database_connection, sql_database_user, sql_database_password);
            documentLabelMySQL.append("SQl Connection:" + sql_database_connection + "\n");
            documentLabelMySQL.append("Connection To MariaDB Destination " + sql_database_connection + " Succeeded" + "\n");
        } catch (Exception e) {
            System.err.println("Mysql Server Destination down, unable to make the connection. " + e);
        }
    }

    private static void connectCloud() {
        try {
            CloudToMySQL.setup();
        } catch (SQLException e) {
            throw new RuntimeException("Cloud MySQL destination is down, couldn't connect.");
        }
    }

    private static void getExperimentInfo(int expID) {
        try {
            Statement statement = connTo.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + sql_experiment_table + " WHERE id=" + expID);
            if (!rs.next())
                throw new RuntimeException("Inexistent experiment ID: " + expID);
            experimentID = expID;
            maxSecNoMovement = rs.getInt("segundos_sem_movimento");
            int totalRats = rs.getInt("numero_ratos");
            int maxRatsInRoom = rs.getInt("limite_ratos_sala");
            float idealTemp = rs.getFloat("temperatura_ideal");
            float maxTempVarianceFromIdeal = rs.getFloat("variacao_temperatura_ideal_maxima");
            Pair<List<Float>> tempThreshholds = getTempThreshholds(idealTemp, rs.wasNull() ? null : maxTempVarianceFromIdeal);
            float maxTempVariance = rs.getFloat("variacao_temperatura_por_sec_maxima");
            int maxConsecutiveTempErrors = rs.getInt("maximo_erros_temperatura_consecutivos");
            maxCurrentMessageDelaySec = rs.getInt("delay_max_mensagem");
            maxDisconnectedMessageDelaySec = rs.getInt("delay_max_mensagem_desconectado");
            int mongoSessionIDSQL = rs.getInt("id_sessao_mongo");
            mongoSessionID = rs.wasNull() ? null : mongoSessionIDSQL;
            if (mongoSessionID != null)
                maxCurrentMessageDelaySec = maxDisconnectedMessageDelaySec;
            float programmedtemp = CloudToMySQL.getProgrammedTemp();
            List<Corridor> corridors = CloudToMySQL.getCorridors();
            int numRooms = CloudToMySQL.getNumRooms();
            expValidator = new ExperimentValidator(tempThreshholds.first(), tempThreshholds.second(), programmedtemp, maxTempVariance, maxConsecutiveTempErrors, corridors, numRooms, totalRats, maxRatsInRoom);
        } catch (SQLException e) {
            throw new RuntimeException("Error executing SQL statement.\n" + e);
        }
    }

    private static Pair<List<Float>> getTempThreshholds(float idealTemp, Float maxTempVarianceFromIdeal) throws SQLException {
        List<Float> lowTempThreshholds = new LinkedList<>();
        List<Float> highTempThreshholds = new LinkedList<>();
        if (maxTempVarianceFromIdeal != null) {
            lowTempThreshholds.add(idealTemp - maxTempVarianceFromIdeal);
            highTempThreshholds.add(idealTemp + maxTempVarianceFromIdeal);
        }
        Statement statement = connTo.createStatement();
        ResultSet rs = statement.executeQuery("SELECT temperatura FROM " + sql_alert_threshholds_table + " WHERE id_experiencia=" + experimentID);
        while (rs.next()) {
            float temp = rs.getFloat("temperatura");
            if (temp > idealTemp)
                highTempThreshholds.add(temp);
            else if (temp < idealTemp)
                lowTempThreshholds.add(temp);
        }
        return new Pair<>(lowTempThreshholds, highTempThreshholds);
    }

    private static void processStatusMessage(int messageSessionID) {
        lastStatusMessageTime = new Timestamp(System.currentTimeMillis());
        if (messageSessionID == -1)
            MySQLWriter.generateAlert(AlertType.CLOUDTOMONGO_DISCONNECT, "O CloudToMongo ou a Cloud do ISCTE está em baixo, dados recebidos a partir de agora serão inválidos.", null, null, null, null);
        else if (mongoSessionID == null) {
            mongoSessionID = messageSessionID;
            MySQLWriter.initExperimentSession();
        } else if (mongoSessionID != messageSessionID)
            MySQLWriter.generateAlert(AlertType.CLOUDTOMONGO_SESSION_CHANGE, "A sessão do CloudToMongo atual não é a mesma da associada à experiência, dados recebidos a partir de agora serão inválidos.", null, null, null, null);
    }

    private static void processMovMessage(int roomFrom, int roomTo, Timestamp messageHour, Timestamp mongoSenderHour) {
        Timestamp correctedTimestamp = expValidator.processMovHour(messageHour, mongoSenderHour);
        ValidationStatus status = expValidator.validateMov(roomFrom, roomTo, correctedTimestamp);
        switch (status) {
            case OK -> MySQLWriter.insertMov(roomFrom, roomTo, correctedTimestamp);
            case NONEXISTENT_CORRIDOR ->
                    MySQLWriter.generateAlert(AlertType.NONEXISTENT_CORRIDOR, "Não existe um corredor da sala: " + roomFrom + " para a sala: " + roomTo + ", ou então pelo menos uma dessas salas não existe.", null, correctedTimestamp, null, null);
            case NEGATIVE_RATS -> {
                if (ProjectUtils.IGNORE_NEGATIVE_RATS)
                    MySQLWriter.insertMov(roomFrom, roomTo, correctedTimestamp);
                else
                    MySQLWriter.generateAlert(AlertType.NEGATIVE_RATS, "Contagem negativa de ratos registada na sala: " + roomFrom + ".", roomFrom, correctedTimestamp, null, null);
            }
            case OVERCROWDED_ROOM -> {
                MySQLWriter.generateAlert(AlertType.OVERCROWDED_ROOM, "A sala: " + roomTo + " está sobrelotada.", roomTo, correctedTimestamp, null, null);
                MySQLWriter.insertMov(roomFrom, roomTo, correctedTimestamp);
            }
        }
    }

    private static void processTempMessage(float temp, int sensorID, Timestamp messageHour, Timestamp mongoSenderHour) {
        Timestamp correctedTimestamp = expValidator.processTempHour(sensorID, messageHour, mongoSenderHour);
        ValidationStatus status = expValidator.validateTemp(sensorID, temp, correctedTimestamp);
        switch (status) {
            case OK -> MySQLWriter.insertTemp(temp, sensorID, correctedTimestamp);
            case ABSURD_TEMP, INVALID_TEMP_SENSOR -> {
            }
            case LOW_TEMP_WARNING -> {
                MySQLWriter.generateAlert(AlertType.LOW_TEMP_WARNING, "A temperatura de " + temp + "ºC registada no sensor: " + sensorID + " está a ficar baixa.", null, correctedTimestamp, sensorID, temp);
                MySQLWriter.insertTemp(temp, sensorID, correctedTimestamp);
            }
            case LOW_TEMP_CRITICAL -> {
                MySQLWriter.generateAlert(AlertType.LOW_TEMP_CRITICAL, "A temperatura de " + temp + "ºC registada no sensor: " + sensorID + " atingiu valores críticos.", null, correctedTimestamp, sensorID, temp);
                MySQLWriter.insertTemp(temp, sensorID, correctedTimestamp);
            }
            case HIGH_TEMP_WARNING -> {
                MySQLWriter.generateAlert(AlertType.HIGH_TEMP_WARNING, "A temperatura de " + temp + "ºC registada no sensor: " + sensorID + " está a ficar elevada.", null, correctedTimestamp, sensorID, temp);
                MySQLWriter.insertTemp(temp, sensorID, correctedTimestamp);
            }
            case HIGH_TEMP_CRITICAL -> {
                MySQLWriter.generateAlert(AlertType.HIGH_TEMP_CRITICAL, "A temperatura de " + temp + "ºC registada no sensor: " + sensorID + " atingiu valores críticos.", null, correctedTimestamp, sensorID, temp);
                MySQLWriter.insertTemp(temp, sensorID, correctedTimestamp);
            }
        }
    }

    public static void endExperiment() {
        ratHappinessChecker.interrupt();
        if (ProjectUtils.USE_MQTT) {
            try {
                mqttclient.disconnect(1000);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        } else {
            closeConnection("experiment ended");
            experimentRunning = false;
        }
        MySQLWriter.insertFinalMeasurements();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Experiência terminada com sucesso");
        System.exit(0);
    }

    private static void connectTCP() {
        System.out.println("Connecting to MongoSender in address: " + ProjectUtils.IP_ADDRESS);
        tcp_socket = null;
        while (tcp_socket == null || !tcp_socket.isConnected()) {
            try {
                tcp_socket = new Socket(InetAddress.getByName(ProjectUtils.IP_ADDRESS), tcp_server_port);
                System.out.println("Connected to MongoSender, creating Input Stream...");
                tcp_in = new ObjectInputStream(tcp_socket.getInputStream());
                System.out.println("Input Stream created");
            } catch (IOException e) {
                System.err.println("Couldn't connect to MongoSender, retrying connection...");
            }
        }

    }

    private static void receiveTCPMessages() {
        try {
            while (experimentRunning) {
                try {
                    String[] tcpMessage = ((String) tcp_in.readObject()).split(ProjectUtils.TCP_MESSAGE_DIVIDER);
                    documentLabelMongoSender.append(tcpMessage[1] + "\n");
                    processMessage(tcpMessage[0], tcpMessage[1]);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    if (experimentRunning) {
                        System.err.println("Connection failed. Reconnecting...");
                        connectTCP();
                    }
                }
            }
        } finally {
            closeConnection("receiver loop stopped");
        }
    }

    private static void closeConnection(String reason) {
        try {
            System.out.println("Trying to close connection because " + reason + "...");
            tcp_in.close();
            tcp_socket.close();
            System.out.println("Connection closed");
        } catch (IOException e) {
            System.err.println("Couldn't close connection, it may have been already closed");
        }
    }

    @SuppressWarnings({"unchecked", "deprecation"})
    private static void processMessage(String topic, String message) {
        try {
            if (topic.equals(mqtt_status_topic)) {
                System.out.println(ProjectUtils.ANSI_BLUE + "Status received: " + message + ProjectUtils.ANSI_RESET);
                processStatusMessage(Integer.parseInt(message));
                return;
            }
            Map<String, Object> messageMap = ((DBObject) JSON.parse(message)).toMap();
            if (messageMap.containsKey("MessageWithError") && topic.equals(mqtt_mov_topic)) {
                MySQLWriter.generateAlert(AlertType.MISFORMATTED_MOVEMENT_MESSAGE, "Mensagem de movimento com formato errado recebida.", null, null, null, null);
                return;
            }
            Timestamp mongoSenderHour = Timestamp.valueOf(messageMap.get("HoraEnvio").toString());
            Timestamp messageHour = Timestamp.valueOf(messageMap.get("Hora").toString());

            if (ProjectUtils.secondsBetween(mongoSenderHour, new Timestamp(System.currentTimeMillis())) > maxCurrentMessageDelaySec) {
                System.out.println(ProjectUtils.ANSI_GREEN + "Mensagem antiga descartada porque estava " + ProjectUtils.secondsBetween(mongoSenderHour, new Timestamp(System.currentTimeMillis())) + " segundos atrasada." + ProjectUtils.ANSI_RESET);
                return;
            }

            if (topic.equals(mqtt_temp_topic)) {
                System.out.println(ProjectUtils.ANSI_CYAN + "Temp message received: " + message + ProjectUtils.ANSI_RESET);
                Object tempObject = messageMap.get("Leitura");
                float temp = tempObject instanceof Integer ? ((Integer) tempObject).floatValue() : ((Double) tempObject).floatValue();
                processTempMessage(temp, (int) messageMap.get("Sensor"), messageHour, mongoSenderHour);
            } else if (topic.equals(mqtt_mov_topic)) {
                System.out.println(ProjectUtils.ANSI_PURPLE + "Mov message received: " + message + ProjectUtils.ANSI_RESET);
                processMovMessage((int) messageMap.get("SalaEntrada"), (int) messageMap.get("SalaSaida"), messageHour, mongoSenderHour);
            } else {
                System.err.println("Invalid topic: " + topic);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void connectMQTT() {
        try {
            mqttclient = new MqttClient(mqtt_server, "MySQLReceiver_" + experimentID, mqtt_buffer);
            MqttConnectOptions mqttOptions = new MqttConnectOptions();
            mqttOptions.setCleanSession(false);
            mqttOptions.setMaxInflight(maxDisconnectedMessageDelaySec * 5);
            mqttclient.connect(mqttOptions);
            mqttclient.setCallback(this);
            mqttclient.subscribe(mqtt_status_topic, 0);
            mqttclient.subscribe(mqtt_temp_topic, 1);
            mqttclient.subscribe(mqtt_mov_topic, 2);
        } catch (MqttException e) {
            MySQLWriter.generateAlert(AlertType.MQTT_DISCONNECT, "Ocorreu um erro de conexão com o broker MQTT.", null, null, null, null);
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        documentLabelMongoSender.append(mqttMessage.toString() + "\n");
        processMessage(topic, mqttMessage.toString());
    }

    @Override
    public void connectionLost(Throwable cause) {
        MySQLWriter.generateAlert(AlertType.MQTT_DISCONNECT, "Ocorreu um erro de conexão com o broker MQTT.", null, null, null, null);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }

    private record Pair<T>(T first, T second) {
    }

    private static class RatHappinessChecker extends Thread {

        private RatHappinessChecker() {
        }

        @SuppressWarnings("BusyWait")
        public void run() {
            try {
                do {
                    sleep((long) (secondsUntilNextCheck() * 1000));
                    if (!getRatHappiness())
                        continue;
                    if (receivedMessageRecently()) {
                        String alertMessage = "Dadas as circunstâncias e o facto não existirem erros que tornem a experiência inválida, assume-se que os ratos estejam contentes nas salas onde se encontram e que a experiência deva terminar.";
                        MySQLWriter.generateAlert(AlertType.RATS_ARE_HAPPY, alertMessage, null, null, null, null);
                        return;
                    } else {
                        maxCurrentMessageDelaySec = maxDisconnectedMessageDelaySec;
                        String alertMessage = "Não foi possível verificar a felicidade dos ratos pois apesar de ter passado o tempo indicado sem movimento, o MongoSender não enviou informação durante esse tempo.";
                        MySQLWriter.generateAlert(AlertType.MONGOSENDER_DISCONNECT, alertMessage, null, null, null, null);
                    }

                } while (!isInterrupted());
            } catch (InterruptedException ignored) {
            }
        }

        private double secondsUntilNextCheck() {
            if (expValidator.getLastValidMovTime() == null)
                return maxSecNoMovement;
            return Math.max(1, maxSecNoMovement - ProjectUtils.secondsBetween(expValidator.getLastValidMovTime(), new Timestamp(System.currentTimeMillis())));
        }

        private boolean receivedMessageRecently() {
            return lastStatusMessageTime != null && ProjectUtils.secondsBetween(lastStatusMessageTime, new Timestamp(System.currentTimeMillis())) < maxSecNoMovement;
        }

        private boolean getRatHappiness() {
            return expValidator.getLastValidMovTime() != null && ProjectUtils.secondsBetween(expValidator.getLastValidMovTime(), new Timestamp(System.currentTimeMillis())) > maxSecNoMovement;
        }
    }

}