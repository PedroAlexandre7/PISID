package MainPrograms;

import StructClasses.ProjectUtils;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import javax.swing.*;
import java.io.FileInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@SuppressWarnings("deprecation")
public class CloudToMongo implements MqttCallback {
    public static MongoClient mongoClient;
    public static DB db;
    public static DBCollection mongocol_temp;
    public static DBCollection mongocol_mov;
    public static DBCollection mongocol_pendingMessages;
    public static DBCollection mongocol_status;
    public static String mongo_user = "";
    public static String mongo_password = "";
    public static String mongo_address = "";
    public static String mongo_replica = "";
    public static String mongo_database = "";
    public static String mongo_temp_collection = "";
    public static String mongo_mov_collection = "";
    public static String mongo_pend_collection = "";
    public static String mongo_status_collection = "";
    public static String mongo_authentication = "";
    public static String cloud_server = "";
    public static String cloud_temp_topic = "";
    public static String cloud_mov_topic = "";
    public static MqttClient mqttclient;
    public static JTextArea documentLabel = new JTextArea("\n");
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms");
    public static SimpleDateFormat dateFormat_alt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//alternativa

    public static void main(String[] args) {
        System.out.println("Creating window...");
        ProjectUtils.createWindow("Cloud to Mongo", documentLabel, "Data from broker:", "Stop MongoSender", false);
        System.out.println("Loading ini...");
        loadIni();
        System.out.println("Connecting Mongo...");
        connectMongo();
        System.out.println("Reseting mongo to new Experiment...");
        resetMongo();
        System.out.println("Connecting to cloud...");
        new CloudToMongo().connectCloud();
    }

    private static void resetMongo() {
        try (DBCursor cursor = mongocol_pendingMessages.find()) {
            System.out.println("Got cursor");
            while (cursor.hasNext())
                mongocol_pendingMessages.remove(cursor.next());
            System.out.println("PendingMessages reseted...\nCurrent session ID:" + saveDate(true) + "\n");
        }
    }

    private static int saveDate(boolean incrementID) {

        DBObject status = mongocol_status.findOne();
        if (status == null) {
            mongocol_status.insert((DBObject) JSON.parse("{SessaoID: " + 1 + ", Hora: " + (new Date()).getTime() + "}"));
            return 1;
        } else {
            int newSessionID = (Integer.parseInt(status.get("SessaoID").toString()));
            if (incrementID)
                newSessionID++;
            mongocol_status.update(status, (DBObject) JSON.parse("{SessaoID: " + newSessionID + ", Hora: " + (new Date()).getTime() + "}"));
            return newSessionID;
        }
    }

    private static void loadIni() {
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("ProjetoJar/data.ini"));
            cloud_server = p.getProperty("cloud_server");
            cloud_temp_topic = p.getProperty("cloud_temp_topic");
            cloud_mov_topic = p.getProperty("cloud_mov_topic");
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_temp_collection = p.getProperty("mongo_temp_collection");
            mongo_mov_collection = p.getProperty("mongo_mov_collection");
            mongo_pend_collection = p.getProperty("mongo_pend_collection");
            mongo_status_collection = p.getProperty("mongo_status_collection");
        } catch (Exception e) {
            System.out.println("Error reading data.ini file " + e);
            JOptionPane.showMessageDialog(null, "The data.ini file wasn't found.", "Ini file",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    public static void connectMongo() {
        String mongoURI = "mongodb://";
        if (mongo_authentication.equals("true"))
            mongoURI += mongo_user + ":" + mongo_password + "@";
        mongoURI += mongo_address;
        if (!mongo_replica.equals("false"))
            if (mongo_authentication.equals("true"))
                mongoURI += "/?replicaSet=" + mongo_replica + "&authSource=admin";
            else
                mongoURI += "/?replicaSet=" + mongo_replica;
        else if (mongo_authentication.equals("true"))
            mongoURI += "/?authSource=admin";
        mongoClient = new MongoClient(new MongoClientURI(mongoURI));
        db = mongoClient.getDB(mongo_database);
        mongocol_temp = db.getCollection(mongo_temp_collection);
        mongocol_mov = db.getCollection(mongo_mov_collection);
        mongocol_pendingMessages = db.getCollection(mongo_pend_collection);
        mongocol_status = db.getCollection(mongo_status_collection);
    }

    private static boolean isValidNumber(String value) {
        try {
            Integer.parseInt(value);
        } catch (NumberFormatException e) {
            try {
                Double.parseDouble(value);
                return true;
            } catch (NumberFormatException | NullPointerException ignored) {
            }
            return false;
        }
        return true;
    }

    private static boolean isValidDate(String value) {
        if (value.length() <= 2)
            return false;
        value = value.substring(1, value.length() - 1);
        try {
            dateFormat.parse(value);
        } catch (ParseException e) {
            try {
                dateFormat_alt.parse(value);//estes poderiam ser ignorados
                return true;
            } catch (ParseException ignored) {
            }
            return false;
        }
        return true;
    }

    private static boolean isValidTemp(String key, String value) {
        if (key.equals("Hora"))
            return isValidDate(value);
        else if (key.equals("Leitura") || key.equals("Sensor"))
            return isValidNumber(value);
        else return false;
    }

    private static boolean isValidMov(String key, String value) {
        if (key.equals("Hora"))
            return isValidDate(value);
        else if (key.equals("SalaEntrada") || key.equals("SalaSaida"))
            return isValidNumber(value);
        else return false;
    }

    public static String prepareSpaces(String message) {
        message = message.replaceAll("SalaEntrada:", "SalaEntrada: ");
        message = message.replaceAll("SalaSaida:", "SalaSaida: ");
        message = message.replaceAll("Leitura:", "Leitura: ");
        message = message.replaceAll("Sensor:", "Sensor: ");
        return message.replaceAll("Hora:", "Hora: ");
    }

    public static boolean isValidJson(String message, String topic) {//verifica se a string q vai ser json tem 3 campos, se está a dizer hora, sala,
        message = prepareSpaces(message);

        String[] fields = message.substring(1, message.length() - 1).split(",");
//        System.out.println("campos tamanho: " + campos.length); //{Hora: "2023-05-01 15:46:19.658732", SalaEntrada: 5, SalaSaida: 3}
        // Verifica se a mensagem tem exatamente três campos
        if (fields.length != 3) {
            return false;
        }
//        System.out.println("campos 0: " + campos[0] + "\ncampos 1: " + campos[1] + "\ncampos 2: " + campos[2])
        //Verifica se cada campo começa com uma chave válida com um valor válido e não repete chave
        List<String> keys = new ArrayList<>();
        for (String field : fields) {
            String[] parts = field.split(": ");
            if (parts.length != 2)
                return false;

            for (int i = 0; i < parts.length; i++)
                parts[i] = parts[i].trim();
            String key = parts[0];
            if (keys.contains(key))
                return false;

            keys.add(key);
            String value = parts[1].trim();
            if (topic.equals(cloud_temp_topic)) {
                if (!isValidTemp(key, value))
                    return false;
            } else if (!isValidMov(key, value))
                return false;
        }
        return true;
    }

    private static DBObject processMessageWithError(String topic, String jsonStr) {
        jsonStr = "{MessageWithError: '" + jsonStr + "'}";
        DBObject document_json = (DBObject) JSON.parse(jsonStr);
        if (topic.equals(cloud_mov_topic)) {
            mongocol_pendingMessages.insert(document_json);
        }
        return document_json;
    }

    private static void saveMessagesToMongo(String topic, DBObject document_json) {
        if (topic.equals(cloud_temp_topic)) {
            System.out.println("mensagem guardada: " + document_json + "\n");
            mongocol_temp.insert(document_json);
        } else {
            System.out.println("mensagem guardada: " + document_json + "\n");
            mongocol_mov.insert(document_json);
        }
        saveDate(false);
    }

    public void connectCloud() {
        try {
            int random = new Random().nextInt(100000);
            mqttclient = new MqttClient(cloud_server, "CloudToMongo_" + random, new MqttDefaultFilePersistence("MQTTlogs"));
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_temp_topic);
            mqttclient.subscribe(cloud_mov_topic);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        documentLabel.append(mqttMessage + "\n");
        System.out.println("mesangem recebida: " + mqttMessage + " do topico " + topic);

        if (!(topic.equals(cloud_temp_topic) || topic.equals(cloud_mov_topic))) {
            System.err.println("Invalid Topic: " + topic + " for MqttMessage " + mqttMessage.toString());
            return;
        }
        String message = mqttMessage.toString();
        int start = message.indexOf("{"); // Find the start index of JSON
        int end = message.lastIndexOf("}"); // Find the end index of JSON
        DBObject document_json;

        if (start == -1 || end == -1 || start >= end)  // -1 means not found
            document_json = processMessageWithError(topic, message);
        else {
            String jsonStr = message.substring(start, end + 1);
            if (isValidJson(jsonStr, topic)) {
                document_json = (DBObject) JSON.parse(jsonStr);
                mongocol_pendingMessages.insert(document_json);
            } else
                document_json = processMessageWithError(topic, jsonStr);

        }
        saveMessagesToMongo(topic, document_json);
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.err.println(cause.toString());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
    }
}