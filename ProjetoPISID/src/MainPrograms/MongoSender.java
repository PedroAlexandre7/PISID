package MainPrograms;

import StructClasses.ProjectUtils;
import com.mongodb.*;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;

import javax.swing.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

@SuppressWarnings("deprecation")
public class MongoSender implements MqttCallback {

    public static MongoClient mongoClient;
    public static DB db;
    public static DBCollection mongocol_pending;
    public static DBCollection mongocol_status;
    public static String mongo_user = "";
    public static String mongo_password = "";
    public static String mongo_address = "";
    public static String mongo_replica = "";
    public static String mongo_database = "";
    public static String mongo_pending_collection = "";
    public static String mongo_status_collection = "";
    public static String mongo_authentication = "";
    public static int tcp_server_port;
    public static ServerSocket tcp_serverSocket;
    public static Socket tcp_socket;
    public static ObjectOutputStream tcp_out;
    public static boolean isSending = true;
    public static String mqtt_server = "";
    public static String mqtt_temp_topic = "";
    public static String mqtt_mov_topic = "";
    public static String mqtt_status_topic = "";
    public static Double mqtt_response_time_limit;
    public static Queue<DBObject> waitingDeliveryTemp = new LinkedBlockingQueue<>();
    public static Queue<DBObject> waitingDeliveryMov = new LinkedBlockingQueue<>();
    public static MqttClient mqttclient;
    public static JTextArea textArea = new JTextArea(10, 50);


    public static void main(String[] args) {
        ProjectUtils.createWindow("Send to MQTT", textArea, "Data to send to broker: ", "Stop MongoSender", false);
        loadIni();
        connectMongo();
        if (ProjectUtils.USE_MQTT)
            new MongoSender().connectMQTT();
        else
            createServerAndConnect();
        while (isSending)
            try {
                sendMessages();
            } catch (IOException e) {
                System.err.println("Message flow stopped. Trying to reconnect...");
                connectTCP();
            }
    }

    private static void createServerAndConnect() {
        try {
            System.out.println("Creating server socket...");
            tcp_serverSocket = new ServerSocket(tcp_server_port);
            connectTCP();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void connectTCP() {
        try {
            System.out.println("Waiting for MySQLReceiver...");
            tcp_socket = tcp_serverSocket.accept();
            System.out.println("Connected to MySQLReceiver");
            tcp_out = new ObjectOutputStream(tcp_socket.getOutputStream());
            System.out.println("Output Stream created");
        } catch (IOException ex) {
            throw new RuntimeException();
        }
    }

    public static void publish(String topic, String reading) {
        try {
            MqttMessage mqtt_message = new MqttMessage();
            mqtt_message.setPayload(reading.getBytes());
            mqttclient.publish(topic, mqtt_message);
        } catch (MqttException e) {
            e.printStackTrace();
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
            mqtt_response_time_limit = Double.parseDouble(p.getProperty("mqtt_response_time_limit"));
            tcp_server_port = Integer.parseInt(p.getProperty("tcp_server_port"));
            mongo_address = p.getProperty("mongo_address");
            mongo_user = p.getProperty("mongo_user");
            mongo_password = p.getProperty("mongo_password");
            mongo_replica = p.getProperty("mongo_replica");
            mongo_database = p.getProperty("mongo_database");
            mongo_authentication = p.getProperty("mongo_authentication");
            mongo_pending_collection = p.getProperty("mongo_pend_collection");
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
        mongocol_pending = db.getCollection(mongo_pending_collection);
        mongocol_status = db.getCollection(mongo_status_collection);
    }

    @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
    private static void sendMessages() throws IOException {
        try {
            while (true) {
                try {
                    // Sleep for 1 second
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    System.err.println("sleep interupted");
                }
                checkCloudAndSendId();
                try (DBCursor cursor = mongocol_pending.find().sort((new BasicDBObject("$natural", 1)))) {/*.sort(new BasicDBObject("$natural", 1))*/
                    while (cursor.hasNext()) {
                        DBObject currentMessage = cursor.next();
                        if (currentMessage.containsField("Leitura")) {
                            if (ProjectUtils.USE_MQTT)
                                sendUsingMQTT(mqtt_temp_topic, currentMessage, waitingDeliveryTemp);
                            else
                                sendUsingTCP(mqtt_temp_topic, currentMessage);
                        } else {
                            if (ProjectUtils.USE_MQTT)
                                sendUsingMQTT(mqtt_mov_topic, currentMessage, waitingDeliveryMov);
                            else
                                sendUsingTCP(mqtt_mov_topic, currentMessage);
                        }
                    }
                }
            }
        } finally {
            if (!ProjectUtils.USE_MQTT) {
                try {
                    System.out.println("Trying to close connection...");
                    tcp_out.close();
                    tcp_socket.close();
                    tcp_serverSocket.close();
                    System.out.println("Connection closed");
                } catch (IOException e) {
                    System.err.println("Couldn't close connection, it may have been already closed?");
                }
            }
        }

    }

    private static void sendUsingTCP(String topic, DBObject currentMessage) throws IOException {
        currentMessage.removeField("_id");
        currentMessage.put("HoraEnvio", new Timestamp(System.currentTimeMillis()).toString());
        tcp_out.writeObject(topic + ProjectUtils.TCP_MESSAGE_DIVIDER + currentMessage);
        currentMessage.removeField("HoraEnvio");
        mongocol_pending.remove(currentMessage);
        System.out.println("Sent: " + currentMessage);
    }

    private static void sendUsingMQTT(String topic, DBObject currentMessage, Queue<DBObject> waitingDelivery) {
        waitingDelivery.offer(currentMessage);
        currentMessage.removeField("_id");
        currentMessage.put("HoraEnvio", new Timestamp(System.currentTimeMillis()).toString());
        publish(topic, currentMessage.toString());
    }

    private static void checkCloudAndSendId() throws IOException {
        DBObject status = mongocol_status.findOne();
        String sessionID = status.get("SessaoID").toString();
        if (ProjectUtils.secondsBetween(new Timestamp(Long.parseLong(status.get("Hora").toString())), new Timestamp(new Date().getTime())) > mqtt_response_time_limit)
            sessionID = "-1";
        if (ProjectUtils.USE_MQTT)
            publish(mqtt_status_topic, status.get("SessaoID").toString());
        else
            tcp_out.writeObject(mqtt_status_topic + ProjectUtils.TCP_MESSAGE_DIVIDER + sessionID);
        System.out.println("Sent cur id");
    }

    public void connectMQTT() {
        try {
            int random = new Random().nextInt(100000);
            mqttclient = new MqttClient(mqtt_server, "MongoSender_" + random, new MqttDefaultFilePersistence("MQTTlogs"));
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(mqtt_temp_topic, 1);
            mqttclient.subscribe(mqtt_mov_topic, 2);
            mqttclient.subscribe(mqtt_status_topic, 0);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.err.println(cause.toString());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        for (String topic : token.getTopics()) {
            if (topic.equals(mqtt_temp_topic)) {
                DBObject message = waitingDeliveryTemp.remove();
                System.out.println("Sent: " + message);
                message.removeField("HoraEnvio");
                mongocol_pending.remove(message);
            } else if (topic.equals(mqtt_mov_topic)) {
                DBObject message = waitingDeliveryMov.remove();
                System.out.println("Sent: " + message);
                message.removeField("HoraEnvio");
                mongocol_pending.remove(message);
            } else if (topic.equals(mqtt_status_topic))
                System.out.println("Sent cur id");
        }
    }
}