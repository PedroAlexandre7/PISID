package StructClasses;

import MainPrograms.MongoSender;
import MainPrograms.MySQLReceiver;

import javax.swing.*;
import java.awt.*;
import java.sql.Timestamp;
import java.time.Duration;

public class ProjectUtils {

    public static final int INITIAL_MAZE_ROOM = 1;
    public static final boolean IGNORE_NEGATIVE_RATS = true;
    public static final boolean USE_MQTT = false;
    public static final boolean USE_MENU = true;
    public static final String TCP_MESSAGE_DIVIDER = "!_";
    public static final String IP_ADDRESS = 123;
    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_YELLOW = "\u001B[33m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_CYAN = "\u001B[36m";
    public static final String ANSI_BLUE = "\u001B[34m";
    public static final String ANSI_PURPLE = "\u001B[35m";

    private ProjectUtils() {
    }

    public static double secondsBetween(Timestamp first, Timestamp second) {
        Duration duration = Duration.between(first.toInstant(), second.toInstant());
        return duration.toNanos() / 1_000_000_000.0;
    }

    public static void createWindow(String windowName, JTextArea textArea, String labelText, String buttonText, boolean mySQLReceiver) {
        textArea.setPreferredSize(new Dimension(600, 200));
        JFrame frame = new JFrame(windowName);
        JLabel textLabel = new JLabel(labelText, SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane(textArea, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b = new JButton(buttonText);
        frame.getContentPane().add(b, BorderLayout.PAGE_END);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        if (mySQLReceiver)
            b.addActionListener(evt -> MySQLReceiver.endExperiment());
        else
            b.addActionListener(evt -> MongoSender.isSending = false);
    }
}
