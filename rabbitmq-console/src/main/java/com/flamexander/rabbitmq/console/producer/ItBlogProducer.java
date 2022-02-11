package com.flamexander.rabbitmq.console.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.*;

public class ItBlogProducer {
    private static final String EXCHANGE_NAME = "topic_programming";

    public static void main(String[] argv) throws Exception {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();

             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String topic_message = reader.readLine();
                if (topic_message.equals("stop")) {
                    break;
                }
                if (topic_message.contains(" ")) {
                    String[] topics_messages = topic_message.split(" ");
                    String topic = topics_messages[0];
                    if (topics_messages[1].isEmpty()) {
                        System.out.println("Неверный формат ввода");
                    } else {
                        String message = topic_message.substring(topic.length() + 1);
                        String routingKey = "prog." + topic;

                        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

                    }
                } else {
                    System.out.println("Неверный формат ввода");
                }
            }
        }
    }
}
