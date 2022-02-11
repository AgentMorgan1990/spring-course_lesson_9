package com.flamexander.rabbitmq.console.consumer;

import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.rabbitmq.client.*;

public class ItBlogConsumer {

    private static final String EXCHANGE_NAME = "topic_programming";


    public static void main(String[] argv) throws Exception {
        Map<String, String> queueMap = new HashMap<>();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String topic_message = reader.readLine();
            if (topic_message.equals("stop")) {
                break;
            }
            if (topic_message.contains(" ")) {
                String[] topics_messages = topic_message.split(" ");
                String command = topics_messages[0];
                if (topics_messages[1].isEmpty()) {
                    System.out.println("Неверный формат ввода");
                }
                if (command.equals("set_topic")) {
                    String queueName = channel.queueDeclare().getQueue();
                    System.out.println("QUEUE NAME: " + queueName);

                    String routingKey = "prog." + topics_messages[1];
                    channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
                    queueMap.put(routingKey, queueName);
                    System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");

                    };
                    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                    });

                } else if (command.equals("delete_topic")) {
                    String routingKey = "prog." + topics_messages[1];
                    channel.queueUnbind(queueMap.get(routingKey), EXCHANGE_NAME, routingKey);
                    queueMap.remove(routingKey);
                    System.out.println(" [*] No more waiting for messages with routing key (" + routingKey + "):");
                } else {
                    System.out.println("Неизвестная команда");
                }

            } else {
                System.out.println("Неверный формат ввода");
            }
        }
    }
}




