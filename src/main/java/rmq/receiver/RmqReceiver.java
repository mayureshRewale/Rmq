package rmq.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RmqReceiver {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Channel channel;
    private String queueName;
    private DeliverCallback deliverCallback;

    private IRmqMessageReader iRmqMessageReader;

    public RmqReceiver(String queueName, IRmqMessageReader IRmqMessageReader) {
        this.queueName = queueName;
        this.iRmqMessageReader = IRmqMessageReader;
    }

    public void createChannel() throws IOException, TimeoutException {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        System.out.println("Waiting for messages");
    }

    public void startListening() throws IOException {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            iRmqMessageReader.readMessage(message);
        };

        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    public void closeConnection() throws IOException, TimeoutException {
        if(channel.isOpen())
            channel.close();
        if(connection.isOpen())
            connection.close();
    }

}
