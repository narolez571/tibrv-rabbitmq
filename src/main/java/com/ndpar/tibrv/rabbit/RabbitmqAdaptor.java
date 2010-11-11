package com.ndpar.tibrv.rabbit;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ndpar.tibrv.MessageListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.tibco.tibrv.TibrvMsg;

@ManagedResource(objectName = "com.ndpar:type=tibrv,id=RabbitmqAdaptor")
public class RabbitmqAdaptor implements MessageListener {

    private AtomicLong count = new AtomicLong(0);

    @Autowired
    private ConnectionFactory cf;
    private String exchange;

    private Connection conn;
    private Channel channel;


    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    @PostConstruct
    public void init() {
        try {
            conn = cf.newConnection();
            channel = conn.createChannel();
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            channel.close();
            conn.close();
        } catch (IOException e) {
            new RuntimeException(e);
        }
    }

    @ManagedAttribute
    public long getMessageCount() {
        return count.get();
    }

    @Override
    public void onMessage(TibrvMsg message) {
        try {
            count.incrementAndGet();
            String routing = message.getSendSubject();
            channel.basicPublish(exchange, routing, null, message.getAsBytes());
        } catch (Exception e) {
            new RuntimeException(e);
        }
    }
}
