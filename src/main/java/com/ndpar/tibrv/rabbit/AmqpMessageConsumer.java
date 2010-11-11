package com.ndpar.tibrv.rabbit;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;

import com.tibco.tibrv.TibrvMsg;

public class AmqpMessageConsumer implements MessageListener {

    protected final Log log = LogFactory.getLog(getClass());

    private AtomicLong count = new AtomicLong(0);

    @Override
    public void onMessage(Message message) {
        long c = count.incrementAndGet();
        if (c % 1000 == 0) {
            log.debug(count);
            try {
                new TibrvMsg(message.getBody());
                log.debug(message.toString() + " <<" + message.getBody().length + ">>");
            } catch (Exception e) {
                new RuntimeException(e);
            }
        }
    }
}
