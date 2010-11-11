package com.ndpar.tibrv;

import java.util.concurrent.Executor;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.jmx.export.annotation.ManagedAttribute;

import com.tibco.tibrv.Tibrv;
import com.tibco.tibrv.TibrvDispatcher;
import com.tibco.tibrv.TibrvException;
import com.tibco.tibrv.TibrvListener;
import com.tibco.tibrv.TibrvMsg;
import com.tibco.tibrv.TibrvMsgCallback;
import com.tibco.tibrv.TibrvQueue;
import com.tibco.tibrv.TibrvRvdTransport;
import com.tibco.tibrv.TibrvTransport;

/**
 * Small abstraction for Tibco RV to make it Spring friendly.
 * 
 * @author Andrey Paramonov
 */
public class SimpleMessageListenerContainer implements TibrvMsgCallback, BeanNameAware {

    protected final Log log = LogFactory.getLog(getClass());

    private String threadName;

    private Executor taskExecutor;
    private MessageListener messageListener;

    private String service;
    private String network;
    private String daemon;
    private String subject;

    public void setTaskExecutor(Executor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    public void setMessageListener(MessageListener messageListener) {
        this.messageListener = messageListener;
    }

    //---------------------------------------------------------------
    // Configuration
    //---------------------------------------------------------------

    @ManagedAttribute
    public String getThreadName() {
        return threadName;
    }

    @ManagedAttribute
    public String getService() {
        return service;
    }

    public void setService(String service) {
        this.service = service;
    }

    @ManagedAttribute
    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }

    @ManagedAttribute
    public String getDaemon() {
        return daemon;
    }

    public void setDaemon(String daemon) {
        this.daemon = daemon;
    }

    @ManagedAttribute
    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    //---------------------------------------------------------------
    // Implementation of BeanNameAware interface
    //---------------------------------------------------------------

    @Override
    public void setBeanName(String name) {
        this.threadName = name;
    }

    //---------------------------------------------------------------
    // TibRv life cycle
    //---------------------------------------------------------------

    @PostConstruct
    public void init() {
        try {
            Tibrv.open(Tibrv.IMPL_NATIVE);
            TibrvTransport transport = new TibrvRvdTransport(service, network, daemon);
            TibrvQueue queue = new TibrvQueue();
            new TibrvListener(queue, this, transport, subject, null);
            new TibrvDispatcher(threadName, queue);

            log.info(String.format("TibRV listener initialized: service=%s; network=%s; daemon=%s; subject=%s",
                    service, network, daemon, subject));

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void destroy() {
        try {
            Tibrv.close();
        } catch (TibrvException e) {
            throw new RuntimeException(e);
        }
    }

    //---------------------------------------------------------------
    // Implementation of TibrvMsgCallback interface
    //---------------------------------------------------------------

    @Override
    public void onMsg(TibrvListener tibrvlistener, final TibrvMsg message) {
        if (log.isTraceEnabled()) log.trace(message);
        if (messageListener == null) {
            throw new IllegalStateException("No message listener specified - see property 'messageListener'");
        }
        if (taskExecutor != null) {
            taskExecutor.execute(new Runnable() {
                @Override public void run() {
                    messageListener.onMessage(message);
                }
            });
        } else {
            messageListener.onMessage(message);
        }
    }
}
