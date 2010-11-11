package com.ndpar.tibrv;

import com.tibco.tibrv.TibrvMsg;

public interface MessageListener {

    void onMessage(TibrvMsg message);
}
