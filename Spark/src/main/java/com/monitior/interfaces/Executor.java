package com.monitior.interfaces;

import java.io.Serializable;

public interface Executor extends Serializable {
    void setEventame(String name);
    String getEventName();
    void setStageID(int id);
    int getStageID();
    void setExecutorID(String id);
    String getExecutorID();
    void stageAttempt(int id);
    int getStageAttempt();
    void executorTime(long Time);
    long getExecutorTime();
    void setExecutorHost(String host);
    String getExecutorHost();
    void setTotalCores(int cores);
    int getTotalCores();
    void setResourceInfo(int resourceInfoId);
    int getResourceInfo();
    void setReasonOfRemoval(String reasonOfRemoval);
    String getReasonOfRemoval();

}
