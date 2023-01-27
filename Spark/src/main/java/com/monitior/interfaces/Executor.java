package com.monitior.interfaces;

public interface Executor {
    void setStageID(int id);
    int getStageID();
    void setExecutorID(int id);
    int getExecutorID();
    void stageAttempt(int id);
    int getStageAttempt();

}
