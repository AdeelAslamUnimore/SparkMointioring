package com.monitior.interfaces;

public interface Task {
    void setID(int id);
    int getID();
    void setHostIP(String Ip);
    String getHostIP();
    void setTaskID(int taskId);
      int getTaskID();
      void setStringExecutorID();
      String getExecutorID();
      void setTaskStatus(String status);
      String getTaskStatus();
      void setIndex(int index);
      int getIndex();
      void setPartition(int partition);
      int getPartition();
      void setLaunchTime(long time);
      long getLaunchTime();
      void setRunningTime(long time);
      long getRunningTime();
      void setFailedStatus(boolean status);
      boolean getFailedStatus();


}
