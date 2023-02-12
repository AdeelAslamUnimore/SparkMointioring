package com.monitior.interfaces;

import com.monitior.spark.TaskMetric;

import java.io.Serializable;

public interface Task extends Serializable {
    public enum TaskStatusForRunning {
        FAILED, SUCCESS, KILLED, SUCCESSFUL,RUNNING,FINISHED, SPECULATIVE;
    }
    void setEventame(String name);
    String getEventName();
    void setID(String id);
    String getID();
    void setHostIP(String Ip);
    String getHostIP();
    void setTaskID(long taskId);
    void setStageID(int id);
    int getStageID();
      long getTaskID();
      void setStringExecutorID(String executorID);
      String getExecutorID();
      void setTaskStatus(String status);
      String getTaskStatus();
      void setIndex(int index);
      int getIndex();
      void setPartition(int partition);
      int getPartition();
      void setLaunchTime(long time);
      long getLaunchTime();
    void setFinishTime(long time);
    long getFinishTime();
    void setGettingTime(long time);
    long getGettingTime();
      void setDurationTime(long time);
      long getDurationTime();
      void setTaskStatus(boolean status);
      boolean getTaskSatus();
        void setTaskStatusForRunning(TaskStatusForRunning taskStatusForRunning);
    TaskStatusForRunning getTaskStatusForRunning();
    com.monitior.spark.TaskMetric getTaskMetric();

    void setTaskMetric(TaskMetric taskMetric);
    // void setTaskRunningStatus(boolean status)


}
