package com.monitior.interfaces;

import com.monitior.spark.TaskMetric;

import java.io.Serializable;

public interface Stage extends Serializable {

    void setEventame(String name);
    String getEventName();
    void setID(int ID);
    int getID();
    void setNumberOfTasks(int tasks);
    int getNumberOfTasks();
    void setStageName(String name);
    String getStageName();
    void setStatus(String Status);
    String getStatus();
    void setDetails(String details);
    String getDetails();
    void  setSubmissionTime(long time);
    long getSubmissionTime();
    void setCompletionTime(long time);
    long getCompletionTime();
     com.monitior.spark.TaskMetric getTaskMetric();

     void setTaskMetric(TaskMetric taskMetric);
    void setExecutorID(String ID);
    String getExecutorID();
    void setStageAttemptId(int id);
     int getStageAttemptId();

}
