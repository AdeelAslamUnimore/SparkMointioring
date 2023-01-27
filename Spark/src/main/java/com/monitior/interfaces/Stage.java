package com.monitior.interfaces;

public interface Stage {
    void setID(int ID);
    int getID();
    void setNumberOfTasks(int tasks);
    String getNumberOfTasks();
    void setStageName(String name);
    String getStageName();
    void setStatus(String Status);
    String getStatus();
    void setDetails(String details);
    String getDetails();
}
