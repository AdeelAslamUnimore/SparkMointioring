package com.monitior.interfaces;

import java.io.Serializable;

public interface Application extends Serializable {
   void setEventame(String name);
   String getEventName();
   void setName(String name);
   String getName();
   void setStartTime(long time);
   long getTime();
   void setAppID(String id);
   String getAppID();
   void setSparkUser(String user);
   String getSparkUser();




}
