package com.monitior.interfaces;

import scala.Serializable;
import scala.collection.immutable.Seq;

public interface Job extends Serializable {
    void setEventame(String name);
    String getEventName();
    void setJobID(int jobID);
    int getJobID();
    void setProductArity(int productArity);
    int getProductArity();
    void setStageID(Seq<Object> stageId );
    Seq<Object> getStageID();

}
