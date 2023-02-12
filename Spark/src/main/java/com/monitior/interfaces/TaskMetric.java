package com.monitior.interfaces;

import java.io.Serializable;

public interface TaskMetric extends Serializable {
    void setBytesRead(long bytesRead);
    long getByteRead();
    void setExecutorDeserializeCpuTime(long executorDeserializeCpuTime );
    long getExecutorDeserializeCpuTime();
    void  setExecutorDeserializeTime(long executorDeserializeTime);
     long getExecutorDeserializeTime();
     void setDiskBytesSpilled(long DiskByteSpilled);
     long getDiskBytesSpilled();
     void setExecutorRunTime(long time);
     long getexecutorRunTime();
     void setjvmGCTime(long time);
     long getJVMGCTime();
     void setPeakExecutionMemory(long peakExecutionMemory);
     long getPeakExecutionMemory();
     void setResultSize(long resultSize);
     long getResultSize();
     void  setResultSerializationTime(long resultSerializationTime);
     long getResultSerializationTime();
     void setRecordsWritten(long recordsWritten);
     long getRecordsWrittern();
     void setBytesWritten(long bytesWritten);
     long getBytesWrittern ();
}
