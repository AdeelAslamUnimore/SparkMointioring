package org.example;

import com.monitior.interfaces.*;
import com.monitior.interfaces.Stage;
import com.monitior.interfaces.Task;
import com.monitior.spark.*;
import com.monitior.spark.ExecutorAdded;
import com.monitior.spark.TaskMetric;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.executor.ExecutorMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ExecutorResourceRequest;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.apache.spark.storage.RDDBlockId;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.immutable.Seq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class SparkListener extends org.apache.spark.scheduler.SparkListener {
    private Application applicationStart;
    private Application applicationEnd;
    private Job jobStart;
    private Job jobEnd;
    private Stage stageSubmitted;
    private Stage stageCompleted;
    private Stage stageExecutorMetrics;
    private Task taskStart;
    private Task taskGettingResult;
    private Task taskEnd;
    private Executor executorAdded;
    private Executor executorRemoved;
    private Executor executorUpdated;
    private List<SerializableObject> objects;
   private  KafkaProducer<String,  byte[]> producer;
   private String kafkaTopic;
    public SparkListener(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
       objects= new ArrayList<>();
       this.kafkaTopic="Topic";
    }


    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
        this.executorAdded.setEventame("ExecutorAdded");
        this.executorAdded= new ExecutorAdded();
        this.executorAdded.setExecutorID(executorAdded.executorId());
        ExecutorInfo executorInfo=executorAdded.executorInfo();
        this.executorAdded.setExecutorHost(executorInfo.executorHost());
        this.executorAdded.setTotalCores(executorInfo.totalCores());
        this.executorAdded.setResourceInfo(executorInfo.resourceProfileId());
        this.executorAdded.setExecutorHost(executorInfo.executorHost());
        this.executorAdded.executorTime(executorAdded.time());
        this.objects.add((SerializableObject) this.executorAdded);

        //On Executor is added to
        //executorInfo.resourcesInfo();

      //  System.exit(-1);
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
        super.onExecutorBlacklisted(executorBlacklisted);

        executorBlacklisted.executorId();
        executorBlacklisted.time();
      //  executorBlacklisted.
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
        super.onExecutorRemoved(executorRemoved);
        this.executorRemoved.setEventame("ExecutorRemoved");
        this.executorRemoved= new ExecutorRemoved();
        this.executorRemoved.setExecutorHost(executorRemoved.executorId());
        this.executorRemoved.setReasonOfRemoval(executorRemoved.reason());
        this.executorRemoved.executorTime(executorRemoved.time());
        this.objects.add((SerializableObject) this.executorRemoved);

        //executorRemove
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
        this.executorUpdated= new ExecutorUpdated();
        this.executorUpdated.setExecutorID(executorMetricsUpdate.execId());
        this.executorUpdated.setEventame("ExecutorUpdated");
       this.objects.add((SerializableObject) this.executorUpdated);

        }



    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        super.onTaskStart(taskStart);
        this.taskStart=new TaskStart();
        TaskInfo taskInfo= taskStart.taskInfo();
        this.taskStart.setID(taskInfo.id());
        this.taskStart.setEventame("OnTaskStart");
        this.taskStart.setHostIP(taskInfo.host());
        this.taskStart.setStringExecutorID(taskInfo.executorId());
        this.taskStart.setTaskStatus(taskInfo.status());
        this.taskStart.setTaskID(taskInfo.taskId());

        this.taskStart.setIndex(taskInfo.index());
        this.taskStart.setLaunchTime(taskInfo.launchTime());

        this.taskStart.setFinishTime(taskInfo.finishTime());
        this.taskStart.setDurationTime(taskInfo.duration());
        this.taskStart.setGettingTime(taskInfo.gettingResultTime());
       this. taskStart.setStageID(taskStart.stageId());
       this.taskStart.setPartition(taskInfo.partitionId());
       if(taskInfo.failed()){
        this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
       }
       else if(taskInfo.finished()){
           this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
       else if(taskInfo.killed()){
           this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
       }
       else  if(taskInfo.running()){
           this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
       }
       else if(taskInfo.successful()){
           this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
       }
       else if(taskInfo.speculative()){
           this.taskStart.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
       }
       else {
           this.taskStart.setTaskStatusForRunning(null);
       }
        this.objects.add((SerializableObject) this.taskStart);



    }
    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);

    }

    @Override
    public void onNodeExcludedForStage(SparkListenerNodeExcludedForStage nodeExcludedForStage) {
        super.onNodeExcludedForStage(nodeExcludedForStage);
        nodeExcludedForStage.stageId();
        nodeExcludedForStage.time();
        nodeExcludedForStage.stageAttemptId();
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        super.onApplicationStart(applicationStart);
        this.applicationStart= new ApplicationStart();
        this.applicationStart.setAppID(applicationStart.appId().get());
        this.applicationStart.setEventame("ApplicationStart");
        this.applicationStart.setName(applicationStart.appName());
        this.applicationStart.setSparkUser(applicationStart.sparkUser());
        this.applicationStart.setStartTime(applicationStart.time());
        this.objects.add((SerializableObject) this.applicationStart);

    }
    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        super.onJobStart(jobStart);
        this.jobStart= new JobStart();
        this.jobStart.setEventame("JobStart");
        this.jobStart.setJobID(jobStart.jobId());
        this.jobStart.setProductArity(jobStart.productArity());
        this.jobStart.setStageID((Seq<Object>) jobStart.stageIds());
        this.objects.add((SerializableObject) this.jobStart);

    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
        super.onJobEnd(jobEnd);
        this.jobEnd= new JobEnd();
        this.jobEnd.setJobID(jobEnd.jobId());
        this.jobEnd.setEventame("JobEnd");
        this.jobEnd.setProductArity(jobEnd.productArity());
        this.objects.add((SerializableObject) this.jobEnd);


    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        super.onTaskEnd(taskEnd);

        this.taskEnd=new TaskStart();
        TaskInfo taskInfo= taskEnd.taskInfo();
        this.taskEnd.setID(taskInfo.id());
        this.taskEnd.setEventame("OnTaskGettingResult");
        this.taskEnd.setHostIP(taskInfo.host());
        this.taskEnd.setStringExecutorID(taskInfo.executorId());
        this.taskEnd.setTaskStatus(taskInfo.status());
        this.taskEnd.setTaskID(taskInfo.taskId());

        this.taskEnd.setIndex(taskInfo.index());
        this.taskEnd.setLaunchTime(taskInfo.launchTime());

        this.taskEnd.setFinishTime(taskInfo.finishTime());
        this.taskEnd.setDurationTime(taskInfo.duration());
        this.taskEnd.setGettingTime(taskInfo.gettingResultTime());
        //  this. taskGettingResult.setStageID(taskGettingResult.stageId());
        this.taskEnd.setPartition(taskInfo.partitionId());
        if(taskInfo.failed()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
        }
        else if(taskInfo.finished()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
        else if(taskInfo.killed()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
        }
        else  if(taskInfo.running()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
        }
        else if(taskInfo.successful()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
        }
        else if(taskInfo.speculative()){
            this.taskEnd.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
        }
        else {
            this.taskEnd.setTaskStatusForRunning(null);
        }


        TaskMetrics taskMetrics= taskEnd.taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        this.taskEnd.setTaskMetric(taskMetric);
        this.objects.add((SerializableObject) this.taskEnd);

    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);

        this.stageCompleted= new StageSubmitted();
        StageInfo stageInfo=stageCompleted.stageInfo();
        this.stageCompleted.setDetails(stageInfo.details());
        this.stageCompleted.setEventame("OnStageSubmitted");
        this.stageCompleted.setStageName(stageInfo.name());
        this.stageCompleted.setStatus(stageInfo.getStatusString());
        this.stageCompleted.setNumberOfTasks(stageInfo.numTasks());
        this.stageCompleted.setID(stageInfo.stageId());
        this.stageCompleted.setSubmissionTime((Long) stageInfo.submissionTime().get());
        //this.stageSubmitted.setCompletionTime((Long) stageInfo.completionTime().get());
        TaskMetrics taskMetrics= stageCompleted.stageInfo().taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        this.stageCompleted.setTaskMetric(taskMetric);
        this.objects.add((SerializableObject) this.stageCompleted);

        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(objects);
            producer.send(new ProducerRecord(kafkaTopic, "Objects", baos.toByteArray()));
            objects= new ArrayList<>();
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        super.onStageSubmitted(stageSubmitted);

        this.stageSubmitted= new StageSubmitted();
        StageInfo stageInfo=stageSubmitted.stageInfo();
        this.stageSubmitted.setDetails(stageInfo.details());
       this.stageSubmitted.setEventame("OnStageSubmitted");
       this.stageSubmitted.setStageName(stageInfo.name());
       this.stageSubmitted.setStatus(stageInfo.getStatusString());
       this.stageSubmitted.setNumberOfTasks(stageInfo.numTasks());
       this.stageSubmitted.setID(stageInfo.stageId());
       this.stageSubmitted.setSubmissionTime((Long) stageInfo.submissionTime().get());
       //this.stageSubmitted.setCompletionTime((Long) stageInfo.completionTime().get());
        TaskMetrics taskMetrics= stageSubmitted.stageInfo().taskMetrics();
        TaskMetric taskMetric= new TaskMetric();
        taskMetric.setExecutorCPUTime(taskMetrics.executorCpuTime());
        taskMetric.setExecutorDeserializeCpuTime(taskMetrics.executorDeserializeCpuTime());
        taskMetric.setExecutorDeserializeTime(taskMetrics.executorDeserializeTime());
        taskMetric.setDiskBytesSpilled(taskMetrics.diskBytesSpilled());
        taskMetric.setExecutorRunTime(taskMetrics.executorRunTime());
        taskMetric.setjvmGCTime(taskMetrics.jvmGCTime());
        taskMetric.setPeakExecutionMemory(taskMetrics.peakExecutionMemory());
        taskMetric.setResultSize(taskMetrics.resultSize());
        taskMetric.setResultSerializationTime(taskMetrics.resultSerializationTime());
        this.stageSubmitted.setTaskMetric(taskMetric);
        this.objects.add((SerializableObject) this.stageSubmitted);


     //   System.exit(-1);
    }

    @Override
    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
        super.onStageExecutorMetrics(executorMetrics);

        this.stageExecutorMetrics= new StageExecutorMetrics();
        this.stageExecutorMetrics.setID(executorMetrics.stageId());
        this.stageExecutorMetrics.setEventame("StageExecutorMetric");
        this.stageExecutorMetrics.setExecutorID(executorMetrics.execId());
        this.stageExecutorMetrics.setStageAttemptId(executorMetrics.stageAttemptId());
        this.objects.add((SerializableObject) this.stageSubmitted);
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        super.onApplicationEnd(applicationEnd);
        this.applicationEnd=new ApplicationEnd();
        this.applicationEnd.setStartTime(applicationEnd.time());
        this.objects.add((SerializableObject) this.applicationEnd);

    }

    @Override
    public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
        super.onResourceProfileAdded(event);
        System.out.println("Event ID"+event.resourceProfile().id());

       System.out.println("EventResourceProile..........." +event.resourceProfile().getExecutorCores());

        ///System.out.println(event.resourceProfile().executorResources());
//Map<String, ExecutorResourceRequest> executorResources= (Map<String, ExecutorResourceRequest>) event.resourceProfile().executorResources();
//for(String k: event.resourceProfile().executorResources().keySet()){
  //  ExecutorResourceRequest executorResourceRequest= executorResources.get(k);
    //System.out.println(executorResourceRequest.amount());
   // System.out.println(executorResourceRequest.resourceName());
    //System.out.println(executorResourceRequest.resourceName());
    //System.out.println(executorResourceRequest.discoveryScript());
   // System.out.println(executorResourceRequest.vendor());
   // System.exit(-1);

}
    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
        super.onTaskGettingResult(taskGettingResult);



        this.taskGettingResult=new TaskStart();
        TaskInfo taskInfo= taskGettingResult.taskInfo();
        this.taskGettingResult.setID(taskInfo.id());
        this.taskGettingResult.setEventame("OnTaskGettingResult");
        this.taskGettingResult.setHostIP(taskInfo.host());
        this.taskGettingResult.setStringExecutorID(taskInfo.executorId());
        this.taskGettingResult.setTaskStatus(taskInfo.status());
        this.taskGettingResult.setTaskID(taskInfo.taskId());

        this.taskGettingResult.setIndex(taskInfo.index());
        this.taskGettingResult.setLaunchTime(taskInfo.launchTime());

        this.taskGettingResult.setFinishTime(taskInfo.finishTime());
        this.taskGettingResult.setDurationTime(taskInfo.duration());
        this.taskGettingResult.setGettingTime(taskInfo.gettingResultTime());
      //  this. taskGettingResult.setStageID(taskGettingResult.stageId());
        this.taskGettingResult.setPartition(taskInfo.partitionId());
        if(taskInfo.failed()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.FAILED);
        }
        else if(taskInfo.finished()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.FINISHED);
        }
        else if(taskInfo.killed()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.KILLED);
        }
        else  if(taskInfo.running()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.RUNNING);
        }
        else if(taskInfo.successful()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.SUCCESSFUL);
        }
        else if(taskInfo.speculative()){
            this.taskGettingResult.setTaskStatusForRunning(Task.TaskStatusForRunning.SPECULATIVE);
        }
        else {
            this.taskGettingResult.setTaskStatusForRunning(null);
        }
        this.objects.add((SerializableObject) this.taskGettingResult);
       // taskGettingResult.

    }

    }




