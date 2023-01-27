package org.example;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.resource.ExecutorResourceRequest;
import org.apache.spark.scheduler.*;
import org.apache.spark.scheduler.cluster.ExecutorInfo;
import org.apache.spark.storage.RDDBlockId;
import scala.Option;
import scala.collection.JavaConverters;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class SparkListener extends org.apache.spark.scheduler.SparkListener {
    long executorCPUTime=0L;
    long resultSize=0L;
    long serlizationTime=0L;
    long totalRecordRead=0L;
    long totalRecordWritter=0L;
    Map<Integer,Integer> stageMap= new HashMap<>();

    public long getSerlizationTime() {
        return serlizationTime;
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
        super.onExecutorAdded(executorAdded);
        System.out.println("Executor..."+executorAdded.executorId());
        ExecutorInfo executorInfo=executorAdded.executorInfo();

        System.out.println("ExecHost...."+executorInfo.executorHost());
        scala.collection.immutable.Map<String, String> map=executorInfo.attributes();
      // Map<String,String> map1= JavaConverters.mapAsJavaMap(map.ma);
        System.out.println(""+executorInfo.resourceProfileId());
        //executorInfo.resourcesInfo()
        System.out.println("totalCore"+executorInfo.totalCores());
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
        executorRemoved.executorId();
        //executorRemove
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
        super.onExecutorMetricsUpdate(executorMetricsUpdate);
        System.out.println(".."+executorMetricsUpdate.execId());
   //  System.exit(-1);
        Set keys = (Set) executorMetricsUpdate.executorUpdates().keySet();
        System.out.println(keys.size());
       // System.exit(-1);
        Iterator i = keys.iterator();
        //while (i.hasNext()) {
          //  System.out.println(i.next());
       // }
       // System.out.println("..");
       // executorMetricsUpdate
          //  System.exit(-1);
    }
    @Override
    public void onTaskStart(SparkListenerTaskStart taskStart) {
        super.onTaskStart(taskStart);

        //System.out.printf("ID____"+taskStart.taskInfo().id());
       // System.out.println("Host------"+taskStart.taskInfo().host());
        //System.out.println("TaskID------"+taskStart.taskInfo().taskId());
       // System.out.println("The------"+taskStart.taskInfo().id());
        //System.out.println("ExecutorID------"+taskStart.taskInfo().executorId());
        //System.out.println("TaskLocality------"+taskStart.taskInfo().taskLocality().toString());
        //System.out.println("status------"+taskStart.taskInfo().status());
        //System.out.println("The------"+taskStart.taskInfo().duration());
        //System.out.println("AttampNo------"+taskStart.taskInfo().attemptNumber());
       // System.out.println("Finish------"+taskStart.taskInfo().finished());
        //System.out.println("FinishTime------"+taskStart.taskInfo().finishTime());
       // System.out.println("PartitionID------"+taskStart.taskInfo().partitionId());
        //System.out.println("GettingResult------"+taskStart.taskInfo().gettingResult());
       // System.out.println("GettingResultTime------"+taskStart.taskInfo().gettingResultTime());
       // System.out.println("TaskInfoIndex------"+taskStart.taskInfo().index());
       // System.out.println("LaunchTime------"+taskStart.taskInfo().launchTime());
       // System.out.println("ExecutorID------"+taskStart.taskInfo().executorId());
       // System.out.println("RunningTimeTillNow------"+taskStart.taskInfo().timeRunning(System.currentTimeMillis()));
       // System.out.println("Failed------"+taskStart.taskInfo().failed());
       // System.out.println("Killed------"+taskStart.taskInfo().killed());
       // System.out.println("TheSuccessfull------"+taskStart.taskInfo().successful());
        //System.out.println("Running------"+taskStart.taskInfo().running());
        //System.out.println("TheStageID------"+taskStart.stageId());

        // System.exit(-1);
    }
    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
        super.onBlockUpdated(blockUpdated);
        //System.out.println("BLOCK ID"+blockUpdated.blockUpdatedInfo().blockId());
       // System.out.println(blockUpdated.blockUpdatedInfo().blockManagerId().host());
       // System.out.println(blockUpdated.blockUpdatedInfo().blockManagerId().topologyInfo().toString());
       // System.out.println(blockUpdated.blockUpdatedInfo().blockId().name());
       // System.out.println(blockUpdated.blockUpdatedInfo().blockId().isRDD());
       // System.out.println(blockUpdated.blockUpdatedInfo().blockId().asRDDId());
        //Option<RDDBlockId> optionRDDBlockId=blockUpdated.blockUpdatedInfo().blockId().asRDDId();
        //optionRDDBlockId.get().name();
        //System.exit(-1);

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
        //System.out.println("........."+getSerlizationTime());
       // System.exit(-1);
       // System.out.println(applicationStart.appName());
        //System.out.println("ApplicationName-------------------"+applicationStart.appName());

        //System.out.println("AttemptID......................."+applicationStart.appAttemptId());
       // System.out.println("ApplicationStartTime......................"+applicationStart.time());
       // System.out.println("Application ID.........."+applicationStart.appId());
       // System.out.println("ProductPredix............"+applicationStart.productPrefix());
       // System.out.println("SparkUser......"+applicationStart.sparkUser());
        //System.out.println("Driver Attribute----"+applicationStart.driverAttributes().iterator());

       // System.out.println(applicationStart.appAttemptId().toString());
        //System.out.println(applicationStart.appId().toString());
        //System.exit(-1);
    }




    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        super.onJobStart(jobStart);
        //System.out.println("JobID---"+jobStart.jobId()+"................JobTime.."+jobStart.time());
       // System.out.println("Product Arity-------"+jobStart.productArity());
        //System.out.println("StageId-------"+jobStart.stageIds().distinct()+"---"+"Stage Info..."+jobStart.stageInfos().distinct().toString());
       // System.out.println(".."+jobStart.);

       // if(jobStart.jobId()==1)
     //
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {


       // System.out.println("Job--");
        //Iterator iterator= (Iterator) jobEnd.productIterator();
        //while(iterator.hasNext()){

        //}
        super.onJobEnd(jobEnd);

        //System.out.println("JobId----"+jobEnd.jobId()+"jobEnd..."+jobEnd.time()+"......JobResult.."+jobEnd.jobResult().toString());
        //obEnd.copy(jobEnd.jobId(),jobEnd.time(),jobEnd.jobResult());
       // System.exit(-1);

    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        super.onTaskEnd(taskEnd);
        TaskMetrics taskMetrics= taskEnd.taskMetrics();
       // System.out.println("The------"+taskEnd.taskInfo().host());
       // System.out.println("The------"+taskEnd.taskInfo().taskId());
       // System.out.println("The------"+taskEnd.taskInfo().id());
        //System.out.println("The------"+taskEnd.taskInfo().executorId());
        //System.out.println("The------"+taskEnd.taskInfo().taskLocality().toString());
       // System.out.println("The------"+taskEnd.taskInfo().status());
       // System.out.println("The------"+taskEnd.taskInfo().duration());
       // System.out.println("The------"+taskEnd.taskInfo().attemptNumber());
       // System.out.println("The------"+taskEnd.taskInfo().finished());
       // System.out.println("The------"+taskEnd.taskInfo().finishTime());
       // System.out.println("The------"+taskEnd.taskInfo().partitionId());
       // System.out.println("The------"+taskEnd.taskInfo().gettingResult());
       // System.out.println("The------"+taskEnd.taskInfo().gettingResultTime());
       // System.out.println("The------"+taskEnd.taskInfo().index());
       // System.out.println("The------"+taskEnd.taskInfo().launchTime());
       // System.out.println("The------"+taskEnd.taskInfo().executorId());
        //System.out.println("The------"+taskEnd.taskInfo().timeRunning(System.currentTimeMillis()));
       // System.out.println("The------"+taskEnd.taskInfo().failed());
        //System.out.println("The------"+taskEnd.taskInfo().killed());
       // System.out.println("The------"+taskEnd.taskInfo().successful());
       // System.out.println("The------"+taskEnd.taskInfo().running());
       // System.out.println("The------"+taskEnd.stageId());

       executorCPUTime= taskMetrics.executorCpuTime();
        resultSize=taskMetrics.resultSize();
        serlizationTime=taskMetrics.resultSerializationTime();
        totalRecordRead =taskMetrics.inputMetrics().recordsRead();
        totalRecordWritter=taskMetrics.outputMetrics().recordsWritten();

       // System.out.println("ExecutorCPUTime"+ taskMetrics.executorCpuTime());
       // System.out.println("RecordsCPU"+taskMetrics.inputMetrics().recordsRead());
        //System.out.println("DiskBytesSpilled"+taskMetrics.diskBytesSpilled());
       // System.out.println("ExecutorDeSerlizationCPUTime"+taskMetrics.executorDeserializeCpuTime());
       // System.out.println("ExecutorDeserlizeTime"+taskMetrics.executorDeserializeTime());
       // System.out.println(".."+taskMetrics.diskBytesSpilled());
       // System.out.println(".."+taskMetrics.diskBytesSpilled());
       // System.out.println("ExecutorRunTime"+taskMetrics.executorRunTime());
       // System.out.println("JVMGCTime"+taskMetrics.jvmGCTime());
       // System.out.println(".."+taskMetrics.peakExecutionMemory());
       // System.out.println(".."+taskMetrics.resultSize());
       // System.out.println(".."+taskMetrics.resultSerializationTime());
       // System.out.println(".."+taskMetrics.outputMetrics().recordsWritten());
       // System.out.println(".."+taskMetrics.outputMetrics().bytesWritten());
        //System.out.println("TaskType"+taskEnd.taskType());
            System.out.println("--------------"+executorCPUTime);
           System.out.println(",,,,,,,,,,,,,,,,,,,"+totalRecordWritter);
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);
        int tasks= stageCompleted.stageInfo().numTasks();
        stageCompleted.stageInfo().numTasks();
        int stageId=stageCompleted.stageInfo().stageId();
        System.out.println(stageCompleted.stageInfo().name());
        System.out.println(stageCompleted.stageInfo().numTasks());
        System.out.println(stageCompleted.stageInfo().completionTime());
        System.out.println("StageExecutorCPU...."+stageCompleted.stageInfo().taskMetrics().executorCpuTime());
        System.out.println("stageExecutorDeserlization.."+stageCompleted.stageInfo().taskMetrics().executorDeserializeTime());
        System.out.println(stageCompleted.stageInfo().details());
        System.out.println(stageCompleted.stageInfo().getStatusString());
        System.out.println(stageCompleted.stageInfo().taskMetrics().executorRunTime());
        stageMap.put(tasks,stageId);

       System.out.println("----"+stageMap.size()+"---------------"+tasks+"----------------------"+stageId);
       //System.exit(-1);
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
        super.onStageSubmitted(stageSubmitted);
        System.out.println("StageInformationDetails"+stageSubmitted.stageInfo().details());
        System.out.println(".."+stageSubmitted.stageInfo().getStatusString());
        System.out.println(".."+stageSubmitted.stageInfo().name());
        System.out.println("StageNumberOfTasks...."+stageSubmitted.stageInfo().numTasks());
        System.out.println("Stage ID"+stageSubmitted.stageInfo().stageId());
        System.out.println("TaskMetricsExecutorCPU"+stageSubmitted.stageInfo().taskMetrics().executorCpuTime());
        System.out.println("TaskMetricsRecordRead"+stageSubmitted.stageInfo().taskMetrics().inputMetrics().recordsRead());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().inputMetrics().bytesRead());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().executorDeserializeCpuTime());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().executorDeserializeTime());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().diskBytesSpilled());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().diskBytesSpilled());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().executorRunTime());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().jvmGCTime());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().peakExecutionMemory());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().resultSize());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().resultSerializationTime());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().outputMetrics().recordsWritten());
        System.out.println(".."+stageSubmitted.stageInfo().taskMetrics().outputMetrics().bytesWritten());
     //   System.exit(-1);
    }

    @Override
    public void onStageExecutorMetrics(SparkListenerStageExecutorMetrics executorMetrics) {
        super.onStageExecutorMetrics(executorMetrics);
        System.out.println(executorMetrics.stageId());
        System.out.println(executorMetrics.execId());
        System.out.println(executorMetrics.stageAttemptId());
        System.out.println(executorMetrics.toString());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        super.onApplicationEnd(applicationEnd);
        Long time= applicationEnd.time();
        System.out.println(applicationEnd.logEvent());
        System.out.println(applicationEnd.productPrefix());

        System.out.printf("ApplicationExecutorTime : %d",executorCPUTime);
        System.out.printf("TotalResultSize : %d",resultSize);
        System.out.printf("TotalSerlizationTime : %d",serlizationTime);
        System.out.printf("TotalRecordRead : %d",totalRecordRead);
        System.out.printf("TotalRecordWrite : %d",totalRecordWritter);

        for (int id:stageMap.keySet()){
          //  System.out.printf("stageid : %d -tasks : %d ",id,stageMap.get(id));
       }
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
        System.out.println("The------"+taskGettingResult.taskInfo().host());
        System.out.println("The------"+taskGettingResult.taskInfo().taskId());
        System.out.println("The------"+taskGettingResult.taskInfo().id());
        System.out.println("The------"+taskGettingResult.taskInfo().executorId());
        System.out.println("The------"+taskGettingResult.taskInfo().taskLocality().toString());
        System.out.println("The------"+taskGettingResult.taskInfo().status());
        System.out.println("The------"+taskGettingResult.taskInfo().duration());
        System.out.println("The------"+taskGettingResult.taskInfo().attemptNumber());
        System.out.println("The------"+taskGettingResult.taskInfo().finished());
        System.out.println("The------"+taskGettingResult.taskInfo().finishTime());
        System.out.println("The------"+taskGettingResult.taskInfo().partitionId());
        System.out.println("The------"+taskGettingResult.taskInfo().gettingResult());
        System.out.println("The------"+taskGettingResult.taskInfo().gettingResultTime());
        System.out.println("The------"+taskGettingResult.taskInfo().index());
        System.out.println("The------"+taskGettingResult.taskInfo().launchTime());
        System.out.println("The------"+taskGettingResult.taskInfo().executorId());
        System.out.println("The------"+taskGettingResult.taskInfo().timeRunning(System.currentTimeMillis()));
        System.out.println("The------"+taskGettingResult.taskInfo().failed());
        System.out.println("The------"+taskGettingResult.taskInfo().killed());
        System.out.println("The------"+taskGettingResult.taskInfo().successful());
        System.out.println("The------"+taskGettingResult.taskInfo().running());
        System.out.println("The------"+taskGettingResult.toString());
       // taskGettingResult.

    }

    }




