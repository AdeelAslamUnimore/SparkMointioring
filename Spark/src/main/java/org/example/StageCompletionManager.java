package org.example;

import org.apache.spark.scheduler.SparkListenerStageCompleted;

public class StageCompletionManager extends SparkListener{
    @Override
    public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
        super.onStageCompleted(stageCompleted);
    }
}
