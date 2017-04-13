package com.sina.sdptools.hadoop.job.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TIPStatus;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobManager {

  private Configuration conf = new Configuration();

  private JobClient jobClient;

  private Logger LOG = LoggerFactory.getLogger(getClass().getSimpleName());

  public JobManager(String hostname, String port) {
    conf.set("mapred.job.tracker", hostname + ":" + port);
    try {
      jobClient = new JobClient(new JobConf(conf));
    }
    catch (IOException e) {
      LOG.error("create jobClient error:" + e);
    }
  }

  public RunningJob getJobById(String jobId) {
    try {

      return jobClient.getJob(JobID.forName(jobId));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public ClusterStatus getClusterStatus() throws IOException {
    return jobClient.getClusterStatus(true);
  }

  public List<String> listOvertimeJobs(long overtime) throws IOException {
    ArrayList<String> result = new ArrayList<String>();
    JobStatus[] jobStatus = jobClient.getAllJobs();
    for (JobStatus js : jobStatus) {
      if (js.getRunState() == JobStatus.RUNNING) {
        long startTime = js.getStartTime();
        long currentTime = System.currentTimeMillis();
        long exceuteTime = currentTime - startTime;
        String jobId = js.getJobID().toString();
        LOG.info("Job:" + jobId + ",exceuteTime:" + exceuteTime / 1000 / 60 + " minutes.");
        if (exceuteTime > overtime) {
          result.add(jobId.toString());
          LOG.info("Job:" + jobId + ",add to overtime list.");
        }
      }
    }
    return result;
  }

  public void killMapTask(String jobId, int factor) throws IOException {
    RunningJob runningJob = getJobById(jobId);
    TaskReport[] reports = jobClient.getMapTaskReports(runningJob.getID());
    long averageTime = getTaskAverageTime(reports);
    LOG.info("Map task average time is:" + averageTime / 1000 / 60 + " minute, for " + jobId);
    killTaskByReport(runningJob, reports, averageTime * factor,false);
  }

  public void killReduceTask(String jobId, int factor) throws IOException {
    RunningJob runningJob = getJobById(jobId);
    TaskReport[] reports = jobClient.getReduceTaskReports(runningJob.getID());
    long averageTime = getTaskAverageTime(reports);
    LOG.info("Reduce task average time is:" + averageTime / 1000 / 60 + " minute, for " + jobId);
    killTaskByReport(runningJob, reports, averageTime * factor,true);
  }

  private void killTaskByReport(RunningJob runningJob, TaskReport[] reports, long overtime,boolean isReduceTask) throws IOException {
    for (TaskReport report : reports) {
      if (report.getCurrentStatus() == TIPStatus.RUNNING) {
        long currentTime = System.currentTimeMillis();
        long startTime = report.getStartTime();
        long exceuteTime = currentTime - startTime;
        float progress_1 = report.getProgress();
        String status = report.getState();
        if ((overtime > 0) && (exceuteTime > overtime)) {
          //System.out.println(progress_1);
          //System.out.println("status:"+status);
          try {
            Thread.sleep(1500*10);
          }
          catch (InterruptedException e) {
          }
          TaskReport[] reports2 = null;
          if(isReduceTask){
            reports2 = jobClient.getReduceTaskReports(runningJob.getID());
          }
          else{
            reports2 = jobClient.getMapTaskReports(runningJob.getID());
          }
          float progress_2 = 0;
          String status2 = "";
          for (TaskReport report2 : reports2) {
            if(report2.getTaskID().equals(report.getTaskID())){
              progress_2 = report2.getProgress();
              status2 = report2.getState();
            }
          }
          //System.out.println(progress_2);
          //System.out.println("status2:"+status2);
          if(progress_2 == progress_1 && status.equals(status2)){
            for (TaskAttemptID t : report.getRunningTaskAttempts()) {
              runningJob.killTask(t, false);
              LOG.warn("Task:" + t.toString() + " exceute " + exceuteTime / 1000 / 60 + " minute, so been killed.");
            }
          }
        }
      }
    }
  }

  private long getTaskAverageTime(TaskReport[] reports) {
    long taskExceuteTime = 0;
    int count = 0;
    long result = 0;
    for (TaskReport report : reports) {
      if (report.getCurrentStatus() == TIPStatus.COMPLETE) {
        long startTime = report.getStartTime();
        long endTime = report.getFinishTime();
        long excuteTime = endTime - startTime;
        taskExceuteTime = taskExceuteTime + excuteTime;
        count++;
      }
    }
    if (count != 0) {
      result = taskExceuteTime / count;
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    String ip = "10.13.0.163";
    //String ip="10.77.112.96";
    String port = "9001";
    long overtime = 60 * 60 * 1000;
    int factor = 3;
    JobManager jm = new JobManager(ip, port);
    List<String> oj = jm.listOvertimeJobs(overtime);
    for (String jobid : oj) {
      jm.killMapTask(jobid, factor);
      jm.killReduceTask(jobid, factor);
    }
  }
}
