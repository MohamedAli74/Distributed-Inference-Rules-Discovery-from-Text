import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.util.List;
import java.nio.file.Paths;

public class runaws {

    private static void waitSeconds(int seconds) {
        try { Thread.sleep(seconds * 1000L); } catch (InterruptedException ignored) {}
    }

    private static void runAwsSync(String s3Src, String localDst) throws Exception {
        Process p = new ProcessBuilder("aws", "s3", "sync", s3Src, localDst)
                .inheritIO()
                .start();
        int code = p.waitFor();
        if (code != 0) throw new RuntimeException("aws s3 sync failed, exitCode=" + code);
    }

    // unzip all *.gz under dir (WSL/Linux)
    private static void gunzipAll(String dir) throws Exception {
        String cmd = "find '" + dir + "' -type f -name '*.gz' -print0 | xargs -0 -r gunzip -f";
        Process p = new ProcessBuilder("bash", "-lc", cmd)
                .inheritIO()
                .start();
        int code = p.waitFor();
        if (code != 0) throw new RuntimeException("gunzip failed, exitCode=" + code);
    }

    private static String firstStepId(AmazonElasticMapReduce emr, String clusterId) {
        ListStepsResult ls = emr.listSteps(new ListStepsRequest().withClusterId(clusterId));
        List<StepSummary> steps = ls.getSteps();
        if (steps == null || steps.isEmpty())
            throw new RuntimeException("No steps found for cluster " + clusterId);
        return steps.get(0).getId();
    }

    private static String stepState(AmazonElasticMapReduce emr, String clusterId, String stepId) {
        DescribeStepResult ds = emr.describeStep(new DescribeStepRequest()
                .withClusterId(clusterId)
                .withStepId(stepId));
        return ds.getStep().getStatus().getState();
    }

    private static String ensureSlash(String s) {
        return s.endsWith("/") ? s : s + "/";
    }

    public static void main(String[] args) {

        // Usage:
        // runaws <jarS3> <unigramS3> <bigramS3> <outBaseS3> <reducers> [instanceCount] [useCombiner(0/1)] [localOutDir] [localLogsDir]
        //
        // Examples:
        // java -jar target/runner-1.0.jar <jar> <uni> <bi> s3://.../output/run1 2 1 1 ./download_out ./download_log
        //
        // If localOutDir/localLogsDir are not provided -> defaults inside current directory (runner):
        // ./download_out/<clusterId> , ./download_log/<clusterId>

        if (args.length < 5) {
            System.err.println("Usage: runaws <jarS3> <unigramS3> <bigramS3> <outBaseS3> <reducers> [instanceCount] [useCombiner(0/1)] [localOutDir] [localLogsDir]");
            System.exit(1);
        }

        String region      = "us-east-1";
        String keyPairName = "vockey";
        String logUri      = "s3://cfggii23/dsp2/logs/";   // EMR writes logs here

        String jarS3    = args[0];
        String unigram  = args[1];
        String bigram   = args[2];
        String outBase  = args[3];
        String reducers = args[4];

        int instanceCount  = (args.length >= 6) ? Integer.parseInt(args[5]) : 3;
        String useCombiner = (args.length >= 7) ? args[6] : "0";

        if (!useCombiner.equals("0") && !useCombiner.equals("1")) {
            System.err.println("useCombiner must be 0 or 1");
            System.exit(1);
        }

        // Default download roots: inside current working directory (runner)
        String cwd = System.getProperty("user.dir");
        String localOutRootDefault  = Paths.get(cwd, "download_out").toAbsolutePath().toString();
        String localLogsRootDefault = Paths.get(cwd, "download_log").toAbsolutePath().toString();

        // If user provided local paths as last 2 args, use them
        String localOutRoot  = (args.length >= 9) ? Paths.get(args[7]).toAbsolutePath().toString() : localOutRootDefault;
        String localLogsRoot = (args.length >= 9) ? Paths.get(args[8]).toAbsolutePath().toString() : localLogsRootDefault;

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        HadoopJarStepConfig hadoopStep = new HadoopJarStepConfig()
                .withJar(jarS3)
                .withMainClass("dsp2.Main")
                .withArgs(unigram, bigram, outBase, reducers, useCombiner);

        StepConfig step = new StepConfig()
                .withName("Run dsp2 pipeline")
                .withHadoopJarStep(hadoopStep)
                .withActionOnFailure("TERMINATE_CLUSTER");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withEc2KeyName(keyPairName)
                .withInstanceCount(instanceCount)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge");

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("dsp2-run")
                .withReleaseLabel("emr-7.12.0")
                .withApplications(new Application().withName("Hadoop"))
                .withInstances(instances)
                .withSteps(step)
                .withLogUri(logUri)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        try {
            RunJobFlowResult result = emr.runJobFlow(request);
            String clusterId = result.getJobFlowId();
            String stepId = firstStepId(emr, clusterId);

            // Local folders per cluster id (inside runner)
            String localLogsDir = Paths.get(localLogsRoot, clusterId).toString();
            String localOutDir  = Paths.get(localOutRoot,  clusterId).toString();

            System.out.println("ClusterId: " + clusterId);
            System.out.println("StepId:    " + stepId);
            System.out.println("Instances: " + instanceCount);
            System.out.println("Combiner:  " + useCombiner);
            System.out.println("OutBase:   " + outBase);
            System.out.println("LocalOut:  " + localOutDir);
            System.out.println("LocalLogs: " + localLogsDir);
            System.out.println("Waiting for step to finish...");

            while (true) {
                String state = stepState(emr, clusterId, stepId);
                System.out.println("Step state: " + state);

                if (state.equals("COMPLETED") || state.equals("FAILED") || state.equals("CANCELLED") || state.equals("INTERRUPTED"))
                    break;

                waitSeconds(30);
            }

            // Download logs always
            String logsS3 = ensureSlash(logUri) + clusterId + "/";
            System.out.println("Downloading logs to: " + localLogsDir);
            runAwsSync(logsS3, localLogsDir);

            // Unzip logs automatically
            System.out.println("Unzipping logs (.gz) ...");
            gunzipAll(localLogsDir);

            // Download output only if completed
            String state = stepState(emr, clusterId, stepId);
            if (!state.equals("COMPLETED")) {
                System.err.println("Step finished with state: " + state);
                System.err.println("Logs saved (unzipped) at: " + localLogsDir);
                System.exit(2);
            }

            String outS3 = ensureSlash(outBase) + "job4_out/";
            System.out.println("Downloading output to: " + localOutDir);
            runAwsSync(outS3, localOutDir);

            System.out.println("done");
            System.out.println("Output: " + localOutDir);
            System.out.println("Logs:   " + localLogsDir);

        } catch (Exception e) {
            System.err.println("Failed");
            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}


//how to run

   /* java -jar target/runner-1.0.jar   s3://cfggii22/jars/dsp2.jar 
  s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/1gram/data  
  
 s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data
   s3://cfggii22/dsp2/output/eng_run1 
     2  (reducers number)
      2 (number of instances)
        0 (no combiner 1 with combiner)
          ./download55_out   ./download55_log  */