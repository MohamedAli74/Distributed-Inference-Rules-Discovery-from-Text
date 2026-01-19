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
        // runaws <jarS3> <inputS3> <workDirS3> <positiveS3> <negativeS3> <reducers> [instanceCount] [localOutDir] [localLogsDir]
        //
        // Example:
        // java -jar target/runner-1.0.jar \
        //   s3://BUCKET/jars/dsp2-1.0.0.jar \
        //   s3://BUCKET/input/biarcs_small10/ \
        //   s3://BUCKET/output/run_small10/ \
        //   s3://BUCKET/test/positive-preds.txt \
        //   s3://BUCKET/test/negative-preds.txt \
        //   4 \
        //   3 \
        //   ./download_out \
        //   ./download_log

        if (args.length < 6) {
            System.err.println("Usage: runaws <jarS3> <inputS3> <workDirS3> <positiveS3> <negativeS3> <reducers> [instanceCount] [localOutDir] [localLogsDir]");
            System.exit(1);
        }

        // ====== EMR CONFIG  ======
        String region      = "us-east-1";
        String keyPairName = "vockey";
        String logUri      = "s3://cfggii23/dsp2/logs/";   // EMR writes logs here

        // ====== ARGS ======
        String jarS3      = args[0];
        String inputS3    = args[1]; // folder (small10 or full)
        String workDirS3  = args[2]; // output base dir for steps
        String positiveS3 = args[3];
        String negativeS3 = args[4];
        String reducers   = args[5];
        int instanceCount = (args.length >= 7) ? Integer.parseInt(args[6]) : 3;

        // ====== LOCAL DOWNLOAD ROOTS ======
        String cwd = System.getProperty("user.dir");
        String localOutRootDefault  = Paths.get(cwd, "download_out").toAbsolutePath().toString();
        String localLogsRootDefault = Paths.get(cwd, "download_log").toAbsolutePath().toString();

        String localOutRoot  = (args.length >= 9) ? Paths.get(args[7]).toAbsolutePath().toString() : localOutRootDefault;
        String localLogsRoot = (args.length >= 9) ? Paths.get(args[8]).toAbsolutePath().toString() : localLogsRootDefault;

        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        // ====== STEP: run your Hadoop jar ======
        HadoopJarStepConfig hadoopStep = new HadoopJarStepConfig()
                .withJar(jarS3)
                // IMPORTANT: 
                .withMainClass("com.example.DirtDriver")
                // DirtDriver: <input> <workDir> <positive> <negative> <reducers>
                .withArgs(inputS3, workDirS3, positiveS3, negativeS3, reducers);

        StepConfig step = new StepConfig()
                .withName("Run DIRT pipeline")
                .withHadoopJarStep(hadoopStep)
                .withActionOnFailure("TERMINATE_CLUSTER");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withEc2KeyName(keyPairName)
                .withInstanceCount(instanceCount)
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withMasterInstanceType("m5.xlarge")
                .withSlaveInstanceType("m5.xlarge");

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("dirt-run")
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

            String localLogsDir = Paths.get(localLogsRoot, clusterId).toString();
            String localOutDir  = Paths.get(localOutRoot,  clusterId).toString();

            System.out.println("ClusterId: " + clusterId);
            System.out.println("StepId:    " + stepId);
            System.out.println("Waiting for step to complete...");

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

            // If step not completed, exit after logs
            String finalState = stepState(emr, clusterId, stepId);
            if (!finalState.equals("COMPLETED")) {
                System.err.println("Step finished with state: " + finalState);
                System.err.println("Logs saved (unzipped) at: " + localLogsDir);
                System.exit(2);
            }

            // Download FINAL output (step7)
            String outS3 = ensureSlash(workDirS3) + "step7/";
            System.out.println("Downloading output to: " + localOutDir);
            runAwsSync(outS3, localOutDir);

            System.out.println("DONE");
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
