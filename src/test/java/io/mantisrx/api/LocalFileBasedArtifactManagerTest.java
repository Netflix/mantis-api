package io.mantisrx.api;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.mantisrx.api.handlers.domain.Artifact;
import org.junit.Test;


public class LocalFileBasedArtifactManagerTest {
    private static final String ROOT_DIR = "/tmp";
    @Test
    public void writeAndReadTest() {
        String baseDirStr = ROOT_DIR + "/writeAndReadTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if(Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);
            LocalFileBasedArtifactManager artifactManager = new LocalFileBasedArtifactManager(
                    baseDirStr + "/",true);
            String data = "{\n"
                    + "  \"jobInfo\": {\n"
                    + "    \"name\": \"PushRequestEventSourceJob\",\n"
                    + "    \"description\": \"Fetches request events from any source in a distributed manner. "
                    + "The output is served via HTTP server using SSE protocol.\",\n"
                    + "    \"numberOfStages\": 1,\n"
                    + "    \"parameterInfo\": {\n"
                    + "      \"bufferDurationMillis\": {\n"
                    + "        \"name\": \"bufferDurationMillis\",\n"
                    + "        \"description\": \"millis to buffer events before processing\",\n"
                    + "        \"defaultValue\": \"250\",\n"
                    + "        \"validatorDescription\": \"range >=100<=1000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"mantis.jobmaster.autoscale.metric\": {\n"
                    + "        \"name\": \"mantis.jobmaster.autoscale.metric\",\n"
                    + "        \"description\": \"Custom autoscale metric for Job Master to use with UserDefined "
                    + "Scaling Strategy. Format: <metricGroup>::<metricName>::<algo> where metricGroup and metricName "
                    + "should exactly match the metric published via Mantis MetricsRegistry and algo = MAX/AVERAGE\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"zoneList\": {\n"
                    + "        \"name\": \"zoneList\",\n"
                    + "        \"description\": \"list of Zones\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"mantis.jobmaster.clutch.experimental.enabled\": {\n"
                    + "        \"name\": \"mantis.jobmaster.clutch.experimental.enabled\",\n"
                    + "        \"description\": \"Enables the experimental version of the Clutch autoscaler. "
                    + "Note this is different from the Clutch used in production today.\",\n"
                    + "        \"defaultValue\": \"false\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Boolean\"\n"
                    + "      },\n"
                    + "      \"mantis.netty.useSingleThread\": {\n"
                    + "        \"name\": \"mantis.netty.useSingleThread\",\n"
                    + "        \"description\": \"use single netty thread\",\n"
                    + "        \"defaultValue\": \"false\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Boolean\"\n"
                    + "      },\n"
                    + "      \"mantis.w2w.spsc\": {\n"
                    + "        \"name\": \"mantis.w2w.spsc\",\n"
                    + "        \"description\": \"Whether to use spsc or blocking queue\",\n"
                    + "        \"defaultValue\": \"false\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Boolean\"\n"
                    + "      },\n"
                    + "      \"mantis.sse.spsc\": {\n"
                    + "        \"name\": \"mantis.sse.spsc\",\n"
                    + "        \"description\": \"Whether to use spsc or blocking queue for SSE\",\n"
                    + "        \"defaultValue\": \"false\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Boolean\"\n"
                    + "      },\n"
                    + "      \"mantis.stageConcurrency\": {\n"
                    + "        \"name\": \"mantis.stageConcurrency\",\n"
                    + "        \"description\": \"Number of cores to use for stage processing\",\n"
                    + "        \"defaultValue\": \"-1\",\n"
                    + "        \"validatorDescription\": \"range >=-1<=16\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"mantis.w2w.toKeyThreads\": {\n"
                    + "        \"name\": \"mantis.w2w.toKeyThreads\",\n"
                    + "        \"description\": \"number of drainer threads on the ScalarToKey stage\",\n"
                    + "        \"defaultValue\": \"1\",\n"
                    + "        \"validatorDescription\": \"range >=1<=8\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"nettyThreadCount\": {\n"
                    + "        \"name\": \"nettyThreadCount\",\n"
                    + "        \"description\": null,\n"
                    + "        \"defaultValue\": \"4\",\n"
                    + "        \"validatorDescription\": \"range >=1<=8\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"mantis.sse.bufferCapacity\": {\n"
                    + "        \"name\": \"mantis.sse.bufferCapacity\",\n"
                    + "        \"description\": \"buffer on SSE per connection\",\n"
                    + "        \"defaultValue\": \"25000\",\n"
                    + "        \"validatorDescription\": \"range >=1<=100000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"MANTIS_WORKER_JVM_OPTS_STAGE0\": {\n"
                    + "        \"name\": \"MANTIS_WORKER_JVM_OPTS_STAGE0\",\n"
                    + "        \"description\": \"command line options for stage 0 mantis worker JVM, "
                    + "setting this field would override the default GC settings\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"MANTIS_WORKER_JVM_OPTS_STAGE1\": {\n"
                    + "        \"name\": \"MANTIS_WORKER_JVM_OPTS_STAGE1\",\n"
                    + "        \"description\": \"command line options for stage 1 mantis worker JVM, "
                    + "setting this field would override the default GC settings\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"mantis.w2w.toKeyMaxChunkSize\": {\n"
                    + "        \"name\": \"mantis.w2w.toKeyMaxChunkSize\",\n"
                    + "        \"description\": \"batch size for bytes drained from Scalar To Key stage\",\n"
                    + "        \"defaultValue\": \"1000\",\n"
                    + "        \"validatorDescription\": \"range >=1<=100000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"targetApp\": {\n"
                    + "        \"name\": \"targetApp\",\n"
                    + "        \"description\": \"target app\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"targetASGs\": {\n"
                    + "        \"name\": \"targetASGs\",\n"
                    + "        \"description\": \"target ASGs CSV regex\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"mantis.EnableCompressedBinary\": {\n"
                    + "        \"name\": \"mantis.EnableCompressedBinary\",\n"
                    + "        \"description\": \"Enables binary compression of SSE data\",\n"
                    + "        \"defaultValue\": \"false\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Boolean\"\n"
                    + "      },\n"
                    + "      \"mantis.w2w.toKeyBuffer\": {\n"
                    + "        \"name\": \"mantis.w2w.toKeyBuffer\",\n"
                    + "        \"description\": \"per connection buffer from Scalar To Key stage\",\n"
                    + "        \"defaultValue\": \"50000\",\n"
                    + "        \"validatorDescription\": \"range >=1<=100000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"MANTIS_WORKER_JVM_OPTS\": {\n"
                    + "        \"name\": \"MANTIS_WORKER_JVM_OPTS\",\n"
                    + "        \"description\": \"command line options for the mantis worker JVM, setting this field "
                    + "would override the default GC settings\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"mantis.sse.maxReadTimeMSec\": {\n"
                    + "        \"name\": \"mantis.sse.maxReadTimeMSec\",\n"
                    + "        \"description\": \"interval at which buffer is drained to write to SSE\",\n"
                    + "        \"defaultValue\": \"250\",\n"
                    + "        \"validatorDescription\": \"range >=1<=100000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"mantis.sse.numConsumerThreads\": {\n"
                    + "        \"name\": \"mantis.sse.numConsumerThreads\",\n"
                    + "        \"description\": \"number of consumer threads draining the queue to write to SSE\",\n"
                    + "        \"defaultValue\": \"1\",\n"
                    + "        \"validatorDescription\": \"range >=1<=8\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      },\n"
                    + "      \"mantis.jobmaster.clutch.config\": {\n"
                    + "        \"name\": \"mantis.jobmaster.clutch.config\",\n"
                    + "        \"description\": \"Configuration for the clutch autoscaler.\",\n"
                    + "        \"defaultValue\": \"\",\n"
                    + "        \"validatorDescription\": \"always passes validation\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"String\"\n"
                    + "      },\n"
                    + "      \"mantis.sse.maxChunkSize\": {\n"
                    + "        \"name\": \"mantis.sse.maxChunkSize\",\n"
                    + "        \"description\": \"SSE chunk size\",\n"
                    + "        \"defaultValue\": \"1000\",\n"
                    + "        \"validatorDescription\": \"range >=1<=100000\",\n"
                    + "        \"required\": false,\n"
                    + "        \"parameterType\": \"Integer\"\n"
                    + "      }\n"
                    + "    },\n"
                    + "    \"sourceInfo\": null,\n"
                    + "    \"sinkInfo\": null,\n"
                    + "    \"stages\": {\n"
                    + "      \"0\": {\n"
                    + "        \"stageNumber\": 0,\n"
                    + "        \"description\": null\n"
                    + "      }\n"
                    + "    }\n"
                    + "  },\n"
                    + "  \"project\": \"mantis-source-job-publish\",\n"
                    + "  \"version\": \"1.3.0-SNAPSHOT\",\n"
                    + "  \"timestamp\": 1570564886521,\n"
                    + "  \"readyForJobMaster\": true\n"
                    + "}";
            ObjectMapper mapper = new ObjectMapper();

            byte[] bytes = data.getBytes(StandardCharsets.UTF_8); //mapper.writeValueAsBytes(data);
            artifactManager.putArtifact(new Artifact("myart.json",bytes.length, bytes));

            Optional<Artifact> artifactOP = artifactManager.getArtifact("myart.json");
            assertTrue(artifactOP.isPresent());

            byte[] content = artifactOP.get().getContent();
            String readData = new String(content);

            assertEquals(data, readData);

        } catch (Exception e) {
            
            e.printStackTrace();
            fail();
        } finally {
            cleanUpDir(baseDir);
        }

    }

    @Test
    public void nonExistentArtifact() {
        String baseDirStr = ROOT_DIR + "/nonExistentArtifact";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if (Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr +"/",true);

            assertFalse(artifactManager.getArtifact("nonExistent.json").isPresent());

        } catch(Exception e) {

        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void nullArtifactTest() {
        String baseDirStr = ROOT_DIR + "/nullArtifactTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if (Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr +"/",true);

            artifactManager.putArtifact(null);
            fail();

        } catch(Exception e) {
            e.printStackTrace();

        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void readTest() {
        String baseDirStr = ROOT_DIR + "/readTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if(Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);
            String someData = "This is some data!";
            writeData(someData, baseDirStr + "/somedata.json");
            LocalFileBasedArtifactManager artifactManager = new LocalFileBasedArtifactManager(baseDirStr +"/",true);

            Optional<Artifact> artifactOP = artifactManager.getArtifact("somedata.json");

            assertTrue(artifactOP.isPresent());

            byte[] content = artifactOP.get().getContent();
            String readData = new String(content);

            assertEquals(someData, readData);


        } catch(Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void listFilesTest() {
        String baseDirStr = ROOT_DIR + "/listFilesTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if(Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            String someData = "This is some data!";
            String file1 = "somedata.json";
            String someData2 = "This is some data!";
            String file2 = "somedata2.json";
            writeData(someData, baseDirStr +"/" + file1);
            writeData(someData2, baseDirStr + "/" + file2);

            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr + "/",true);
            List<String> artifacts = artifactManager.getArtifacts();
            boolean foundFile1 = false;
            boolean foundFile2 = false;
            for(String artifact : artifacts) {
                System.out.println("Processing " + artifact);
                if(file1.equals(artifact)) {

                    foundFile1 = true;
                    Optional<Artifact> artifactOp = artifactManager.getArtifact(artifact);
                    assertTrue(artifactOp.isPresent());
                    String data1 = new String(artifactOp.get().getContent(), StandardCharsets.UTF_8);
                    assertEquals(someData, data1);
                } else if(file2.equals(artifact)) {
                    foundFile2 = true;
                    Optional<Artifact> artifactOp = artifactManager.getArtifact(artifact);
                    assertTrue(artifactOp.isPresent());
                    String data2 = new String(artifactOp.get().getContent(), StandardCharsets.UTF_8);
                    assertEquals(someData2, data2);
                }

            }
            assertTrue(foundFile1 && foundFile2);

        } catch (Exception e) {
            e.printStackTrace();
            fail();
        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void localDiskCacheDisabledListFileTest() {
        String baseDirStr = ROOT_DIR + "/localDiskCacheDisabledTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if (Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            String someData = "This is some data!";
            String file1 = "somedata.json";
            String someData2 = "This is some data!";
            String file2 = "somedata2.json";
            writeData(someData, baseDirStr + "/" + file1);
            writeData(someData2, baseDirStr + "/" + file2);

            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr + "/",false);

            // should not read from local filesystem.
            assertTrue(artifactManager.getArtifacts().isEmpty());

        } catch(Exception e) {
            fail();
        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void localDiskCacheDisabledGetFileTest() {
        String baseDirStr = ROOT_DIR + "/localDiskCacheDisabledGetFileTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if (Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            String someData = "This is some data!";
            String file1 = "somedata.json";

            writeData(someData, baseDirStr + "/" + file1);


            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr + "/",false);

            // should not read from local filesystem.
            assertFalse(artifactManager.getArtifact("somedata.json").isPresent());


        } catch(Exception e) {
            fail();
        } finally {
            cleanUpDir(baseDir);
        }
    }

    @Test
    public void localDiskCacheDisabledPutFileTest() {
        String baseDirStr = ROOT_DIR + "/localDiskCacheDisabledPutFileTest";
        Path baseDir = Paths.get(baseDirStr);
        try {

            if (Files.exists(baseDir, LinkOption.NOFOLLOW_LINKS)) {
                cleanUpDir(baseDir);
            }
            Files.createDirectory(baseDir);

            String someData = "This is some data!";
            String file1 = "somedata.json";

           // writeData(someData, baseDirStr + "/" + file1);


            LocalFileBasedArtifactManager artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr + "/",false);

            // should not write to local filesystem.
            byte [] dataInBytes = someData.getBytes(StandardCharsets.UTF_8);
            artifactManager.putArtifact(new Artifact("somedata.json",dataInBytes.length,dataInBytes));

            artifactManager =
                    new LocalFileBasedArtifactManager(baseDirStr + "/",false);

            // As the file was not written to disk the getArtifact should return empty.
            assertFalse(artifactManager.getArtifact("somedata.json").isPresent());


        } catch(Exception e) {
            fail();
        } finally {
            cleanUpDir(baseDir);
        }
    }



    private void cleanUpDir(Path baseDir) {

        try {
            Files.list(baseDir).forEach((p) -> {
                try {
                    System.out.println("Cleaning file " + p);
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            Files.delete(baseDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeData(String someData, String fileName) throws Exception {
        OutputStream os = new FileOutputStream(fileName);

        os.write(someData.getBytes(StandardCharsets.UTF_8));
        //logger.info("{} written successfully to disk ", fileName);

        // Close the file
        os.close();

    }
}
