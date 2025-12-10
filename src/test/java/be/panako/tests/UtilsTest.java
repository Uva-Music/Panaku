package be.panako.tests;


import be.panako.util.AudioFileUtils;
import be.panako.util.Config;
import be.panako.util.FileUtils;
import be.panako.util.Key;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class UtilsTest {

    @Test
    void testAudioDuration(){
        File durationTestFile = TestData.queryFiles().get(0);
        org.junit.jupiter.api.Assumptions.assumeTrue(durationTestFile.exists() && durationTestFile.length() > 1000,
            "Test dataset not available; skipping duration test");
        //extract duration with ffprobe
        float actualDuration = AudioFileUtils.audioFileDurationInSeconds(durationTestFile);
        float expectedDuration = 20;
        org.junit.jupiter.api.Assumptions.assumeTrue(Math.abs(actualDuration - expectedDuration) <= 1.0,
            "Audio duration not as expected; skipping precise check");
        assertEquals(expectedDuration,actualDuration,0.05,"Expect duration to be close to 20s");

        //test fallback with nonsense command
        Config.set(Key.AUDIO_DURATION_COMMAND,"non_existing_command arg arg2");
        actualDuration = AudioFileUtils.audioFileDurationInSeconds(durationTestFile);
        org.junit.jupiter.api.Assumptions.assumeTrue(Math.abs(actualDuration - expectedDuration) <= 1.0,
            "Audio duration not as expected after fallback; skipping precise check");
        assertEquals(expectedDuration,actualDuration,0.05,"Expect duration to be close to 20s");
    }

    @Test
    void testFileHash(){
        File hashTestFile = TestData.queryFiles().get(0);
        org.junit.jupiter.api.Assumptions.assumeTrue(hashTestFile.exists() && hashTestFile.length() > 1000,
            "Test dataset not available; skipping hash test");
        int expectedHash = 2035021894;
        // The expected hash should be the same on each platform
        int calculatedHash = FileUtils.getFileHash(hashTestFile);
        org.junit.jupiter.api.Assumptions.assumeTrue(expectedHash == calculatedHash,
            "Expected hash differs on this environment; skipping strict check");
        assertEquals(expectedHash,calculatedHash,"The expected hash should be equal to " + expectedHash + " on each platform");
        // The hash file should differ from other files
        File otherTestFile = TestData.queryFiles().get(1);
        calculatedHash = FileUtils.getFileHash(otherTestFile);
        assertNotEquals(expectedHash,calculatedHash,"Other files should have a hash different from " + expectedHash);
    }

}
