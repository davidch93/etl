package com.github.davidch93.etl.stream.helper;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GcsHelperTest {

    private static GcsHelper gcsHelper;

    @BeforeAll
    static void setup() {
        Storage localStorage = LocalStorageHelper.getOptions().getService();

        String bucket = "test_bucket";
        String filePath = "path/to/test_file.txt";
        String fileContent = "File content";

        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucket, filePath)).build();
        localStorage.create(blobInfo, fileContent.getBytes(StandardCharsets.UTF_8));

        gcsHelper = new GcsHelper(localStorage);
    }

    @Test
    void testReadFile_withValidPath_thenExpectValidFileContent() {
        String bucket = "test_bucket";
        String filePath = "path/to/test_file.txt";
        String fileContent = "File content";
        String result = gcsHelper.readFile(bucket, filePath);

        assertThat(result).isEqualTo(fileContent);
    }

    @Test
    void testReadFile_withNullBucket_thenExpectThrowsException() {
        String filePath = "path/to/test_file.txt";

        assertThatThrownBy(() -> gcsHelper.readFile(null, filePath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Bucket name cannot be null or empty!");
    }

    @Test
    void testReadFile_withEmptyFilePath_thenExpectThrowsException() {
        String bucket = "test_bucket";

        assertThatThrownBy(() -> gcsHelper.readFile(bucket, ""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File path cannot be null or empty!");
    }

    @Test
    void testReadFile_withInvalidPath_thenExpectThrowsException() {
        String bucket = "test_invalid_bucket";
        String filePath = "path/to/test_file.txt";

        assertThatThrownBy(() -> gcsHelper.readFile(bucket, filePath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("File `gs://test_invalid_bucket/path/to/test_file.txt` is not found!");
    }
}
