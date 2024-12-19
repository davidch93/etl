package com.github.davidch93.etl.stream.helpers;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * Helper class for interacting with Google Cloud Storage (GCS).
 * Provides methods for reading files from GCS buckets.
 * <p>
 * Example usage:
 * <pre>{@code
 * // Create a new GcsHelper instance
 * GcsHelper gcsHelper = new GcsHelper();
 *
 * // Read the content of a file from a GCS bucket
 * String bucketName = "my_bucket";
 * String filePath = "path/to/file.json";
 * String fileContent = gcsHelper.readFile(bucketName, filePath);
 * }</pre>
 * </p>
 *
 * <p>
 * Note: The class provides methods for reading files from GCS buckets.
 * It assumes that the specified file exists in the bucket.
 * Throws a {@code IllegalArgumentException} if the file is not found or if there is an error accessing the file.
 * </p>
 *
 * @author david.christianto
 */
public class GcsHelper {

    private static final Logger logger = LoggerFactory.getLogger(GcsHelper.class);

    private final Storage storage;

    /**
     * Constructs a new GcsHelper instance using the default Storage service.
     */
    public GcsHelper() {
        this.storage = StorageOptions.getDefaultInstance().getService();
    }

    /**
     * Constructs a new GcsHelper instance with the specified Storage service.
     *
     * @param storage the Storage service to be used
     */
    public GcsHelper(Storage storage) {
        this.storage = storage;
    }

    /**
     * Reads the content of a file from the specified bucket and file path in Google Cloud Storage.
     *
     * @param bucket   the name of the GCS bucket
     * @param filePath the path to the file in the GCS bucket
     * @return the content of the file as a string
     * @throws IllegalArgumentException if the file is not found or if there is an error accessing the file
     */
    public String readFile(String bucket, String filePath) {
        if (StringUtils.isEmpty(bucket)) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty!");
        }
        if (StringUtils.isEmpty(filePath)) {
            throw new IllegalArgumentException("File path cannot be null or empty!");
        }

        logger.debug("[ETL-STREAM] Reading file from bucket: {}, path: {}", bucket, filePath);

        BlobId blobId = BlobId.of(bucket, filePath);
        Blob blob = storage.get(blobId);

        if (blob == null) {
            throw new IllegalArgumentException("File `" + blobId.toGsUtilUri() + "` is not found!");
        }

        return new String(blob.getContent(), StandardCharsets.UTF_8);
    }
}
