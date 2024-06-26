/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.gs;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/** The GS file system options. */
public class GSFileSystemOptions {

    /* Flink config option to set the bucket name for temporary blobs. */
    public static final ConfigOption<String> WRITER_TEMPORARY_BUCKET_NAME =
            ConfigOptions.key("gs.writer.temporary.bucket.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the bucket name used by the recoverable writer to store temporary files. "
                                    + "If not specified, temporary files are stored in the same bucket as the final file being written.");

    /* Flink config option to set the chunk size for writing to GCS. */
    public static final ConfigOption<MemorySize> WRITER_CHUNK_SIZE =
            ConfigOptions.key("gs.writer.chunk.size")
                    .memoryType()
                    .noDefaultValue()
                    .withDescription(
                            "This option sets the chunk size for writes to the underlying Google storage. If set, this must be a multiple "
                                    + "of 256KB. If not set, writes will use Google's default chunk size.");

    /* Flink config option to determine if entropy should be enabled in filesink gcs path. */
    public static final ConfigOption<Boolean> ENABLE_FILESINK_ENTROPY =
            ConfigOptions.key("gs.filesink.entropy.enabled")
                    .booleanType()
                    .defaultValue(Boolean.FALSE)
                    .withDescription(
                            "This option can be used to improve performance due to hotspotting "
                                    + "issues on GCS. If this is enabled, entropy in the form of "
                                    + "temporary object id will be injected in the beginning of "
                                    + "gcs path of the temporary objects. The final object path "
                                    + "remains unchanged.");

    /**
     * Flink config option to set the http connection timeout. It will be used by cloud-storage
     * library.
     */
    public static final ConfigOption<Integer> GCS_HTTP_CONNECT_TIMEOUT =
            ConfigOptions.key("gs.http.connect-timeout")
                    .intType()
                    .defaultValue(20_000)
                    .withDescription(
                            "This option sets the timeout in milliseconds to establish a connection.");

    /**
     * Flink config option to set the http read timeout. It will be used by cloud-storage library.
     */
    public static final ConfigOption<Integer> GCS_HTTP_READ_TIMEOUT =
            ConfigOptions.key("gs.http.read-timeout")
                    .intType()
                    .defaultValue(20_000)
                    .withDescription(
                            "This option sets the timeout in milliseconds to read data from an established connection.");

    /** The Flink configuration. */
    private final Configuration flinkConfig;

    /**
     * Constructs an options instance.
     *
     * @param flinkConfig The Flink configuration
     */
    public GSFileSystemOptions(Configuration flinkConfig) {
        this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
        this.flinkConfig
                .getOptional(WRITER_CHUNK_SIZE)
                .ifPresent(
                        chunkSize ->
                                Preconditions.checkArgument(
                                        chunkSize.getBytes() > 0
                                                && chunkSize.getBytes() % (256 * 1024) == 0,
                                        "Writer chunk size must be greater than zero and a multiple of 256KB"));
    }

    /**
     * The temporary bucket name to use for recoverable writes, if different from the final bucket
     * name.
     */
    public Optional<String> getWriterTemporaryBucketName() {
        return flinkConfig.getOptional(WRITER_TEMPORARY_BUCKET_NAME);
    }

    /** Timeout in millisecond to establish the connection. */
    public Optional<Integer> getHTTPConnectionTimeout() {
        return flinkConfig.getOptional(GCS_HTTP_CONNECT_TIMEOUT);
    }

    /** Timeout in millisecond to read content from connection. */
    public Optional<Integer> getHTTPReadTimeout() {
        return flinkConfig.getOptional(GCS_HTTP_READ_TIMEOUT);
    }

    /** The chunk size to use for writes on the underlying Google WriteChannel. */
    public Optional<MemorySize> getWriterChunkSize() {
        return flinkConfig.getOptional(WRITER_CHUNK_SIZE);
    }

    /** Whether entropy insertion is enabled in filesink path. */
    public Boolean isFileSinkEntropyEnabled() {
        return flinkConfig.get(ENABLE_FILESINK_ENTROPY);
    }

    @Override
    public String toString() {
        return "GSFileSystemOptions{"
                + "writerTemporaryBucketName="
                + getWriterTemporaryBucketName()
                + ", writerChunkSize="
                + getWriterChunkSize()
                + '}';
    }
}
