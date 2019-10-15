/*
 * Copyright 2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.mantisrx.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.netflix.archaius.api.Property;
import com.netflix.archaius.api.PropertyRepository;
import io.mantisrx.api.handlers.domain.Artifact;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A simple artifact store that is backed by an optional file system store. For use in dev/testing only.
 * For production use consider a distributed file store like S3 etc.
 */
public class LocalFileBasedArtifactManager implements ArtifactManager {

    private static Logger logger = LoggerFactory.getLogger(LocalFileBasedArtifactManager.class);
    private Map<String, Artifact> artifacts = new HashMap<>();
    private final String artifactStoreLocation;
    private final boolean enableDiskCache;

    public LocalFileBasedArtifactManager(PropertyRepository propertyRepository) {
        // Root directory for store artifacts
        Property<String> artifactStoreLocationProp = propertyRepository.get(PropertyNames.artifactRepositoryLocation,
                String.class).orElse("/tmp/");

        artifactStoreLocation = artifactStoreLocationProp.get();

        // Disable writing to local file system (Maintains artifacts in memory only)
        Property<Boolean> cacheArtifactsLocally = propertyRepository.get(PropertyNames.artifactCacheEnabled,
                Boolean.class).orElse(false);

        this.enableDiskCache = cacheArtifactsLocally.get();

        logger.info("LocalFileBased Artifact Manager initialized! Store to Local File system enabled is {} "
                + "Artifact directory set to {}", enableDiskCache, artifactStoreLocation);
    }

    /**
     * For unit testing.
     * @param artifactStoreLocation
     */
    LocalFileBasedArtifactManager(String artifactStoreLocation, boolean enableDiskCache) {
        this.artifactStoreLocation = artifactStoreLocation;
        this.enableDiskCache = enableDiskCache;

    }

    @Override
    public List<String> getArtifacts() {
        if(enableDiskCache && artifacts.isEmpty()) {
            loadArtifacts(artifacts, artifactStoreLocation);
        }
        return artifacts
                .values()
                .stream()
                .map(Artifact::getFileName)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Artifact> getArtifact(String name) {
        if(artifacts.containsKey(name)) {
            return Optional.of(artifacts.get(name));
        } else {
            if(enableDiskCache) {
                Optional<byte[]> fileBytes = readFromFile(Paths.get(artifactStoreLocation + name));
                if (fileBytes.isPresent()) {

                    Artifact artifact = new Artifact(name, fileBytes.get().length, fileBytes.get());
                    artifacts.put(name, artifact);
                    return Optional.of(artifact);
                }
            }
        }
        return Optional.empty();
    }

    @Override
    public void deleteArtifact(String name) {
        this.artifacts.remove(name);
        if(enableDiskCache) {
            Path path = Paths.get(artifactStoreLocation + name);
            if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
                try {
                    Files.delete(path);
                    logger.debug("File {} deleted successfully", path);
                } catch (IOException e) {
                    logger.warn("Exception {} deleting file {}", e, path);
                }
            }
        }

    }

    @Override
    public void putArtifact(Artifact artifact) {
        Objects.requireNonNull(artifact, "Artifact cannot be null");
        this.artifacts.put(artifact.getFileName(), artifact);
        if(enableDiskCache) {
            String path = artifactStoreLocation + artifact.getFileName();
            writeToFile(artifact.getContent(), Paths.get(path));
        }
    }


    private void loadArtifacts(Map<String, Artifact> artifacts, String artifactStoreLocation) {
        Path path = Paths.get(artifactStoreLocation);
        if(!Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            try {
                Files.createDirectory(path);
            } catch (IOException e) {
                logger.error("Exception {} creating artifact dir ", e);
            }
            return;
        } else {
            try {
                Stream<Path> fileList = Files.list(path);
                fileList
                        .filter((fileName) -> {
                            if(fileName.getFileName().toString().endsWith("json")
                                    || fileName.getFileName().toString().endsWith("zip")) {
                                return true;
                            }
                            return false;
                        })
                        .map((fileName)-> {
                            Optional<byte[]> bytesO = readFromFile(fileName);
                            if(bytesO.isPresent()) {
                                return new Artifact(fileName.getFileName().toString(),
                                        bytesO.get().length,
                                        bytesO.get());
                            } else {
                                return null;
                            }
                        }).filter(Objects::nonNull)
                        .forEach((artifact) -> {
                            artifacts.put(artifact.getFileName(), artifact);
                        });
                    logger.info("Loaded {} files from filesystem", artifacts.size());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /*package protected*/ Optional<byte[]> readFromFile(Path path) {
        logger.debug("Reading file {} ", path);
        if(Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
            try {
                byte[] data = Files.readAllBytes(path);
                logger.debug("Read file {} ", path);
                return Optional.of(data);
            } catch (IOException e) {
                logger.error("Error {} reading from file {}", e, path);
            }
        } else {
            logger.warn("File {} doesn't exist", path);
        }
        return Optional.empty();
    }

    /*package protected*/ boolean writeToFile(byte [] fileData, Path path) {

        try {
            if(Files.isWritable(path.getParent())) {
                Files.write(path,fileData);
                logger.debug("{} written successfully to disk ", path);
            } else {
                logger.warn("File {} is not writeable", path);
            }
        } catch (Exception e) {
            logger.error("Exception: {} saving file {}", e, path);
            return false;
        }
        return true;
    }
}
