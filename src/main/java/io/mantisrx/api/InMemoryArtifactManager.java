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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.mantisrx.api.handlers.domain.Artifact;


public class InMemoryArtifactManager implements ArtifactManager {
    private Map<String, Artifact> artifacts = new HashMap<>();

    @Override
    public List<String> getArtifacts() {
        return artifacts
                .values()
                .stream()
                .map(Artifact::getFileName)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<Artifact> getArtifact(String name) {
        return artifacts
                .values()
                .stream()
                .filter(artifact -> artifact.getFileName().equals(name))
                .findFirst();
    }

    @Override
    public void deleteArtifact(String name) {
        this.artifacts.remove(name);
    }

    @Override
    public void putArtifact(Artifact artifact) {
        this.artifacts.put(artifact.getFileName(), artifact);
    }
}
