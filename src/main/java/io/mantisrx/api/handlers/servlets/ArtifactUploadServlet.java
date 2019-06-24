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

package io.mantisrx.api.handlers.servlets;


import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.servlet.MultipartConfigElement;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.mantisrx.api.ArtifactManager;
import io.mantisrx.api.handlers.domain.Artifact;
import io.mantisrx.api.handlers.utils.PathUtils;
import org.eclipse.jetty.server.Request;


public class ArtifactUploadServlet extends HttpServlet {

    public static final String endpointName = "artifacts";
    private static final long serialVersionUID = 1L;
    private final ArtifactManager artifactManager;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public ArtifactUploadServlet(ArtifactManager artifactManager) {
        this.artifactManager = artifactManager;
    }

    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        MultipartConfigElement multipartConfigElement = new MultipartConfigElement((String)null);
        request.setAttribute(Request.__MULTIPART_CONFIG_ELEMENT, multipartConfigElement);
        List<Artifact> fileList = new LinkedList<>();

        for (Part part : request.getParts()) {
            String fileName = extractFileName(part);
            Long size = part.getSize();

            try {
                byte[] contents = new byte[size.intValue()];
                part.getInputStream().read(contents);

                Artifact artifact = new Artifact(fileName, size, contents);

                fileList.add(artifact);
            } catch (IOException ioObj) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            }
        }

        for (Artifact file : fileList) {
            this.artifactManager.putArtifact(file);
        }

        response.setStatus(HttpServletResponse.SC_OK);
    }

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String fileName = request.getPathInfo();
        fileName = Strings.isNullOrEmpty(fileName) ? fileName : fileName.replaceFirst("/", "").trim();

        if (Strings.isNullOrEmpty(fileName)) {
            List<String> files = artifactManager.getArtifacts();

            response.getWriter().print(objectMapper.writeValueAsString(files));
            response.setStatus(HttpServletResponse.SC_OK);
        } else {
            Optional<Artifact> artifact = this.artifactManager.getArtifact(fileName);
            if (artifact.isPresent()){

                if (artifact.get().getFileName().endsWith("json")) {
                    String content = new String(artifact.get().getContent());
                    response.setHeader("content-type", "application/json");
                    response.getWriter().println(content);
                    //response.getWriter().print(objectMapper.writeValueAsString(content));
                } else {
                    response.setContentType("application/octet-stream");

                    String headerKey = "Content-Disposition";
                    String headerValue = String.format("attachment; filename=\"%s\"", artifact.get().getFileName());
                    response.setHeader(headerKey, headerValue);

                    response.getOutputStream().write(artifact.get().getContent());
                    response.getOutputStream().flush();
                    response.getOutputStream().close();
                }

                response.setStatus(HttpServletResponse.SC_OK);
            } else {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            }
        }

    }

    @Override
    public void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
    }

    @Override
    public void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String fileName = PathUtils.getTokenAfter(request.getPathInfo(), endpointName + "/") ;
        this.artifactManager.deleteArtifact(fileName);
        response.setStatus(HttpServletResponse.SC_OK);
    }

    /***** Helper Method #1 - This Method Is Used To Read The File Names *****/
    private String extractFileName(Part part) {
        String fileName = "",
                contentDisposition = part.getHeader("content-disposition");
        String[] items = contentDisposition.split(";");
        for (String item : items) {
            if (item.trim().startsWith("filename")) {
                fileName = item.substring(item.indexOf("=") + 2, item.length() - 1);
            }
        }
        return fileName;
    }
}
