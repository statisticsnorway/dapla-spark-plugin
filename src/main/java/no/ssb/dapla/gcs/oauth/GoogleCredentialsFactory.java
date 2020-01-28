/*
 * Copyright 2019 Google LLC
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

package no.ssb.dapla.gcs.oauth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;

public class GoogleCredentialsFactory {

    private static final Logger LOG = LoggerFactory.getLogger(GoogleCredentialsFactory.class);

    public static GoogleCredentialsDetails createCredentialsDetails(String keyFile, boolean useComputeEngineFallback, String... scopes) {
        try {
            GoogleCredentials credentials;
            String email;
            AccessToken accessToken = null;
            if (keyFile != null) {
                LOG.info("Using Service Account key file: " + keyFile);
                // Use the JSON private key if provided
                credentials = ServiceAccountCredentials
                        .fromStream(new FileInputStream(keyFile))
                        .createScoped(scopes);
                email = ((ServiceAccountCredentials) credentials).getAccount();
            } else if (useComputeEngineFallback) {
                // Fall back to using the default Compute Engine service account
                credentials = ComputeEngineCredentials.create();
                email = ((ComputeEngineCredentials) credentials).getAccount();
            } else {
                throw new RuntimeException("Could not find service account key file");
            }
            accessToken = credentials.refreshAccessToken();
            return new GoogleCredentialsDetails(credentials, email, accessToken.getTokenValue(),
                    accessToken.getExpirationTime().getTime());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
