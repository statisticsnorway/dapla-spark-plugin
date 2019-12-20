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

import java.io.FileInputStream;
import java.io.IOException;

public class GoogleCredentialsFactory {

    public static final String SERVICE_ACCOUNT_KEY_FILE = "DAPLA_SPARK_SERVICE_ACCOUNT_KEY_FILE";

    public static GoogleCredentialsDetails createCredentialsDetails(boolean generateAccessToken, String... scopes) {
        String jsonPath = System.getenv().get(SERVICE_ACCOUNT_KEY_FILE);
        GoogleCredentials credentials;
        String email;
        AccessToken accessToken = null;
        if (jsonPath != null) {
            System.out.println("Using Service Account key file");
            // Use the JSON private key if provided
            try {
                credentials = ServiceAccountCredentials
                    .fromStream(new FileInputStream(jsonPath))
                    .createScoped(scopes);
                email = ((ServiceAccountCredentials) credentials).getAccount();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            // Fall back to using the default Compute Engine service account
            credentials = ComputeEngineCredentials.create();
            email = ((ComputeEngineCredentials) credentials).getAccount();
        }
        if (generateAccessToken) {
            try {
                accessToken = credentials.refreshAccessToken();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new GoogleCredentialsDetails(credentials, email, accessToken.getTokenValue(),
                accessToken.getExpirationTime().getTime());
    }

}
