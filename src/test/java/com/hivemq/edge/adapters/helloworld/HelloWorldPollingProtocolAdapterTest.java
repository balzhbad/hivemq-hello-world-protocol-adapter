/*
 * Copyright 2023-present HiveMQ GmbH
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
package com.hivemq.edge.adapters.helloworld;


import com.hivemq.adapter.sdk.api.model.ProtocolAdapterInput;
import com.hivemq.adapter.sdk.api.polling.batch.BatchPollingInput;
import com.hivemq.edge.adapters.simplecounter.SimpleCounterPollingProtocolAdapter;
import com.hivemq.edge.adapters.simplecounter.HelloWorldProtocolAdapterInformation;
import com.hivemq.edge.adapters.simplecounter.config.HelloWorldAdapterConfig;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HelloWorldPollingProtocolAdapterTest {

    private final @NotNull ProtocolAdapterInput<HelloWorldAdapterConfig> adapterInput = mock();
    private final @NotNull HelloWorldAdapterConfig config = mock();
    private @TempDir
    @NotNull File temporaryDir;

    @Test
    void test_poll_whenFileIsPresent_thenFileContentsAreSetInOutput() throws IOException {
        final File fileWithData = new File(temporaryDir, "data.txt");
        Files.writeString(fileWithData.toPath(), "Hello World");
        when(adapterInput.getConfig()).thenReturn(config);
        final BatchPollingInput pollingInput = mock(BatchPollingInput.class);
        final TestPollingOutput pollingOutput = new TestPollingOutput();

        final SimpleCounterPollingProtocolAdapter adapter =
                new SimpleCounterPollingProtocolAdapter(new HelloWorldProtocolAdapterInformation(), adapterInput);

        adapter.poll(pollingInput, pollingOutput);

        assertEquals(42, pollingOutput.getDataPoints().get("dataPoint1"));
        assertEquals(1337, pollingOutput.getDataPoints().get("dataPoint2"));
    }
}