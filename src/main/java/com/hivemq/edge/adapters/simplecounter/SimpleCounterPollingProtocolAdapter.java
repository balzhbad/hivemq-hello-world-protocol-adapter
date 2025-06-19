/*
 * Copyright 2023-present HiveMQ GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.hivemq.edge.adapters.simplecounter;

import com.hivemq.adapter.sdk.api.ProtocolAdapterInformation;
import com.hivemq.adapter.sdk.api.model.*;
import com.hivemq.adapter.sdk.api.polling.batch.BatchPollingInput;
import com.hivemq.adapter.sdk.api.polling.batch.BatchPollingOutput;
import com.hivemq.adapter.sdk.api.polling.batch.BatchPollingProtocolAdapter;
import com.hivemq.adapter.sdk.api.state.ProtocolAdapterState;
import com.hivemq.edge.adapters.simplecounter.config.SimpleCounterAdapterConfig;


import org.jetbrains.annotations.NotNull;
import java.nio.charset.StandardCharsets;

public class SimpleCounterPollingProtocolAdapter implements BatchPollingProtocolAdapter {

    private final @NotNull SimpleCounterAdapterConfig adapterConfig;
    private final @NotNull ProtocolAdapterInformation adapterInformation;
    private final @NotNull ProtocolAdapterState protocolAdapterState;
    private final @NotNull String adapterId;

    public SimpleCounterPollingProtocolAdapter(
            final @NotNull ProtocolAdapterInformation adapterInformation,
            final @NotNull ProtocolAdapterInput<SimpleCounterAdapterConfig> input) {
        this.adapterId = input.getAdapterId();
        this.adapterInformation = adapterInformation;
        this.adapterConfig = input.getConfig();
        this.protocolAdapterState = input.getProtocolAdapterState();
    }
    
    // --- NEW INSTANCE VARIABLE FOR THE COUNTER ---
    private int currentCounterValue;

    @Override
    public @NotNull String getId() {
        return adapterId;
    }

    @Override
    public void start(
            final @NotNull ProtocolAdapterStartInput input,
            final @NotNull ProtocolAdapterStartOutput output) {
        // any setup which should be done before the adapter starts polling comes here.
        try {
        	// --- INITIALIZE THE COUNTER ---
            this.currentCounterValue = adapterConfig.getInitialCounterValue();
            protocolAdapterState.setConnectionStatus(ProtocolAdapterState.ConnectionStatus.STATELESS);
            output.startedSuccessfully();
        } catch (final Exception e) {
            output.failStart(e, null);
        }
    }

    @Override
    public void stop(
            final @NotNull ProtocolAdapterStopInput protocolAdapterStopInput,
            final @NotNull ProtocolAdapterStopOutput protocolAdapterStopOutput) {
        protocolAdapterStopOutput.stoppedSuccessfully();
    }

    @Override
    public @NotNull ProtocolAdapterInformation getProtocolAdapterInformation() {
        return adapterInformation;
    }

    @Override
    public void poll(
            final @NotNull BatchPollingInput pollingInput,
            final @NotNull BatchPollingOutput pollingOutput) {
        // here the sampling must be done. F.e. sending a http request
    	// --- INCREMENT THE COUNTER ---
        currentCounterValue++;
        // --- PUBLISH THE COUNTER VALUE TO THE CONFIGURED MQTT TOPIC ---
        String topic = adapterConfig.getMqttTopic();
        String payload = String.valueOf(currentCounterValue); // Convert int to String for payload
    	
//        try {
//            // Get the MqttClient from the adapterInput
//            pollingInput.getAdapterInput().getMqttClient().publish(
//                    topic,
//                    payload.getBytes(StandardCharsets.UTF_8), // Assuming StandardCharsets is fixed
//                    0, 
//                    false 
//            );
//        } catch (Exception e) {
//            // Handle exceptions during publishing if necessary
//            // For example, log the error or set an error state
//        	protocolAdapterState.setConnectionStatus(ProtocolAdapterState.ConnectionStatus.ERROR);
//            // Note: `pollingOutput.finish()` should still be called to complete the poll cycle.
//        }
        pollingOutput.addDataPoint("dataPoint1", 42);
        pollingOutput.addDataPoint("dataPoint2", 1337);
        pollingOutput.finish();
    }

    @Override
    public int getPollingIntervalMillis() {
        return adapterConfig.getPollingIntervalMillis();
    }

    @Override
    public int getMaxPollingErrorsBeforeRemoval() {
        return adapterConfig.getMaxPollingErrorsBeforeRemoval();
    }
}
