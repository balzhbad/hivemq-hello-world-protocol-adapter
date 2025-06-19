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
package com.hivemq.edge.adapters.simplecounter.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.hivemq.adapter.sdk.api.annotations.ModuleConfigField;
import com.hivemq.adapter.sdk.api.config.ProtocolSpecificAdapterConfig;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;


@SuppressWarnings({"unused", "FieldCanBeLocal", "FieldMayBeFinal"})
@JsonPropertyOrder({
        "url",
        "destination"})
public class SimpleCounterAdapterConfig implements ProtocolSpecificAdapterConfig {

    private static final @NotNull String ID_REGEX = "^([a-zA-Z_0-9-_])*$";

    @JsonProperty(value = "id", required = true)
    @ModuleConfigField(title = "Identifier",
            description = "Unique identifier for this protocol adapter",
            format = ModuleConfigField.FieldType.IDENTIFIER,
            required = true,
            stringPattern = ID_REGEX,
            stringMinLength = 1,
            stringMaxLength = 1024)
    protected @NotNull String id;

    @JsonProperty("pollingIntervalMillis")
    @ModuleConfigField(title = "Polling Interval [ms]",
            description = "Time in millisecond that this endpoint will be polled",
            numberMin = 1,
            required = true,
            defaultValue = "1000")
    private int pollingIntervalMillis = 1000;

    @JsonProperty("maxPollingErrorsBeforeRemoval")
    @ModuleConfigField(title = "Max. Polling Errors",
            description = "Max. errors polling the endpoint before the polling daemon is stopped",
            numberMin = 3,
            defaultValue = "10")
    private int maxPollingErrorsBeforeRemoval = 10;
    
    @JsonProperty("initialCounterValue")
    @ModuleConfigField(
            title = "Initial Counter Value",
            description = "The value from which the counter should start.",
            defaultValue = "0",
            numberMin = 0
    )
    private int initialCounterValue = 0; // Default to 0 if not specified

    @JsonProperty("mqttTopic")
    @ModuleConfigField(
            title = "MQTT_Topic",
            description = "The MQTT topic to which the counter value will be published.",
            format = ModuleConfigField.FieldType.MQTT_TOPIC,
            required = true // Make this a required field
    )
    private @NotNull String mqttTopic = "simple-counter/value"; // Default if not specified, but 'required' makes user provide it.

  



    public SimpleCounterAdapterConfig() {
    }
    
    // --- GETTERS FOR NEW FIELDS ---
    public int getInitialCounterValue() {
        return initialCounterValue;
    }

    public @NotNull String getMqttTopic() {
        return mqttTopic;
    }
    
    public int getPollingIntervalMillis() {
        return pollingIntervalMillis;
    }

    public int getMaxPollingErrorsBeforeRemoval() {
        return maxPollingErrorsBeforeRemoval;
    }
    
   
}
