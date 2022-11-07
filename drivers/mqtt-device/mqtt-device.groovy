/**
 * MQTT Device
 *
 * MIT License
 *
 * Copyright (c) 2022 Neil Cumpstey
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import groovy.json.JsonSlurper
import groovy.json.JsonOutput

static String version() { return 'v1.0.0' }
static String rootTopic() { return 'hubitat' }

//hubitat / {hub-name} / { device-name } / { device-capability } / STATE

metadata {
  definition(
    name: 'MQTT Device',
    namespace: 'cwm',
    author: 'Neil Cumpstey, et al',
    description: 'Handles the connection to the MQTT broker for the MQTT Link app',
    iconUrl: 'https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections.png',
    iconX2Url: 'https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections@2x.png',
    iconX3Url: 'https://s3.amazonaws.com/smartapp-icons/Connections/Cat-Connections@3x.png'
  ) {
    capability 'Notification'

    preferences {
      input(
        name: 'brokerIp',
        type: 'string',
        title: 'MQTT Broker IP Address',
        description: 'e.g. 192.168.1.200',
        required: true,
        displayDuringSetup: true
      )
      input(
        name: 'brokerPort',
        type: 'string',
        title: 'MQTT Broker Port',
        description: 'e.g. 1883',
        required: true,
        displayDuringSetup: true
      )
      input(
        name: 'brokerUser',
        type: 'string',
        title: 'MQTT Broker Username',
        description: 'e.g. mqtt_user',
        required: false,
        displayDuringSetup: true
      )
      input(
        name: 'brokerPassword',
        type: 'password',
        title: 'MQTT Broker Password',
        description: 'e.g. ^L85er1Z7g&%2En!',
        required: false,
        displayDuringSetup: true
      )
      input(
        name: 'sendPayload',
        type: 'bool',
        title: 'Send full payload messages on device events',
        required: false,
        default: false
      )
      input(
        name: 'debugLogging',
        type: 'bool',
        title: 'Enable debug logging',
        required: false,
        default: false
      )
    }

    // Provided for broker setup and troubleshooting
    command 'publish', [
      [name:'topic*', type:'STRING', title:'test', description:'Topic'],
      [name:'message', type:'STRING', description:'Message']
    ]
    command 'subscribe', [[name:'topic*', type:'STRING', description:'Topic']]
    command 'unsubscribe', [[name:'topic*', type:'STRING', description:'Topic']]
    command 'connect'
    command 'disconnect'
  }
}

def initialize() {
  debug('Initializing driver...')

  try {
    interfaces.mqtt.connect(getBrokerUri(),
                            "hubitat_${getHubId()}",
                            settings?.brokerUser,
                            settings?.brokerPassword,
                            lastWillTopic: "${getTopicPrefix()}LWT",
                            lastWillQos: 0,
                            lastWillMessage: 'offline',
                            lastWillRetain: true)

    // delay for connection
    pauseExecution(1000)
  } catch (Exception e) {
    error("[d:initialize] ${e}")
  }
}

// ========================================================
// MQTT COMMANDS
// ========================================================

void publish(String topic, String payload) {
  publishMqtt(topic, payload)
}

void subscribe(String topic) {
  if (notMqttConnected()) {
    connect()
  }

  debug("[d:subscribe] full topic: ${getTopicPrefix()}${topic}")
  interfaces.mqtt.subscribe("${getTopicPrefix()}${topic}")
}

void unsubscribe(String topic) {
  if (notMqttConnected()) {
    connect()
  }

  debug("[d:unsubscribe] full topic: ${getTopicPrefix()}${topic}")
  interfaces.mqtt.unsubscribe("${getTopicPrefix()}${topic}")
}

void connect() {
  initialize()
  connected()
}

void disconnect() {
  try {
    interfaces.mqtt.disconnect()
    disconnected()
  } catch (e) {
    warn('Disconnection from broker failed', ${ e.message })
    if (interfaces.mqtt.isConnected()) {
      connected()
    }
  }
}

// ========================================================
// MQTT LINK APP MESSAGE HANDLER
// ========================================================

// Device event notification from MQTT Link app via mqttLink.deviceNotification()
void deviceNotification(message) {
  debug("[d:deviceNotification] Received message from MQTT Link app: '${message}'")

  def slurper = new JsonSlurper()
  def parsed = slurper.parseText(message)

  // Scheduled event in MQTT Broker app that renews device topic subs
  if (parsed.path == '/subscribe') {
    deviceSubscribe(parsed)
  }

  // Device event
  if (parsed.path == '/push') {
    sendDeviceEvent(parsed.body)
  }

  // Publish a specific message
  if (parsed.path == '/publish') {
    publishMqttExact(parsed.body.topic, parsed.body.payload, parsed.body.qos ?: 0, parsed.body.retained ?: false)
  }

  // Device state refresh
  if (parsed.path == '/ping') {
    if (mqttConnected) {
      connected()
    }

    parsed.body.each { device ->
      sendDeviceEvent(device)
    }
  }
}

void deviceSubscribe(message) {
  // Clear all prior subsciptions
  if (message.update) {
    unsubscribe('#')
  }

  // TODO: name these loop variables better, so it's clear what they are.
  // They're currently named entirely wrong.
  message.body.devices.each { key, capability ->
    capability.each { attribute ->
      def normalizedAttrib = normalize(attribute)
      def topic = "${normalizedAttrib}/cmd/${key}".toString()

      debug("[d:deviceSubscribe] topic: ${topic} attribute: ${attribute}")
      subscribe(topic)
    }
  }
}

void sendDeviceEvent(message) {
  publishMqtt("${message.normalizedId}/${message.name}", message.value, 0, !message.momentary)

  if (message.pingRefresh) {
    return
  }

  if (settings.sendPayload) {
    // Send detailed event object
    publishMqtt("${message.normalizedId}/payload", JsonOutput.toJson(message))
  }
}

// ========================================================
// MQTT METHODS
// ========================================================

// Parse incoming message from the MQTT broker
def parse(String event) {
  Map message = interfaces.mqtt.parseMessage(event)
  def (name, hub, device, cmd, type) = message.topic.tokenize( '/' )

  // ignore all msgs that aren't commands
  if (cmd != 'cmd'){
    return
  }

  debug("[d:parse] Received MQTT message: ${message}")

  String json = new groovy.json.JsonOutput().toJson([
    device: device,
    type: type,
    value: message.payload
  ])
  return createEvent(name: 'message', value: json, displayed: false)
}

void mqttClientStatus(status) {
  debug("[d:mqttClientStatus] status: ${status}")
}

void publishMqtt(topic, payload, qos = 0, retained = false) {
  def pubTopic = "${getTopicPrefix()}${topic}"
  publishMqttExact(pubTopic, payload, qos, retained)
}

void publishMqttExact(topic, payload, qos, retained) {
  if (notMqttConnected()) {
    debug('[d:publishMqttExact] not connected')
    initialize()
  }

  try {
    debug("[d:publishMqttExact] topic: ${topic}; payload: ${payload}; qos: ${qos} retained: ${retained}")
    interfaces.mqtt.publish(topic, payload, qos ?: 0, retained ?: false)
  } catch (Exception e) {
    error("[d:publishMqttExact] Unable to publish message: ${e}")
  }
}

// ========================================================
// ANNOUNCEMENTS
// ========================================================

void connected() {
  debug('[d:connected] Connected to broker')
  sendEvent(name: 'connectionState', value: 'connected')
  announceLwtStatus('online')
}

void disconnected() {
  debug('[d:disconnected] Disconnected from broker')
  sendEvent(name: 'connectionState', value: 'disconnected')
  announceLwtStatus('offline')
}

void announceLwtStatus(String status) {
  publishMqtt('LWT', status)
  publishMqtt('FW', "${location.hub.firmwareVersionString}")
  publishMqtt('IP', "${location.hub.localIP}")
  publishMqtt('UPTIME', "${location.hub.uptime}")
}

// ========================================================
// HELPERS
// ========================================================

String normalize(String name) {
  return name.replaceAll('[^a-zA-Z0-9]+', '-').toLowerCase()
}

String getBrokerUri() {
  return "tcp://${settings?.brokerIp}:${settings?.brokerPort}"
}

String getHubId() {
  def hub = location.hubs[0]
  def hubNameNormalized = normalize(hub.name)
  return "${hubNameNormalized}-${hub.hardwareID}".toLowerCase()
}

String getTopicPrefix() {
  return "${rootTopic()}/${getHubId()}/"
}

Boolean mqttConnected() {
  return interfaces.mqtt.isConnected()
}

Boolean notMqttConnected() {
  return !mqttConnected()
}

// ========================================================
// LOGGING
// ========================================================

void debug(msg) {
  if (debugLogging) {
    log.debug msg
  }
}

void info(msg) {
  log.info msg
}

void warn(msg) {
  log.warn msg
}

void error(msg) {
  log.error msg
}
