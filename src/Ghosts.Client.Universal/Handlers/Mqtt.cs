// Copyright 2017 Carnegie Mellon University. All Rights Reserved. See LICENSE.md file for terms.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Ghosts.Client.Universal.Infrastructure;
using Ghosts.Domain;
using Ghosts.Domain.Code;
using Ghosts.Domain.Code.Helpers;
using MQTTnet;
using MQTTnet.Protocol;



namespace Ghosts.Client.Universal.Handlers;

/// <summary>
/// MQTT Protocol Handler.
/// Opens a connection to the specified MQTT broker IP and send randomized sensor data.
/// Also listen for MQTT packets and log to Ghosts log file.
/// 
/// </summary>
public class Mqtt(Timeline entireTimeline, TimelineHandler timelineHandler, CancellationToken cancellationToken)
    : BaseHandler(entireTimeline, timelineHandler, cancellationToken)
{
   
    private string _targetIP;

    private int _targetPort = 1883;
    private int _executionProbability = 100;
    private int _jitterFactor;
    private string _publishTopic = "sensor";
    private string _subscribeTopic = "#";  // subscribe to everything
    private bool _enableSubscription = true;
    private int _executionTime = 300000;
    private string _contentGeneration = "random";  // for expansion purposes
    private int _messageDelay = 100000;

    // C# 12+ Collection Expression syntax
    private static readonly Dictionary<string, List<int>> _measurements = new()
    {
        ["length(ft)"] = [0, 1000],
        ["length(m)"] = [0, 1000],
        ["mass(kg)"] = [0, 1000],
        ["mass(lb)"] = [0, 1000],
        ["time(hours)"] = [0, 1000],
        ["time(minutes)"] = [0, 1000],
        ["time(hours)"] = [0, 1000],
        ["temperature(C)"] = [-100, 100],
        ["temperature(F)"] = [-500, 500],
        ["speed(m/s)"] = [0, 1000],
        ["speed(mph)"] = [0, 1000],
        ["force(newtons)"] = [0, 1000],
        ["pressure(psi)"] = [0, 1000],
        ["current(amps)"] = [0, 1000],
        ["dcVolts(amps)"] = [0, 1000],
        ["acVolts(amps)"] = [0, 1000]
    };
    
    
    protected override Task RunOnce()
    {
        try
        {
            if (Handler.HandlerArgs != null)
            {
                if (Handler.HandlerArgs.ContainsKey("target-ip") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["target-ip"].ToString()))
                {
                    _targetIP = Handler.HandlerArgs["target-ip"].ToString();
                }
                else
                {
                   _log.Error("MQTT:: No target-ip is supplied in handler arguments, exiting.");
                    return Task.CompletedTask;
                }

                if (Handler.HandlerArgs.TryGetValue("target-port", out var portArg))
                {
                    if (int.TryParse(portArg.ToString(), out var pt))
                    {
                        _targetPort = (pt < 0) ? 1883 : pt;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("execution-probability", out var execProbArg))
                {
                    if (int.TryParse(execProbArg.ToString(), out var ep))
                    {
                        _executionProbability = (ep < 0 || ep > 100) ? 100 : ep;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("execution-time", out var execTimeArg))
                {
                    if (int.TryParse(execTimeArg.ToString(), out var et))
                    {
                        _executionTime = et < 0 ? 300000 : et;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("message-delay", out var delayArg))
                {
                    if (int.TryParse(delayArg.ToString(), out var md))
                    {
                        _messageDelay = md < 0 ? 100000 : md;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("delay-jitter", out var jitterArg))
                {
                    _jitterFactor = Jitter.JitterFactorParse(jitterArg.ToString());
                }

                if (Handler.HandlerArgs.ContainsKey("publish-topic") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["publish-topic"].ToString()))
                {
                    _publishTopic = Handler.HandlerArgs["publish-topic"].ToString();
                }

                if (Handler.HandlerArgs.ContainsKey("subscribe-topic") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["subscribe-topic"].ToString()))
                {
                    _subscribeTopic = Handler.HandlerArgs["subscribe-topic"].ToString();
                }

                if (Handler.HandlerArgs.ContainsKey("content-generation") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["content-generation"].ToString()))
                {
                    _contentGeneration = Handler.HandlerArgs["content-generation"].ToString();
                }

                if (Handler.HandlerArgs.ContainsKeyWithOption("enable-subscription", "false"))
                {
                    _enableSubscription = false;
                }
            }

            while (true) {
                // loop here. Ex will connect, send for _executionTime seconds, then disconnect
                // This way generated traffic will include connection/disconnection requests in addition to messages
                if (!Ex())
                {
                    // some sort of error occurred. Wait 30 seconds, then try again
                    if (Token.WaitHandle.WaitOne(30000)) Token.ThrowIfCancellationRequested();
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            _log.Error(e);
        }

        return Task.CompletedTask;
    }

    private Task HandleMqttMessage(MqttApplicationMessageReceivedEventArgs e)
    {
        string topic = e.ApplicationMessage.Topic;
        string payload = e.ApplicationMessage.ConvertPayloadToString();
        _log.Info($"MQTT:: Message received: [{topic}] {payload}.");
        // Return a completed task since the event signature expects a Task
        return Task.CompletedTask;
    }

    private string GenerateMqttMessage()
    {

        string measurementName = _measurements.Keys.ElementAt(Random.Shared.Next(_measurements.Count));
        var  pair = _measurements[measurementName];
        double measurementValue  = Random.Shared.Next(pair[0]*10, pair[1]*10)/10.0;
        int sensorNum = Random.Shared.Next(0,10000);

        string deviceName = $"sensor-{sensorNum:D4}";
        string timestamp = DateTime.UtcNow.ToUniversalTime().ToString("o");

        string jsonPayload = $"{{\"DeviceId\":\"{deviceName}\",\"{measurementName}\":{measurementValue},\"Timestamp\":\"{timestamp}\"}}";
        return jsonPayload;
    }

    private bool Ex()
    {
        // Open a connection here
        var factory = new MqttClientFactory();
        using var mqttClient = factory.CreateMqttClient();

        try {

            if (_enableSubscription)
            {
                mqttClient.ApplicationMessageReceivedAsync += e => HandleMqttMessage(e);
            }
            // Configure connection options
            var options = new MqttClientOptionsBuilder()
                .WithTcpServer(_targetIP, _targetPort)
                .WithClientId($"ghosts-publisher-{Guid.NewGuid()}")
                .WithCleanSession()
                .Build();

            // Connect synchronously
            mqttClient.ConnectAsync(options, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
            _log.Info($"MQTT:: Successful connection to broker {_targetIP} on port {_targetPort}");

            if (_enableSubscription)
            {
                // Subscribe
                var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(_subscribeTopic)
                .Build();

                mqttClient.SubscribeAsync(subscribeOptions, cancellationToken).GetAwaiter().GetResult();
                _log.Info($"MQTT:: Subscribed to broker with topic {_subscribeTopic}");
            }

        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            _log.Error($"MQTT:: Unable to connect to broker {_targetIP} on port {_targetPort}, exiting.");
            _log.Error(e);
            return false;
        }

        var waitTime = 0;
        Stopwatch swExecutionTime = Stopwatch.StartNew();
        while (swExecutionTime.ElapsedMilliseconds < _executionTime)
        {
            foreach (var timelineEvent in Handler.TimeLineEvents)
            {
                Token.ThrowIfCancellationRequested();
                WorkingHours.Is(Handler);

                if (timelineEvent.DelayBeforeActual > 0){
                    waitTime = timelineEvent.DelayBeforeActual;
                    if (Token.WaitHandle.WaitOne(waitTime )) Token.ThrowIfCancellationRequested();
                }

                _log.Trace($"MQTT Command: {timelineEvent.Command} with delay after of {timelineEvent.DelayAfterActual}");

                switch (timelineEvent.Command)
                {
                    case "random":
                        if (_executionProbability < _random.Next(0, 100))
                        {
                            _log.Trace("MQTT:: Choice skipped due to execution probability");
                            waitTime = Jitter.JitterFactorDelay(timelineEvent.DelayAfterActual, _jitterFactor);
                            if (Token.WaitHandle.WaitOne(waitTime)) Token.ThrowIfCancellationRequested();
                            continue;
                        }
                        var payload = GenerateMqttMessage();

                        // Build the MQTT application message
                        var message = new MqttApplicationMessageBuilder()
                            .WithTopic(_publishTopic)
                            .WithPayload(payload)
                            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                            .WithRetainFlag(false)
                            .Build();

                        // Publish synchronously
                        mqttClient.PublishAsync(message, cancellationToken)
                            .GetAwaiter()
                            .GetResult();
                        _log.Trace($"MQTT:: Published message: {payload}");
                        
                        waitTime = Jitter.JitterFactorDelay(timelineEvent.DelayAfterActual, _jitterFactor);
                        if (Token.WaitHandle.WaitOne(waitTime)) Token.ThrowIfCancellationRequested();
                        break;
                }

                if (timelineEvent.DelayAfterActual > 0) {
                    waitTime = Jitter.JitterFactorDelay(timelineEvent.DelayAfterActual, _jitterFactor);
                    if (Token.WaitHandle.WaitOne(waitTime)) Token.ThrowIfCancellationRequested();
                }
            
            }
        }
        try {

            // Disconnect cleanly
            mqttClient.DisconnectAsync().GetAwaiter().GetResult();
            _log.Info($"MQTT:: Successful disconnection from broker {_targetIP} on port {_targetPort}");
            // 10 second pause after disconnection
            if (Token.WaitHandle.WaitOne(10000)) Token.ThrowIfCancellationRequested();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            _log.Error($"MQTT:: Error disconnecting from broker {_targetIP} on port {_targetPort}.");
            _log.Error(e);
            return false;
        }
        return true;
    }
   

}
