// Copyright 2017 Carnegie Mellon University. All Rights Reserved. See LICENSE.md file for terms.

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Ghosts.Client.Universal.Infrastructure;
using Ghosts.Domain;
using Ghosts.Domain.Code;
using Ghosts.Domain.Code.Helpers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.IO.Pipelines;
using System.Text;
using System.Collections;
using System.Buffers;
using System.Collections.Concurrent;



namespace Ghosts.Client.Universal.Handlers;

/// <summary>
/// IRC Protocol Handler.
/// Opens a connection to the specified IRC server IP and send randomized message to specified channel.
/// Also listen to specified channel and echo to ghosts log file
/// 
/// </summary>
public class Irc(Timeline entireTimeline, TimelineHandler timelineHandler, CancellationToken cancellationToken)
    : BaseHandler(entireTimeline, timelineHandler, cancellationToken)
{
   
    private string _targetIP;
    private IPAddress _targetAsIpaddr = null;

    private int _targetPort = 6667;
    private int _executionProbability = 100;
    private int _jitterFactor;

    private string _nickname = null;
    private string _channelname = "general";

    private string _contentGeneration = "random";  // for expansion purposes
    private int _messageDelay = 100000;
    private int _executionTime = 300000;

    private ChatContent messages = null;

    private static readonly string[] WordList =
    {
        "hello", "how", "are", "you", "doing", "today", "great", "thanks",
        "sure", "let", "me", "know", "sounds", "good", "talk", "later",
        "meeting", "tomorrow", "project", "update", "quick", "question",
        "appreciate", "help", "working", "on", "that", "right", "now",
        "check", "email", "sent", "earlier", "lunch", "break", "coffee",
        "team", "deadline", "friday", "status", "report", "review",
        "document", "shared", "folder", "looks", "fine", "agreed",
        "morning", "afternoon", "evening", "weekend", "plans", "busy",
        "free", "available", "schedule", "call", "chat", "soon"
    };


    protected override Task RunOnce()
    {
        try
        {
            if (messages == null) messages = new ChatContent();
            if (Handler.HandlerArgs != null)
            {
                if (Handler.HandlerArgs.ContainsKey("target-ip") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["target-ip"].ToString()))
                {
                    _targetIP = Handler.HandlerArgs["target-ip"].ToString();
                    try
                    {
                        _targetAsIpaddr = IPAddress.Parse(_targetIP);
                    }
                    catch (Exception e)
                    {
                        _log.Error(e);
                        _log.Error($"IRC: Unable to parse {_targetIP} as valid IP address, exiting.");
                        return Task.CompletedTask;
                    }
                }
                else
                {
                   _log.Error("IRC:: No target-ip is supplied in handler arguments, exiting.");
                    return Task.CompletedTask;
                }

                if (Handler.HandlerArgs.TryGetValue("target-port", out var portArg))
                {
                    if (int.TryParse(portArg.ToString(), out var pt))
                    {
                        _targetPort = (pt < 0) ? 6667 : pt;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("execution-probability", out var execProbArg))
                {
                    if (int.TryParse(execProbArg.ToString(), out var ep))
                    {
                        _executionProbability = (ep < 0 || ep > 100) ? 100 : ep;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("message-delay", out var delayArg))
                {
                    if (int.TryParse(delayArg.ToString(), out var md))
                    {
                        _messageDelay = md < 0 ? 100000 : md;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("execution-time", out var execTimeArg))
                {
                    if (int.TryParse(execTimeArg.ToString(), out var et))
                    {
                        _executionTime = et < 0 ? 300000 : et;
                    }
                }

                if (Handler.HandlerArgs.TryGetValue("delay-jitter", out var jitterArg))
                {
                    _jitterFactor = Jitter.JitterFactorParse(jitterArg.ToString());
                }

                if (Handler.HandlerArgs.ContainsKey("nickname") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["nickname"].ToString()))
                {
                    _nickname = Handler.HandlerArgs["nickname"].ToString();
                }

                if (Handler.HandlerArgs.ContainsKey("content-generation") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["content-generation"].ToString()))
                {
                    _contentGeneration = Handler.HandlerArgs["content-generation"].ToString();
                }


                if (Handler.HandlerArgs.ContainsKey("channelname") &&
                    !string.IsNullOrEmpty(Handler.HandlerArgs["channelname"].ToString()))
                {
                    _channelname = Handler.HandlerArgs["channelname"].ToString();
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

    private string GenerateRandomMessage()
    {
        string message = null;
        if (messages != null)
        {
            // this uses the ChatContent used by the Windows Pidgin handler
            // defined in config/blog-reply.csv
            message =  messages.MessageNext();
            if (message != "nochatcontentavailable")
            {
                return message;
            }
        }
        // if get here, no chat content available, generate a random message
        var wordCount = _random.Next(3, 12);
        var words = new List<string>(wordCount);
        for (var i = 0; i < wordCount; i++)
        {
            words.Add(WordList[_random.Next(0, WordList.Length)]);
        }

        message = string.Join(" ", words);
        // Capitalize first letter
        if (message.Length > 0)
            message = char.ToUpper(message[0]) + message.Substring(1);

        return message;
    }


    private bool Ex()
    {
        bool returnValue = false;
        try 
        {

            // Open a connection here
            using var tcpClient = new TcpClient();
            tcpClient.Connect(_targetAsIpaddr, _targetPort);

            NetworkStream stream = tcpClient.GetStream();

            using var reader = new StreamReader(stream, Encoding.UTF8);
            using var writer = new StreamWriter(stream, new UTF8Encoding(false))
            {
                NewLine = "\r\n",
                AutoFlush = true,
            };

            using var sendQueue = new BlockingCollection<string>();
            bool hasJoined = false;
            int sendWaitTime = 0;


            // Start the separate writer task with a Stopwatch
            // The writer task periodically writes messages to the channel
            // The foreground reader handles all input from the IRC server
            Task writerTask = Task.Run(() =>
            {
                var stopwatch = Stopwatch.StartNew();

                try
                {
                    // Keep running until the queue is marked complete and empty
                    while (!sendQueue.IsCompleted)
                    {
                        // Wait up to 100ms for a queued command (PING, JOIN, etc.)
                        if (sendQueue.TryTake(out string? command, 100))
                        {
                            writer.WriteLine(command);
                        }

                        // Check send wait time elapsed after joining the channel
                        if (hasJoined && stopwatch.ElapsedMilliseconds >= sendWaitTime)
                        {
                            //string message = $"Automated message #{messageCount++}";
                            string message = GenerateRandomMessage();
                            writer.WriteLine($"PRIVMSG {_channelname} :{message}");
                            stopwatch.Restart();
                            // get next delay
                            sendWaitTime = Jitter.JitterFactorDelay(_messageDelay, _jitterFactor);
                        }
                    }
                }
                catch (IOException)
                {
                    // Stream closed while writing
                }
            });
            
            // 1. Register nickname and user details
            sendQueue.Add($"NICK {_nickname}");
            sendQueue.Add($"USER {_nickname} 0 * :{_nickname}");
            Stopwatch swExecutionTime = Stopwatch.StartNew();
            
            
            // read loop
            // wait a maximum of 30 seconds for connection
            while (true)
            {
                // exit conditions
                if (!hasJoined && swExecutionTime.ElapsedMilliseconds > 30000)
                {
                    // unable to join server after 30 seconds
                    _log.Info($"IRC:: Unable to join to server {_targetIP} on port {_targetPort}");
                    returnValue = false;
                    break;
                }
                if (hasJoined && swExecutionTime.ElapsedMilliseconds > _executionTime)
                {
                    _log.Info($"IRC:: Execution time exceeded, closing connection to server {_targetIP} on port {_targetPort}");
                    returnValue = true;
                    break;
                }

                // read data from server
                string? message = reader.ReadLine();
                if (message == null) break;

                _log.Info($"IRC:: Received {message} from server {_targetIP} on port {_targetPort} with channel {_channelname}");
                // Respond to PING immediately to avoid disconnection
                if (message.StartsWith("PING "))
                {
                    string pongResponse = "PONG " + message.Substring(5);
                    sendQueue.Add(pongResponse);
                    _log.Info($"IRC:: Pong response {pongResponse} to server {_targetIP} on port {_targetPort}");
                    continue;
                }
                if (!hasJoined)
                {
                    if (message.Contains(" 001 ")) {
                        sendQueue.Add($"JOIN {_channelname}");
                    }
                    else if (message.Contains(" 366 "))
                    {
                        hasJoined = true;
                        _log.Info($"IRC:: Successful join to server {_targetIP} on port {_targetPort}");
                    }
                }
                
                
            }
            sendQueue.CompleteAdding();
            writerTask.Wait();
            // 10 second pause after disconnection
            if (Token.WaitHandle.WaitOne(10000)) Token.ThrowIfCancellationRequested();
        }
        
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            _log.Error($"IRC:: Error for server {_targetIP} on port {_targetPort}, exiting.");
            _log.Error(e);
            return false;
        }

        return returnValue;
    }
   

}
