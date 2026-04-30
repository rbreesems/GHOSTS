// Copyright 2017 Carnegie Mellon University. All Rights Reserved. See LICENSE.md file for terms.

using System;
using Ghosts.Domain;
using Ghosts.Domain.Code;
using Newtonsoft.Json;
using NLog;

namespace ghosts.client.linux.handlers
{
    public abstract class BaseHandler
    {
        public static readonly Logger _log = LogManager.GetCurrentClassLogger();
        private static readonly Logger _timelineLog = LogManager.GetLogger("TIMELINE");
        internal static readonly Random _random = new();

        public static void Init(TimelineHandler handler)
        {
            WorkingHours.Is(handler);
        }

        public static void Report(ReportItem reportItem)
        {
            var result = new TimeLineRecord
            {
                Handler = reportItem.Handler,
                Command = reportItem.Command,
                CommandArg = reportItem.Arg,
                Result = reportItem.Result,
                TrackableId = reportItem.Trackable
            };

            var o = JsonConvert.SerializeObject(result,
                Formatting.None,
                new JsonSerializerSettings
                {
                    NullValueHandling = NullValueHandling.Ignore
                });

            _timelineLog.Info($"TIMELINE|{DateTime.UtcNow}|{o}");
        }

        public bool CheckProbabilityVar(string name, int value)
        {
            if (!(value >= 0 && value <= 100))
            {
                _log.Trace($"Variable {name} with value {value} must be an int between 0 and 100, setting to 0");
                return false;
            }
            return true;
        }

        /// <summary>
        /// Select an action from a list of probabilities.
        /// It is assumed that the list of probabilities adds up to <= 100.
        /// Each probability is associated with a string name in action list
        /// If the probabilities add up to less than 100, then null can be returned
        /// which means that no action was chosen
        /// </summary>
        /// <param name="probabilityList"></param>
        /// <param name="actionList"></param>
        /// <returns></returns>
        public static string SelectActionFromProbabilities(int[] probabilityList, string[] actionList)
        {
            int choice = _random.Next(0, 101);
            int endRange;
            int startRange = 0;
            int index = 0;
            foreach (var probability in probabilityList)
            {
                if (probability > 0)
                {
                    endRange = startRange + probability;
                    if (choice >= startRange && choice <= endRange) return actionList[index];
                    else startRange = endRange + 1;
                }
                index++;
            }

            return null;
        }

    }

}
