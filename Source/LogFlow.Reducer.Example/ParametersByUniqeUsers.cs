﻿using System;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace LogFlow.Reducer.Example
{
    public class ParametersByUniqeUsers : Reduction<SearchEvent, ReducedEvent, Helper>
	{
        public ParametersByUniqeUsers()
		{
			CreateReduction()
                .ReduceIntputWith((inputData, result) =>
			{
			    foreach (var parameter in inputData.Parameters)
			    {
			        var id = parameter + "_" + inputData.TimeStamp.ToString("yyyy-MM-dd");
                    if(result.ContainsKey(id))
                    {
                        if(result[id].Helper.UserNames.Contains(inputData.User))
                            continue;

                        result[id].Helper.UserNames.Add(inputData.User);

                        result[id].Output.NumberOfUniqeUsers = result[id].Helper.UserNames.Count;
                    }
                    else
                    {
                        var resultData = new ReductionResultData<ReducedEvent, Helper>();
                        resultData.Output = new ReducedEvent() { NumberOfUniqeUsers = 1, Parameter = parameter, TimeStamp = inputData.TimeStamp.Date };
                        resultData.Helper = new Helper() { UserNames = new List<string>()};
                        resultData.Helper.UserNames.Add(inputData.User);
                        result.Add(id, resultData);
                    }

			    }

			    return result;
			});
		}
	}

	public class SearchEvent
	{
        [JsonProperty("@timestamp")]
        public DateTime TimeStamp { get; set; }
        public List<string> Parameters { get; set; }
        public string User { get; set; }
	}

    public class ReducedEvent
    {
        [JsonProperty("@timestamp")]
        public DateTime TimeStamp { get; set; }
        public string Parameter { get; set; }
        public int NumberOfUniqeUsers { get; set; }
    }

    public class Helper
    {
        public List<string> UserNames { get; set; }
    }
}