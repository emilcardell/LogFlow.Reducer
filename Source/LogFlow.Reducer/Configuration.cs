using System;
using System.Configuration;
using System.IO;

namespace LogFlow.Reducer
{
	public class Configuration
	{
		public static string StoragePath
		{
			get
			{
				return ConfigurationManager.AppSettings["StoragePath"] ??
				       Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "StateStorage");
			}
		}
	}
}