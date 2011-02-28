using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DatadumpToMongo
{
    public static class Utilities
    {
        public static void ConsoleWriter(String s)
        {
            Console.WriteLine(DateTime.Now.ToUniversalTime() + " : " + s);
        }
    }
}
