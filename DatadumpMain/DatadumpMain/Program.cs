using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DatadumpToMongo;

namespace DatadumpMain
{
    class Program
    {
        static void Main(string[] args)
        {
            String mssql = @"";
            String mongo = @"mongodb://localhost/?safe=true";

            // Create the dumper
            Datadumper dd = new Datadumper(mongo, mssql);
            // Enable debug
            dd.Debug = true;

            // Run test!
            dd.TestDumper(1000000);

            // Wait for user input before exit!
            Console.ReadLine();
        }
    }
}
