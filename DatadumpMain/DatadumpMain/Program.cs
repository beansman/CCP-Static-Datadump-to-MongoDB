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
            String mssql = @"Data Source=.\sqlexpress;Initial Catalog=evedb;Integrated Security=True";
            String mongo = @"mongodb://localhost/?safe=true";
            String mongoDb = "KingBoard";

            Console.WriteLine("Press any key to start conversion...");
            Console.ReadLine();

            // Create the dumper
            Datadumper dd = new Datadumper(mongo, mssql, mongoDb);
            // Enable debug
            dd.Debug = false;

            // Run test!
            //dd.TestDumper(1000000);
            try
            {
                dd.DumpToMongoFromMssql();
            }
            catch (Exception e)
            {
                Utilities.ConsoleWriter("Exception in conversion: " + e.Message.ToString());
            }

            // Wait for user input before exit!
            Console.ReadLine();
        }
    }
}
