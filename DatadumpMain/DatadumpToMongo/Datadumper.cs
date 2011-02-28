using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;

namespace DatadumpToMongo
{
    /// <summary>
    /// Class for converting a CCP Eve Online Static Datadump to MongoDB.
    /// This code _DOES NOT_ handle most exceptions internally. So be aware of this when using it!!!
    /// </summary>
    public class Datadumper
    {
        /// <summary>
        /// Connectionstring for mongo
        /// </summary>
        public String MongoConnString { get; set; }
        /// <summary>
        /// Connectionstring for mssql
        /// </summary>
        public String MssqlConnString { get; set; }

        /// <summary>
        /// Is debug enabled?
        /// </summary>
        public bool Debug { get; set; }

        /// <summary>
        /// Is MongoDB connected and ready
        /// </summary>
        public bool IsMongoConnected { get; private set; }
        /// <summary>
        /// Is Mssql connected and ready
        /// </summary>
        public bool IsMssqlConnected { get; private set; }

        /// <summary>
        /// MongoDB server instance
        /// </summary>
        private MongoServer mongoServer;
        /// <summary>
        /// MongoDB database instance
        /// </summary>
        private MongoDatabase mongoDatabase;
        /// <summary>
        /// MongoDB collection instance
        /// </summary>
        private MongoCollection mongoCollection;

        private String mongoDatabaseName = "SomeDatabase";
        private String mongoCollectionName = "SomeCollection";

        public Datadumper(String mongoConnString, String mssqlConnString)
        {
            if (Debug) Utilities.ConsoleWriter("Constructing dumper...");
            MongoConnString = mongoConnString;
            MssqlConnString = mssqlConnString;
            IsMongoConnected = false;
            IsMssqlConnected = false;
            if (Debug) Utilities.ConsoleWriter("Dumper constructed...");
        }

        /// <summary>
        /// Do NOT run this on a live database. It WILL empty your data!
        /// </summary>
        /// <param name="iterations"></param>
        /// <returns></returns>
        public bool TestDumper(int iterations)
        {
            // TEST CONNECTION
            if (Debug) Utilities.ConsoleWriter("Running tests...");
            if (!IsMongoConnected)
            {
                if (Debug) Utilities.ConsoleWriter("MongoDB was not connected, Connecting...");
                try
                {
                    ConnectMongoDB();
                }
                catch (MongoException me)
                {
                    if (Debug) Utilities.ConsoleWriter("MongoException: " + me.Message.ToString());
                    throw;
                }
                catch (Exception e)
                {
                    if (Debug) Utilities.ConsoleWriter("Exception: " + e.Message.ToString());
                    throw;
                }
                if (Debug) Utilities.ConsoleWriter("MongoDB connected...");
            }

            // EMPTY DATABASE
            if (Debug) Utilities.ConsoleWriter("Emptying collection...");
            this.mongoCollection.RemoveAll();
            if (Debug) Utilities.ConsoleWriter("Collection contains: " + this.mongoCollection.Count() + " elements...");


            // RUN ITERATION TEST
            if (Debug) Utilities.ConsoleWriter("Running iteration test over " + iterations + " iterations");
            for (int i = 0; i < iterations; i++)
            {
                var document = new BsonDocument
                {
                    {"demo1", i.ToString()}
                };
                this.mongoCollection.Insert(document);
            }
            if (Debug) Utilities.ConsoleWriter("Done inserting...");

            //CHECK TEST
            if (mongoCollection.Count() != iterations)
            {
                if (Debug) Utilities.ConsoleWriter("There was an error in the insertion, count not matching!!!");
                return false;
            }
            if (Debug) Utilities.ConsoleWriter("All iterations inserted correctly! We have connection");

            return true;
        }



        /// <summary>
        /// Connect to MongoDB instance.
        /// Returns true if already connected!
        /// </summary>
        /// <returns>true if connected correctly</returns>
        private Boolean ConnectMongoDB()
        {
            // Server connected? Select database and collection
            if (this.mongoServer != null && this.mongoServer.State == MongoServerState.Connected)
            {
                this.mongoDatabase = this.mongoServer[this.mongoDatabaseName];
                this.mongoCollection = this.mongoDatabase.GetCollection(this.mongoCollectionName);
                IsMongoConnected = true;

                return true;
            }

            // Server not connected? connect and select db and collection
            if (this.mongoServer != null && this.mongoServer.State == MongoServerState.Disconnected)
            {
                // Connect
                this.mongoServer.Connect();
                // Did it work?
                if (this.mongoServer.State == MongoServerState.Connected)
                {
                    this.mongoDatabase = this.mongoServer[this.mongoDatabaseName];
                    this.mongoCollection = this.mongoDatabase.GetCollection(this.mongoCollectionName);
                    IsMongoConnected = true;

                    return true;
                }
                else
                {
                    this.IsMongoConnected = false;
                    this.mongoCollection = null;
                    this.mongoDatabase = null;
                    this.mongoServer = null;

                    return false;
                }
            }

            this.mongoServer = MongoServer.Create(MongoConnString);
            // Connect
            this.mongoServer.Connect();
            // Did it work?
            if (this.mongoServer.State == MongoServerState.Connected)
            {
                this.mongoDatabase = this.mongoServer[this.mongoDatabaseName];
                this.mongoCollection = this.mongoDatabase.GetCollection(this.mongoCollectionName);
                IsMongoConnected = true;
                return true;
            }
            else
            {
                // Did not work, reset
                this.IsMongoConnected = false;
                this.mongoCollection = null;
                this.mongoDatabase = null;
                this.mongoServer = null;

                return false;
            }
        }

    }
}
