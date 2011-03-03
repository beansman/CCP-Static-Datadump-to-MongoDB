using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using DatadumpToMongo.Converters;

namespace DatadumpToMongo
{
    /// <summary>
    /// Class for converting a CCP Eve Online Static Datadump to MongoDB.
    /// This code _DOES NOT_ handle most exceptions internally. So be aware of this when using it!!!
    /// 
    /// All items will have a unique itemID called uniqueID
    /// </summary>
    public class Datadumper
    {
                /// <summary>
        /// Define settings for the JSON output (debug, yay!)
        /// </summary>
        JsonWriterSettings set = new JsonWriterSettings() { OutputMode = JsonOutputMode.JavaScript, NewLineChars = "\r\n", Indent = true, IndentChars = "  " };

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

        #region Mongospecific stuff
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

        public String MongoDatabaseName { get; set; }
        public String MongoCollectionName { get; set; }
        #endregion

        #region Mssql specific stuff
        private SDDDataContext dataContext;
        #endregion

        /// <summary>
        /// Create a new object of Datadumper
        /// </summary>
        /// <param name="mongoConnString">Connectionstring for MongoDB (url format)</param>
        /// <param name="mssqlConnString">Connectionstring for mssql</param>
        /// <param name="mongoDatabaseName">Databasename for mongo</param>
        /// <param name="mongoCollectionName">Collectionname for mongo</param>
        public Datadumper(String mongoConnString, String mssqlConnString, String mongoDatabaseName, String mongoCollectionName)
        {
            if (Debug) Utilities.ConsoleWriter("Constructing dumper...");
            MongoConnString = mongoConnString;
            MssqlConnString = mssqlConnString;
            MongoDatabaseName = mongoDatabaseName;
            MongoCollectionName = mongoCollectionName;

            IsMongoConnected = false;
            IsMssqlConnected = false;
            if (Debug) Utilities.ConsoleWriter("Dumper constructed...");
        }

        ~Datadumper()
        {
            if (Debug) Utilities.ConsoleWriter("Destroying!");
            if (this.mongoServer != null && this.mongoServer.State == MongoServerState.Connected)
                this.mongoServer.Disconnect();
        }

        /// <summary>
        /// Create a new datacontext object and use the given connectionstring
        /// </summary>
        private void CreateMssqlContext()
        {
            this.dataContext = new SDDDataContext(MssqlConnString);
            IsMssqlConnected = true;
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
                this.mongoDatabase = this.mongoServer[MongoDatabaseName];
                this.mongoCollection = this.mongoDatabase.GetCollection(MongoCollectionName);
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
                    this.mongoDatabase = this.mongoServer[MongoDatabaseName];
                    this.mongoCollection = this.mongoDatabase.GetCollection(MongoCollectionName);
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
                this.mongoDatabase = this.mongoServer[MongoDatabaseName];
                this.mongoCollection = this.mongoDatabase.GetCollection(MongoCollectionName);
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

        /// <summary>
        /// Do NOT run this on a live database. It WILL empty your data!
        /// </summary>
        /// <param name="iterations"></param>
        /// <returns></returns>
        public bool TestDumperMongo(int iterations)
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

        public bool DumpToMongoFromMssql()
        {
            DateTime dtStart = DateTime.Now;

            #region Connection stuff

            if (!IsMssqlConnected)
            {
                // Try and create the connection, if exception throw
                try
                {
                    CreateMssqlContext();
                }
                catch (Exception e)
                {
                    if (Debug) Utilities.ConsoleWriter("Exception in datacontext: " + e.Message.ToString());
                    throw;
                }
            }
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
            #endregion

            #region Prepare Mongo
            this.mongoCollection.RemoveAll();
            #endregion

            // NPC Names
            var npc = from i in dataContext.eveNames
                      select i;
            // Grab agents
            var agents = from i in dataContext.agtAgents
                         // Join info about agents (qual)
                         join c in dataContext.agtConfigs on i.agentID equals c.agentID
                         join r in dataContext.agtResearchAgents on i.agentID equals r.agentID
                         select new
                         {
                             i = i,
                             c = c,
                             r = r
                         };
            // npc corps
            var crps = from i in dataContext.crpNPCCorporations
                       select i;

            List<IConverter> parsers = new List<IConverter>();

            parsers.Add(new SolarsystemConverter() { dataContext = dataContext, mongoCollection = mongoCollection, Debug = Debug });

            parsers.Add(new InvTypeConverter() { dataContext = dataContext, mongoCollection = mongoCollection, Debug = Debug });

            // Loop the parsers!
            foreach (IConverter item in parsers)
            {
                item.DoParse();
            }
            
            TimeSpan dtSpan = DateTime.Now - dtStart;
            Utilities.ConsoleWriter("Mongo contains: " + this.mongoCollection.Count() + " documents");
            Utilities.ConsoleWriter("Took " + dtSpan.TotalSeconds + "s");

            return true;
        }

    }

        
}
