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
    /// All items will have a unique itemID called uniqueID and a unique name called uniqueName
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

        public String MongoDatabaseName { get; set; }
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
        public Datadumper(String mongoConnString, String mssqlConnString, String mongoDatabaseName)
        {
            if (Debug) Utilities.ConsoleWriter("Constructing dumper...");
            MongoConnString = mongoConnString;
            MssqlConnString = mssqlConnString;
            MongoDatabaseName = mongoDatabaseName;

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
                    IsMongoConnected = true;

                    return true;
                }
                else
                {
                    this.IsMongoConnected = false;
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
                IsMongoConnected = true;
                return true;
            }
            else
            {
                // Did not work, reset
                this.IsMongoConnected = false;
                this.mongoDatabase = null;
                this.mongoServer = null;

                return false;
            }
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

            Utilities.ConsoleWriter("Dropping the old db. Clean start!");
            // Drop the db!
            this.mongoDatabase.Drop();
            this.mongoDatabase = this.mongoServer[this.MongoDatabaseName];

            // Create a list of converters
            List<IConverter> converters = new List<IConverter>();

            // Add the solarsystems
            converters.Add(new SolarsystemConverter()
            {
                dataContext = dataContext,
                mongoCollection = this.mongoDatabase.GetCollection("Solarsystems"),
                Debug = Debug
            });

            // Add the regions
            converters.Add(new RegionConverter()
            {
                dataContext = dataContext,
                mongoCollection = this.mongoDatabase.GetCollection("Regions"),
                Debug = Debug
            });


            // Add the InvTypes
            converters.Add(new InvTypeConverter()
            {
                dataContext = dataContext,
                mongoCollection = this.mongoDatabase.GetCollection("Types"),
                Debug = Debug
            });

            // Loop the converters!
            foreach (IConverter item in converters)
            {
                item.DoParse();
            }
            
            TimeSpan dtSpan = DateTime.Now - dtStart;
            foreach (var item in this.mongoDatabase.GetCollectionNames())
            {
                Utilities.ConsoleWriter("Mongo collection: " + item + " contains: " + this.mongoDatabase.GetCollection(item).Count() + " documents");
            }
            
            Utilities.ConsoleWriter("Took " + dtSpan.TotalSeconds + "s");

            return true;
        }

    }

        
}
