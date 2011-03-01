using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace DatadumpToMongo
{
    /// <summary>
    /// Class for converting a CCP Eve Online Static Datadump to MongoDB.
    /// This code _DOES NOT_ handle most exceptions internally. So be aware of this when using it!!!
    /// </summary>
    public class Datadumper
    {
        /// <summary>
        /// Categories as taken from invCategories
        /// </summary>
        enum CategoryTypes
        {
            _System = 0,
            Owner=1,
            Celestial=2,
            Station=3,
            Material=4,
            Accessories=5,
            Ship=6,
            Module=7,
            Charge=8,
            Blueprint=9,
            Trading=10,
            Entity=11,
            Bonus=12,
            Skill=16,
            Commodity=17,
            Drone=18,
            Implant=20,
            Deployable=22,
            Structure=23,
            Reaction=24,
            Asteroid=25,
            Interiors=26,
            Placeables=27,
            Abstract=29,
            Subsystem=32,
            Ancient_Relics=34,
            Decryptors=35,
            Infrastructure_Upgrades=39,
            Sovereignty_Structures=40,
            Planetary_Interaction=41,
            Planetary_Resources=42,
            Planetary_Commodities=43,
        }

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

        private String mongoDatabaseName = "SomeDatabase";
        private String mongoCollectionName = "SomeCollection";
        #endregion

        #region Mssql specific stuff
        private SDDDataContext dataContext;
        #endregion

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
        /// Create a new datacontext object and use the given connectionstring
        /// </summary>
        private void CreateMssqlContext()
        {
            this.dataContext = new SDDDataContext();//MssqlConnString);
            IsMssqlConnected = true;
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

        public bool TestDumperMssql()
        {
            if (!IsMssqlConnected) CreateMssqlContext();


            var data = (from i in dataContext.invTypes
                        //where i.typeName == "Echelon"
                        join g in dataContext.invGroups on i.groupID equals g.groupID
                        join c in dataContext.invCategories on g.categoryID equals c.categoryID
                        select new BaseType
                        {
                            InvType = i,
                            InvGroup = g,
                            InvCategory = c
                        });

            
            foreach (var item in data)
            {
                switch ((CategoryTypes)Convert.ToInt32(item.InvCategory.categoryID))
                {
                    case CategoryTypes._System:
                        break;
                    case CategoryTypes.Owner:
                        break;
                    case CategoryTypes.Celestial:
                        break;
                    case CategoryTypes.Station:
                        break;
                    case CategoryTypes.Material:
                        break;
                    case CategoryTypes.Accessories:
                        break;
                    case CategoryTypes.Ship:
                        Utilities.ConsoleWriter("Parsing ship: " + item.InvType.typeName);
                        Console.WriteLine(DoShip(item).ToJson(set));
                        return true;
                        break;
                    case CategoryTypes.Module:
                        break;
                    case CategoryTypes.Charge:
                        break;
                    case CategoryTypes.Blueprint:
                        break;
                    case CategoryTypes.Trading:
                        break;
                    case CategoryTypes.Entity:
                        break;
                    case CategoryTypes.Bonus:
                        break;
                    case CategoryTypes.Skill:
                        break;
                    case CategoryTypes.Commodity:
                        break;
                    case CategoryTypes.Drone:
                        break;
                    case CategoryTypes.Implant:
                        break;
                    case CategoryTypes.Deployable:
                        break;
                    case CategoryTypes.Structure:
                        break;
                    case CategoryTypes.Reaction:
                        break;
                    case CategoryTypes.Asteroid:
                        break;
                    case CategoryTypes.Interiors:
                        break;
                    case CategoryTypes.Placeables:
                        break;
                    case CategoryTypes.Abstract:
                        break;
                    case CategoryTypes.Subsystem:
                        break;
                    case CategoryTypes.Ancient_Relics:
                        break;
                    case CategoryTypes.Decryptors:
                        break;
                    case CategoryTypes.Infrastructure_Upgrades:
                        break;
                    case CategoryTypes.Sovereignty_Structures:
                        break;
                    case CategoryTypes.Planetary_Interaction:
                        break;
                    case CategoryTypes.Planetary_Resources:
                        break;
                    case CategoryTypes.Planetary_Commodities:
                        break;
                    default:
                        break;
                }
            }

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

        #region Grab detail about a type and save to mongo
        private object DoShip(BaseType baseType)
        {
            var data = (from ta in dataContext.dgmTypeAttributes
                        where ta.typeID == baseType.InvType.typeID
                        join at in dataContext.dgmAttributeTypes on ta.attributeID equals at.attributeID
                        join ac in dataContext.dgmAttributeCategories on at.categoryID equals ac.categoryID
                        join uc in dataContext.eveUnits on at.unitID equals uc.unitID
                        select new
                        {
                            typeID = ta.typeID,
                            valueInt = ta.valueInt,
                            valueFloat = ta.valueFloat,

                            attributeID = ta.attributeID,
                            attributeName = at.attributeName,
                            description = at.description,
                            iconID = at.iconID,
                            defaultValue = at.defaultValue,
                            published = at.published,
                            displayName = at.displayName,
                            stackable = at.stackable,
                            highIsGood = at.highIsGood,

                            categoryID = at.categoryID,
                            categoryName = ac.categoryName,
                            categoryDescription = ac.categoryDescription,

                            unitID = at.unitID,
                            unitName = uc.unitName,
                            unitDisplayName = uc.displayName,
                            unitDescription = uc.description
                        });
                        //select new { ta = ta, at = at, ac = ac, uc = uc });
     
  
            var ship = new
            {
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = data
            };

            //dataContext.dgmAttributeTypes
            return ship;
        }

        #endregion
    }

    class BaseType
    {
        public invType InvType { get; set; }
        public invGroup InvGroup { get; set; }
        public invCategory InvCategory { get; set; }
    }
}
