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
    /// 
    /// All items will have a unique itemID called uniqueID
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
            
            // Do extracted stuff.

            DumpMapSolarsystems();

            DumpInvType();

            TimeSpan dtSpan = DateTime.Now - dtStart;
            Utilities.ConsoleWriter("Mongo contains: " + this.mongoCollection.Count() + " documents");
            Utilities.ConsoleWriter("Took " + dtSpan.TotalSeconds + "s");

            return true;
        }

        private void DumpMapSolarsystems()
        {
            var systems = from i in dataContext.mapSolarSystems
                          join c in dataContext.mapConstellations on i.constellationID equals c.constellationID
                          join r in dataContext.mapRegions on i.regionID equals r.regionID
                          select new
                          {
                              System = i,
                              Constellation = c,
                              Region = r
                          };

            foreach (var item in systems)
            {
                if (Debug) Utilities.ConsoleWriter("Parsing solarsystem: " + item.System.solarSystemName);
                BsonDocument document = item.System.ToBsonDocument();
                document.Add("Constellation", item.Constellation.ToBsonDocument());
                document.Add("Region", item.Region.ToBsonDocument());
                document.Add("uniqueID", new BsonInt64(item.System.solarSystemID));
                if (item != null)
                    this.mongoCollection.Insert(document);
            }
        }

        private void DumpInvType()
        {
            // InvTypes
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
                if (Debug) Utilities.ConsoleWriter("Parsing " + Enum.GetName(typeof(CategoryTypes), (CategoryTypes)item.InvCategory.categoryID) + ": " + item.InvType.typeName);

                object document = null;
                switch ((CategoryTypes)item.InvCategory.categoryID)
                {
                    case CategoryTypes._System:
                    case CategoryTypes.Owner:
                    case CategoryTypes.Celestial:
                    case CategoryTypes.Station:
                    case CategoryTypes.Material:
                    case CategoryTypes.Accessories:
                    case CategoryTypes.Charge:
                    case CategoryTypes.Blueprint:
                    case CategoryTypes.Trading:
                    case CategoryTypes.Entity:
                    case CategoryTypes.Bonus:
                    case CategoryTypes.Skill:
                    case CategoryTypes.Commodity:
                    case CategoryTypes.Drone:
                    case CategoryTypes.Implant:
                    case CategoryTypes.Deployable:
                    case CategoryTypes.Structure:
                    case CategoryTypes.Reaction:
                    case CategoryTypes.Asteroid:
                    case CategoryTypes.Interiors:
                    case CategoryTypes.Placeables:
                    case CategoryTypes.Abstract:
                    case CategoryTypes.Subsystem:
                    case CategoryTypes.Ancient_Relics:
                    case CategoryTypes.Decryptors:
                    case CategoryTypes.Infrastructure_Upgrades:
                    case CategoryTypes.Sovereignty_Structures:
                    case CategoryTypes.Planetary_Interaction:
                    case CategoryTypes.Planetary_Resources:
                    case CategoryTypes.Planetary_Commodities:
                    default:
                        // ALl that doesn't fall under a specific category we will let drop through and get handled here
                        document = DoUnknown(item);
                        break;
                    case CategoryTypes.Ship:
                        document = DoShip(item);
                        break;
                    case CategoryTypes.Module:
                        document = DoModule(item);
                        break;
                    
                }

                // Only insert if document is filled
                if (document != null)
                    this.mongoCollection.Insert(document.ToBsonDocument());
            }
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

        #region Grab detail about a type and save to mongo
        /// <summary>
        /// Parse an unknown type from invType
        /// </summary>
        /// <param name="baseType"></param>
        /// <returns></returns>
        private object DoUnknown(BaseType baseType)
        {
            // Fetch the attributes
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

            // Fetch effects?

            // Combine it all in a great mashup!
            var unknown = new
            {
                uniqueID = baseType.InvType.typeID,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                //Attributes = data.ToList()
            };
            //System.IO.File.WriteAllText(@"C:\json.txt", ship.ToJson(set));
            return unknown;
        }

        /// <summary>
        /// Parse a module from invType
        /// </summary>
        /// <param name="baseType"></param>
        /// <returns></returns>
        private object DoModule(BaseType baseType)
        {
            // Fetch the attributes
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

            // Fetch effects?

            // Combine it all in a great mashup!
            var module = new
            {
                uniqueID = baseType.InvType.typeID,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = data.ToList()
            };
            //System.IO.File.WriteAllText(@"C:\json.txt", ship.ToJson(set));
            return module;
        }

        /// <summary>
        /// Parse a ship from invType
        /// </summary>
        /// <param name="baseType"></param>
        /// <returns></returns>
        private object DoShip(BaseType baseType)
        {
            // Fetch the attributes
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

            // Fetch effects?
  
            // Combine it all in a great mashup!
            var ship = new
            {
                uniqueID = baseType.InvType.typeID,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = data.ToList()
            };
            //System.IO.File.WriteAllText(@"C:\json.txt", ship.ToJson(set));  
            return ship;
        }
     
   

        /// <summary>
        /// Get the marketgroup document
        /// </summary>
        /// <param name="marketGroupID">marketgroupid</param>
        /// <returns></returns>
        private object DoMarketGroup(short? marketGroupID)
        {
            // Yes, bad recursion! Should catch this before doing another one. But this is easier!
            if (marketGroupID == null) return null;

            // Generate marketgroup tree
            var marketgroup = from m in dataContext.invMarketGroups
                              where m.marketGroupID == marketGroupID
                              select new
                              {
                                  marketGroupID = m.marketGroupID,
                                  marketGroupName = m.marketGroupName,
                                  iconID = m.iconID,
                                  hasTypes = m.hasTypes,
                                  description = m.description,
                                  parentGroup = DoMarketGroup(m.parentGroupID)
                              };
            // Return it, .ToList() is called to force the execution of the above statement
            return marketgroup.ToList();
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
