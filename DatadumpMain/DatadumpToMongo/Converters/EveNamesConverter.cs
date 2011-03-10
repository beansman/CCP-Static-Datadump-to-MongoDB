using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace DatadumpToMongo.Converters
{
    /// <summary>
    /// Not used!
    /// </summary>
    class EveNamesConverter : IConverter
    {
        enum category
        {
            Names = 1,
            Celestials = 2,

        }
        enum GroupNames
        {
            Character = 1,
            Corporation = 2,
            Faction=19,
            // Wtf?
            Neutral_Object_Oriented_bastards=32,
        }

        enum GroupCelestials
        {
            Region = 3,
            Constellation=4,
            Solarsystem=5,
            Star=6,
            Planet=7,
            Moon=8,
            Asteroid_Belt=9,
            Station=15
        }


        private void DumpEveNames()
        {
            var data = from name in dataContext.eveNames
                       select new BaseName
                       {
                           // Cast to non-nullable (we are almost sure these are set)
                           categoryID = (byte)name.categoryID,
                           groupID = (short)name.groupID,
                           itemID = name.itemID,
                           itemName = name.itemName,
                           typeID = (int)name.typeID
                       };

            // Combine the data into one document with correct structure
            foreach (var item in data)
            {
                object document = null;
                switch ((category)item.categoryID)
                {
                    case category.Names:
                        if (Debug) Utilities.ConsoleWriter("Parsing Name: " + item.itemName);
                        document = DoName(item);
                        break;
                    case category.Celestials:
                        if (Debug) Utilities.ConsoleWriter("Parsing Celestial: " + item.itemName);
                        document = DoCelestial(item);
                        break;
                    default:
                        if (Debug) Utilities.ConsoleWriter("Unkown type: " + item.itemName);
                        break;
                }

                // CHeck for "null" documents
                if (document != null)
                {
                    this.mongoCollection.Insert(document.ToBsonDocument());
                }
            }
        }

        private object DoName(BaseName name)
        {
            switch ((GroupNames)name.groupID)
            {
                case GroupNames.Character:
                case GroupNames.Corporation:
                case GroupNames.Faction:
                case GroupNames.Neutral_Object_Oriented_bastards:
                default:
                    return DoUnknown(name);
            }
        }

        private object DoCelestial(BaseName name)
        {
            switch ((GroupCelestials)name.groupID)
            {
                case GroupCelestials.Region:
                case GroupCelestials.Constellation:
                case GroupCelestials.Star:
                case GroupCelestials.Planet:
                case GroupCelestials.Moon:
                case GroupCelestials.Asteroid_Belt:
                case GroupCelestials.Station:
                default:
                    return DoUnknown(name);
                case GroupCelestials.Solarsystem:
                    return DoSystem(name);
            }
        }


        /*if (Debug) Utilities.ConsoleWriter("Parsing agent: " + item.Agents.agentID);
                BsonDocument document = item.System.ToBsonDocument();
                document.Add("Constellation", item.Constellation.ToBsonDocument());
                document.Add("Region", item.Region.ToBsonDocument());
                document.Add("uniqueID", new BsonInt64(item.System.solarSystemID));
                if (document != null)
                    this.mongoCollection.Insert(document);*/

        #region Specific parsers
        private object DoUnknown(BaseName name)
        {
            return new
            {
                uniqueID = name.itemID,
                uniqueName = name.itemName,
                name.typeID,
                name.itemName,
                name.itemID,
                name.groupID,
                category = name.categoryID
            };
        }

        private object DoNpc(BaseName name)
        {
            // Grab agents
            var agents = from i in dataContext.agtAgents
                         where i.agentID == name.itemID
                         // Join info about agents (qual)
                         join c in dataContext.agtConfigs on i.agentID equals c.agentID
                         join r in dataContext.agtResearchAgents on i.agentID equals r.agentID
                         join t in dataContext.agtAgentTypes on i.agentTypeID equals t.agentTypeID
                         join n in dataContext.eveNames on i.agentID equals n.itemID
                         select new
                         {
                             Agents = i,
                             Configs = c,
                             ResearchAgents = r
                         };
            return agents.ToList();
        }

        private object DoSystem(BaseName name)
        {
            var system = from i in dataContext.mapSolarSystems
                         where i.solarSystemID == name.itemID
                         from c in dataContext.mapConstellations
                         where i.constellationID == c.constellationID
                         from r in dataContext.mapRegions
                         where i.regionID == r.regionID
                         select new
                         {
                             uniqueID = i.solarSystemID,
                             uniqueName = i.solarSystemName,
                             i.solarSystemID,
                             i.solarSystemName,
                             i.border,
                             i.constellation,
                             i.constellationID,
                             i.corridor,
                             i.factionID,
                             i.fringe,
                             i.hub,
                             i.international,
                             i.luminosity,
                             i.radius,
                             i.regional,
                             i.regionID,
                             i.security,
                             i.securityClass,
                             i.sunTypeID,
                             i.x,
                             i.xMax,
                             i.xMin,
                             i.y,
                             i.yMax,
                             i.yMin,
                             i.z,
                             i.zMax,
                             i.zMin,
                             Constellation = c,
                             Region = r
                         };
            return (system.Count() > 0) ? system.First() : null;
        }
        #endregion

        #region IParser Members

        public void DoParse()
        {
            DumpEveNames();
        }

        public bool Debug { get; set; }

        public MongoDB.Driver.MongoCollection mongoCollection { get; set; }

        public SDDDataContext dataContext { get; set; }

        #endregion
    }

    /// <summary>
    /// Holding class for passing EveNames stuff around
    /// </summary>
    class BaseName
    {
        public Int64 itemID { get; set; }
        public String itemName { get; set; }
        public byte categoryID { get; set; }
        public int groupID { get; set; }
        public int typeID { get; set; }
    }
}
