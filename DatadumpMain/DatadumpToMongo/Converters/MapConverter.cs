using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace DatadumpToMongo.Converters
{
    /// <summary>
    /// Convert solarsystems to mongo
    /// </summary>
    class MapConverter : IConverter
    {
        /// <summary>
        /// Converts solarsystems from mapDenormalize and adds subdata
        /// </summary>
        private void DoSolarsystems()
        {
            var systems = (from s in dataContext.mapDenormalizes
                                  where s.groupID == 5
                                  select s).ToList();

            foreach (var item in systems)
            {
                if (Debug) 
                    Utilities.ConsoleWriter("Solarsystem: " + item.itemName);
                var system = new
                {
                    item.itemID,
                    item.itemName,
                    item.celestialIndex,
                    item.constellationID,
                    item.groupID,
                    item.orbitID,
                    item.orbitIndex,
                    item.radius,
                    item.regionID,
                    item.security,
                    item.solarSystemID,
                    item.typeID,
                    item.x,
                    item.y,
                    item.z,
                    Star = (from s in dataContext.mapDenormalizes
                            where s.groupID == 6 && s.solarSystemID == item.itemID
                            select s).Single(),
                    Planets = (from p in dataContext.mapDenormalizes
                               where p.groupID == 7 && p.solarSystemID == item.itemID
                               select p).ToList(),
                    Moons = (from m in dataContext.mapDenormalizes
                             where m.groupID == 8 && m.solarSystemID == item.itemID
                             select m).ToList(),
                    Belts = (from b in dataContext.mapDenormalizes
                             where b.groupID == 9 && b.solarSystemID == item.itemID
                             select b).ToList(),
                    Region = (from r in dataContext.mapDenormalizes
                              where r.groupID == 3 && r.itemID == item.regionID
                              select r).Single(),
                    Jumps = (from j in dataContext.mapDenormalizes
                             where j.groupID == 10 && j.solarSystemID == item.itemID
                             join s in dataContext.mapJumps on j.itemID equals s.stargateID
                             select new
                             {
                                 j.itemID,
                                 j.itemName,
                                 j.celestialIndex,
                                 j.constellationID,
                                 j.groupID,
                                 j.orbitID,
                                 j.orbitIndex,
                                 j.radius,
                                 j.regionID,
                                 j.security,
                                 j.solarSystemID,
                                 j.typeID,
                                 j.x,
                                 j.y,
                                 j.z,
                                 s.stargateID,
                                 s.celestialID
                             }).ToList(),
                    Stations = (from s in dataContext.mapDenormalizes
                                where s.groupID == 15 && s.solarSystemID == item.itemID
                                select s).ToList(),
                    Anomalies = (from a in dataContext.mapDenormalizes
                                 where a.groupID == 995 && a.solarSystemID == item.itemID
                                 select a).ToList(),
                    Constellation = (from c in dataContext.mapDenormalizes
                                     where c.groupID == 4 && c.itemID == item.constellationID
                                     select c).Single()
                };

                if (system != null)
                {
                    this.SystemCollection.Insert(system);
                    //System.IO.File.WriteAllText(@"c:\system.txt", system.ToJson(set));
                }
                //break;
            }
        }
        
        /// <summary>
        /// Converts constellations from mapDenormalize and adds systems and region
        /// </summary>
        private void DoConstellations()
        {
            var constellations = (from c in dataContext.mapDenormalizes
                           where c.groupID == 4
                           select c).ToList();

            foreach (var item in constellations)
            {
                if (Debug) Utilities.ConsoleWriter("Constellations: " + item.itemName);
                var constellation = new
                {
                    item.itemID,
                    item.itemName,
                    item.celestialIndex,
                    item.constellationID,
                    item.groupID,
                    item.orbitID,
                    item.orbitIndex,
                    item.radius,
                    item.regionID,
                    item.security,
                    item.solarSystemID,
                    item.typeID,
                    item.x,
                    item.y,
                    item.z,
                    Region = (from r in dataContext.mapDenormalizes
                                      where r.groupID == 3 && r.itemID == item.regionID
                                      select r).Single(),

                    Solarsystems = (from s in dataContext.mapDenormalizes
                                    where s.groupID == 5 && s.constellationID == item.itemID
                                    select s).ToList()
                };

                if (constellation != null)
                {
                    this.ConstellationCollection.Insert(constellation);
                    //System.IO.File.WriteAllText(@"c:\constellation.txt", constellation.ToJson(set));
                }
                //break;
            }
        }

        /// <summary>
        /// Converts regions from mapDenormalize and adds systems and constellations
        /// </summary>
        private void DoRegions()
        {
            var regions = (from r in dataContext.mapDenormalizes
                          where r.groupID == 3
                          select r).ToList();

            foreach (var item in regions)
            {
                if (Debug) Utilities.ConsoleWriter("Region: " + item.itemName);
                var region = new
                {
                    item.itemID,
                    item.itemName,
                    item.celestialIndex,
                    item.constellationID,
                    item.groupID,
                    item.orbitID,
                    item.orbitIndex,
                    item.radius,
                    item.regionID,
                    item.security,
                    item.solarSystemID,
                    item.typeID,
                    item.x,
                    item.y,
                    item.z,
                    Constellations = (from c in dataContext.mapDenormalizes
                                      where c.groupID == 4 && c.regionID == item.itemID
                                      select c).ToList(),

                    Solarsystems = (from s in dataContext.mapDenormalizes
                                    where s.groupID == 5 && s.regionID == item.itemID
                                    select s).ToList()
                };

                if (region != null)
                {
                    this.RegionCollection.Insert(region);
                    //System.IO.File.WriteAllText(@"c:\region.txt", region.ToJson(set));
                }
                //break;
            }
        }

        /// <summary>
        /// Collection for the regions
        /// </summary>
        public MongoDB.Driver.MongoCollection RegionCollection { get; set; }
        /// <summary>
        /// Collection for the constellations
        /// </summary>
        public MongoDB.Driver.MongoCollection ConstellationCollection { get; set; }
        /// <summary>
        /// Collections for the systems
        /// </summary>
        public MongoDB.Driver.MongoCollection SystemCollection { get; set; }

        MongoDB.Bson.IO.JsonWriterSettings set = new MongoDB.Bson.IO.JsonWriterSettings() { OutputMode = MongoDB.Bson.IO.JsonOutputMode.JavaScript, NewLineChars = "\r\n", Indent = true, IndentChars = "  " };

        #region IParser Members

        /// <summary>
        /// STart the parsing
        /// </summary>
        public void DoParse()
        {
            // kinda hacky, but needed to fit the interface ;)
            // grab the extra collections we need by using the database referenced in the given collection
            RegionCollection = mongoCollection.Database.GetCollection("Kingboard_EveRegion");
            ConstellationCollection = mongoCollection.Database.GetCollection("Kingboard_EveConstellation");
            SystemCollection = mongoCollection;

            // Do the actual parsing
            DoSolarsystems();
            DoRegions();
            DoConstellations();
        }

        public bool Debug { get; set; }

        public MongoDB.Driver.MongoCollection mongoCollection { get; set; }

        public SDDDataContext dataContext { get; set; }
        #endregion
    }
}
