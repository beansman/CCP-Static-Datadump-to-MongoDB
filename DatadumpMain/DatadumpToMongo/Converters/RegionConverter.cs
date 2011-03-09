using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace DatadumpToMongo.Converters
{
    /// <summary>
    /// Convert regions to mongo
    /// </summary>
    class RegionConverter : IConverter
    {
        private void DumpMapRegions()
        {
            var systems = from i in dataContext.mapRegions
                          select new
                          {
                              i.regionID,
                              i.regionName,
                              i.factionID,
                              i.radius,
                              i.x,
                              i.xMax,
                              i.xMin,
                              i.y,
                              i.yMax,
                              i.yMin,
                              i.z,
                              i.zMax,
                              i.zMin,
                              Constellations = (from c in dataContext.mapConstellations
                                               where i.regionID == c.regionID
                                               select c).ToList(),
                              Systems = (from s in dataContext.mapSolarSystems
                                        where i.regionID == s.regionID
                                        select s).ToList()
                          };
            
            foreach (var item in systems)
            {
                if (Debug) Utilities.ConsoleWriter("Parsing region: " + item.regionName);
                if (item != null)
                    this.mongoCollection.Insert(item);
            }
        }


        #region IParser Members

        public void DoParse()
        {
            DumpMapRegions();
        }

        public bool Debug { get; set; }

        public MongoDB.Driver.MongoCollection mongoCollection { get; set; }

        public SDDDataContext dataContext { get; set; }

        #endregion
    }
}
