using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Bson;

namespace DatadumpToMongo.Converters
{
    class SolarsystemConverter:IConverter
    {

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
                if (document != null)
                    this.mongoCollection.Insert(document);
            }
        }


        #region IParser Members

        public void DoParse()
        {
            DumpMapSolarsystems();
        }

        public bool Debug { get; set; }

        public MongoDB.Driver.MongoCollection mongoCollection { get; set; }

        public SDDDataContext dataContext { get; set; }

        #endregion
    }
}
