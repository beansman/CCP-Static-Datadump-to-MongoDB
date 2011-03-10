using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;

namespace DatadumpToMongo.Converters
{
    public interface IConverter
    {
        /// <summary>
        /// Do the parsing
        /// </summary>
        void DoParse();
        /// <summary>
        /// Console Debug on/off
        /// </summary>
        bool Debug { get; set; }
        /// <summary>
        /// The collection to insert the data to
        /// </summary>
        MongoCollection mongoCollection { get; set; }
        /// <summary>
        /// The mssql datacontext
        /// </summary>
        SDDDataContext dataContext { get; set; }
    }
}
