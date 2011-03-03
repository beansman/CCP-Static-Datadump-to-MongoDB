using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;

namespace DatadumpToMongo.Converters
{
    public interface IConverter
    {
        void DoParse();
        bool Debug { get; set; }
        MongoCollection mongoCollection { get; set; }
        SDDDataContext dataContext { get; set; }
    }
}
