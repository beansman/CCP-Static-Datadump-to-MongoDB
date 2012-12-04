using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace DatadumpToMongo.Converters
{
    class InvTypeConverter:IConverter
    {
        /// <summary>
        /// Categories as taken from invCategories
        /// </summary>
        enum CategoryTypes
        {
            _System = 0,
            Owner = 1,
            Celestial = 2,
            Station = 3,
            Material = 4,
            Accessories = 5,
            Ship = 6,
            Module = 7,
            Charge = 8,
            Blueprint = 9,
            Trading = 10,
            Entity = 11,
            Bonus = 12,
            Skill = 16,
            Commodity = 17,
            Drone = 18,
            Implant = 20,
            Deployable = 22,
            Structure = 23,
            Reaction = 24,
            Asteroid = 25,
            Interiors = 26,
            Placeables = 27,
            Abstract = 29,
            Subsystem = 32,
            Ancient_Relics = 34,
            Decryptors = 35,
            Infrastructure_Upgrades = 39,
            Sovereignty_Structures = 40,
            Planetary_Interaction = 41,
            Planetary_Resources = 42,
            Planetary_Commodities = 43,
        }

        #region IConverter Members

        public void DoParse()
        {
            DumpInvType();
        }

        public bool Debug { get; set; }

        public MongoDB.Driver.MongoCollection mongoCollection { get; set; }

        public SDDDataContext dataContext { get; set; }

        #endregion

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

        #region Grab detail about a type and save to mongo
        /// <summary>
        /// Parse an unknown type from invType
        /// </summary>
        /// <param name="baseType"></param>
        /// <returns></returns>
        private object DoUnknown(BaseType baseType)
        {
            // Fetch the attributes
            var attributes = DoAttributes(baseType.InvType.typeID);
            // Fetch effects?
            var effects = DoEffects(baseType.InvType.typeID);

            // Combine it all in a great mashup!
            var unknown = new
            {
                //uniqueID = baseType.InvType.typeID,
                //uniqueName = baseType.InvType.typeName,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                //radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                //graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = attributes,
                Effects = effects
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
            var attributes = DoAttributes(baseType.InvType.typeID);

            // Fetch effects?
            var effects = DoEffects(baseType.InvType.typeID);

            // Combine it all in a great mashup!
            var module = new
            {
                //uniqueID = baseType.InvType.typeID,
                //uniqueName = baseType.InvType.typeName,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                //radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                //graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = attributes,
                Effects = effects
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
        {// Fetch the attributes
            var attributes = DoAttributes(baseType.InvType.typeID);

            // Fetch effects?
            var effects = DoEffects(baseType.InvType.typeID);

            // Combine it all in a great mashup!
            var ship = new
            {
                //uniqueID = baseType.InvType.typeID,
                //uniqueName = baseType.InvType.typeName,
                typeID = baseType.InvType.typeID,
                typeName = baseType.InvType.typeName,
                volume = baseType.InvType.volume,
                //radius = baseType.InvType.radius,
                raceID = baseType.InvType.raceID,
                published = baseType.InvType.published,
                portionSize = baseType.InvType.portionSize,
                mass = baseType.InvType.mass,
                marketGroupID = baseType.InvType.marketGroupID,
                // This is a method call! Grabs marketgroup untill parentgroupid == null
                marketGroup = DoMarketGroup(baseType.InvType.marketGroupID),
                iconID = baseType.InvType.iconID,
                groupID = baseType.InvType.groupID,
                //graphicID = baseType.InvType.graphicID,
                description = baseType.InvType.description,
                chanceOfDuplicating = baseType.InvType.chanceOfDuplicating,
                capacity = baseType.InvType.capacity,
                basePrice = baseType.InvType.basePrice,
                Group = baseType.InvGroup,
                Category = baseType.InvCategory,
                Attributes = attributes,
                Effects = effects
            };
            //System.IO.File.WriteAllText(@"C:\json.txt", ship.ToJson(set));  
            return ship;
        }

        #region Helper stuff
        /// <summary>
        /// Get attributes for an invType item
        /// </summary>
        /// <param name="typeID"></param>
        /// <returns></returns>
        private object DoAttributes(int typeID)
        {
            var data = (from ta in dataContext.dgmTypeAttributes
                        where ta.typeID == typeID
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

            return data.ToList();
        }

        /// <summary>
        /// Get the effects for an invType item
        /// </summary>
        /// <param name="typeID"></param>
        /// <returns></returns>
        private object DoEffects(int typeID)
        {
            var data = from ef in dataContext.dgmTypeEffects
                       where ef.typeID == typeID
                       join f in dataContext.dgmEffects on ef.effectID equals f.effectID
                       select new
                       {
                           effectdID = ef.effectID,
                           isDefault = ef.isDefault,
                           f.description,
                           f.disallowAutoRepeat,
                           f.dischargeAttributeID,
                           f.displayName,
                           f.distribution,
                           f.durationAttributeID,
                           f.effectCategory,
                           f.effectName,
                           f.electronicChance,
                           f.falloffAttributeID,
                           f.fittingUsageChanceAttributeID,
                           f.guid,
                           f.iconID,
                           f.isAssistance,
                           f.isOffensive,
                           f.isWarpSafe,
                           f.npcActivationChanceAttributeID,
                           f.npcUsageChanceAttributeID,
                           f.postExpression,
                           f.preExpression,
                           f.propulsionChance,
                           f.published,
                           f.rangeAttributeID,
                           f.rangeChance,
                           f.sfxName,
                           f.trackingSpeedAttributeID
                       };
            return data.ToList();
        }

        /// <summary>
        /// Get the marketgroup document
        /// </summary>
        /// <param name="marketGroupID">marketgroupid</param>
        /// <returns></returns>
        private object DoMarketGroup(int? marketGroupID)
        {
            // Yes, bad recursion! Should catch this before doing another one. But this is easier!
            if (marketGroupID == null) 
                return null;

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
            return marketgroup.Single();
        }
        #endregion
        #endregion
    }
    
    /// <summary>
    /// Holding class for initial search
    /// </summary>
    class BaseType
    {
        public invType InvType { get; set; }
        public invGroup InvGroup { get; set; }
        public invCategory InvCategory { get; set; }
    }
}
