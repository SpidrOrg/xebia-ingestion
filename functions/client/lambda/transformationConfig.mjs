import dateFns from "date-fns";
import {DB_DATE_FORMAT} from "./constants.mjs";
import _ from "lodash";

 //setting primary key for programming struct.
 //change date format.
//setting default values to 0 for numbers

export default {
  actuals: {
    rawFileKey: () => `raw-data/JDA_baseline.csv`,
    transformFileKey: () => `tranform-data/client_actuals/client_actuals.csv`,
    primaryKeyIndexes: [1, 2, 6],          //setting primary key for programming struct.
    lineTransformationConfig: [{}, {
      transformer: v => {
        const dateP = dateFns.parse(v, "d-MMM-yy", new Date())   //change date format.
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    }, {
      default: () => 0     //setting default values to 0 for numbers
    }, {
      default: () => 0
    },{
      default: () => 0
    },{},{
      default: () => 0
    }, {}, {}, {
      default: () => 0
    }]
  },
  forecast: {
    //s3bucket: "spi-3184919584-abccorp",
    rawFileKey: () => `raw-data/JDA_new.csv`,
    transformFileKey: () => `tranform-data/client_forecast/client_forecast.csv`,
    primaryKeyIndexes: [2, 3, 4, 5, 13, 14],      //setting primary key for programming struct.
    lineTransformationConfig: [{},{},{
      transformer: v => {
        const dateP = dateFns.parse(v, "d-MMM-yy", new Date())      //change date format.
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    }, {
      transformer: v => {
        const dateP = dateFns.parse(v, "MMM-dd-yyyy", new Date())
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    }, {
      transformer: v => {
        const dateP = dateFns.parse(v, "d-MMM-yy", new Date())
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    }, {}, {}, {
      default: () => 0            //setting default values to 0 for numbers
    },{
      default: () => 0
    }, {
      default: () => 0
    }, {
      default: () => 0
    }, {
      default: () => 0
    }, {}, {}]
  },
  pricePerUnit: {
    //s3bucket: "spi-3184919584-abccorp",
    rawFileKey: (event)=>{
      const fileKey = _.get(event, "Records[0].s3.object.key");
      if (_.includes(["raw-data/Price_by_customer.csv", "raw-data/Price_all_customer.csv"],  fileKey)){
        return fileKey;
      }
    },
    transformFileKey: () => `tranform-data/client_price_per_unit/client_price_per_unit.csv`,
    primaryKeyIndexes: [1, 2, 6],
    lineTransformationConfig: [{}, {
      transformer: v => {
        const dateP = dateFns.parse(v, "M/d/yyyy", new Date())
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    }, {
      default: () => 0
    },{
      default: () => 0
    }, {
      default: () => 0
    },{}, {}]
  },
  marketShare: {
    //s3bucket: "spi-3184919584-abccorp",
    rawFileKey: () => `raw-data/Market_share.csv`,
    transformFileKey: () => `tranform-data/client_market_share/client_market_share.csv`,
    primaryKeyIndexes: [2, 15],
    lineTransformationConfig: [{}, {
      transformer: v => {
        const dateP = dateFns.parse(_.split(v, " ")[0], "M/d/yyyy", new Date())
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    },{},{},{},{
      default: () => 0
    },{
      default: () => 0
    },{},{
      default: () => 0
    },{
      default: () => 0
    },{},{},{},{},{
      transformer: v => {
        const dateP = dateFns.parse(v, "d-MMM-yy", new Date())
        return dateFns.format(dateP, DB_DATE_FORMAT);
      }
    },{},{},{},{}]
  }
}
