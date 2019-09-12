const readline = require("readline");
const fs = require("fs");
const mongoose = require("mongoose");

//connection
mongoose.connect("mongodb://localhost:27017/m201", { useNewUrlParser: true });

const Schema = mongoose.Schema;
const ObjectId = Schema.ObjectId;

//  = new Schema(
//   {},
//   { strict: false, collection: "open_postcode_geo" }
// );

const PostCode = mongoose.model(
  "PostCode",
  new Schema({
    postcode: String,
    status: String,
    usertype: String,
    easting: Number,
    northing: Number,
    positional_quality_indicator: Number,
    country: String,
    latitude: Number,
    longitude: Number,
    postcode_no_space: String,
    postcode_fixed_width_seven: String,
    postcode_fixed_width_eight: String,
    postcode_area: String,
    postcode_district: String,
    postcode_sector: String,
    outcode: String,
    incode: String
  }),
  "open_postcode_geo"
);

var writeStream = fs.createWriteStream("./output/output.csv", { flags: "a" });

// create instance of readline
// each instance is associated with single input stream
let rl = readline.createInterface({
  input: fs.createReadStream("./postcodes.csv")
});

let line_no = 0;

// event is emitted after each line
rl.on("line", async function(line) {
  try {
    if (line == "Postcode") {
      //do nothing
    } else {
      let postcode = await PostCode.findOne({
        postcode_no_space: line.toUpperCase()
      }).exec();
      if (postcode && postcode.postcode_no_space) {
        const dataToWrite = `${postcode.postcode_no_space},${postcode.latitude},${postcode.longitude}\n`;
        writeStream.write(dataToWrite, function(err) {
          // Now the data has been written.
          if (err) {
            console.error(err);
          } else {
            console.log(
              `${postcode.postcode_no_space}: ${line_no} written to file...`
            );
          }
          line_no++;
        });
      }else{
        line_no++;
      }
    }
  } catch (err) {
    console.error(err);
    line_no++;
  }
});

// end
rl.on("close", function(line) {
  console.log("Total lines : " + line_no);
});
