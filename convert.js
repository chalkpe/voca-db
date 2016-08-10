var glob = require('glob');
var jsonfile = require('jsonfile');
var lineReader = require('line-reader');
var MongoClient = require('mongodb').MongoClient;

jsonfile.spaces = 4;

MongoClient.connect('mongodb://localhost:27017/voca', (err, db) => {
    if(err) return console.log(err);

    glob('csv/*.csv', (err, csvFiles) => {
        if(err) return console.log(err);
        console.log("glob", csvFiles);

        csvFiles.forEach(csvFile => {
            let days = {};
            let colName = 'days_' + csvFile.replace(/(csv|\.|\/)/g, '');

            console.log(colName);

            db.createCollection(colName, (err, col) => {
                if(err) return console.log(err);
                console.log(colName, "createCollection");

                lineReader.eachLine(csvFile, { encoding: 'utf8' }, (line, last) => {
                    let data = line.split(';').map(v => v.substring(1, v.length - 1));
                    let [day, word, meaning, level] = [parseInt(data[1], 10), data[2], data[3], parseInt(data[4], 10)];

                    days[day] = days[day] || { _id: day, words: [] };
                    days[day].words.push({ word, meaning, level });

                    if(last) col.deleteMany({}, (err, result) => {
                        if(err) return console.log(err);
                        console.log(colName, "deleteMany");

                        col.insert(Object.keys(days).map(k => days[k]), (err, result) => {
                            if(err) return console.log(err);
                            console.log(colName, "insert", result.insertedCount);
                        });
                    });
                });
            });
        });
    });
});
