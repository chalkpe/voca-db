const colName = 'days';

var glob = require('glob');
var lineReader = require('line-reader');

let log = (...args) => console.log(new Date().toISOString(), ...args);
let err = (error) => console.error(new Date().toISOString(), error);

require('mongodb').MongoClient.connect('mongodb://localhost:27017/voca', (err, db) => {
    if(err) return console.error(err);
    log("MongoClient.connect");

    db.createCollection(colName, (err, col) => {
        if(err) return console.error(err);
        log("db.createCollection", colName);

        col.deleteMany({}, (err, result) => {
            if(err) return console.error(err);
            log("col.deleteMany", colName);

            glob('csv/*.csv', (err, csvFiles) => {
                if(err) return console.error(err);
                log("glob", csvFiles);

                csvFiles.forEach(csvFile => {
                    let dayMap = {};
                    let bookName = csvFile.replace(/(csv|\.|\/)/g, '');
                    log("csvFiles.forEach", colName, bookName);

                    lineReader.eachLine(csvFile, { encoding: 'utf8' }, (line, last) => {
                        let data = line.split(';').map(v => v.substring(1, v.length - 1));
                        let [day, word, meaning, level] = [parseInt(data[1], 10), data[2], data[3], parseInt(data[4], 10)];

                        dayMap[day] = dayMap[day] || { book: bookName, day, words: [] };
                        dayMap[day].words.push({ word, meaning, level });

                        if(last) col.insert(Object.keys(dayMap).map(k => dayMap[k]), (err, result) => {
                            if(err) return console.log(err);
                            log("col.insert", colName, bookName, result.insertedCount);
                        });
                    });
                });
            });
        });
    });
});
