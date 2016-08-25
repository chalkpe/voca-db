/* jshint node: true, esversion: 6 */

let [fs, glob, lineReader, names] = ['fs', 'glob', 'line-reader', './names.json'].map(p => require(p));

let log = (...args) => console.log(new Date().toISOString(), ...args);
let error = (error) => console.error(new Date().toISOString(), error);

require('mongodb').MongoClient.connect('mongodb://localhost:27017/voca', (err, db) => {
    if(err) return error(err);
    log(`MongoClient.connect`);

    db.createCollection('books', (err, books) => {
        if(err) return error(err);
        log(`db.createCollection`, `books`);

        books.deleteMany({}, (err, result) => {
            if(err) return error(err);
            log(`books.deleteMany`);

            db.createCollection('days', (err, days) => {
                if(err) return error(err);
                log(`db.createCollection`, `days`);

                days.deleteMany({}, (err, result) => {
                    if(err) return error(err);
                    log(`days.deleteMany`);

                    glob('csv/*.csv', (err, files) => {
                        if(err) return error(err);
                        log(`glob`, files.join());

                        files.forEach(file => {
                            log(`files.forEach`, file);
                            let dayMap = {}, bookId = file.replace(/(csv|\.|\/)/g, '');

                            lineReader.eachLine(file, { encoding: 'utf8' }, (line, last) => {
                                let data = line.split(';').map(v => v.substring(1, v.length - 1));
                                let [day, word, meaning, level] = [parseInt(data[1], 10), data[2], data[3], parseInt(data[4], 10)];

                                (dayMap[day] = dayMap[day] || { book: bookId, day, words: [] }).words.push({ word, meaning, level });

                                if(last) days.insert(Object.keys(dayMap).map(k => dayMap[k]), (err, result) => {
                                    if(err) return error(err);
                                    log(`days.insert`, bookId, result.insertedCount);

                                    fs.readFile(`img/${bookId}.jpg`, (err, data) => {
                                        if(err) return error(err);
                                        log(`fs.readFile`, bookId);

                                        books.insert({ id: bookId, name: names[bookId], image: 'data:image/jpeg;base64,' + new Buffer(data).toString('base64') }, (err, result) => {
                                            if(err) return error(err);
                                            log(`books.insert`, bookId, result.insertedCount);
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});
