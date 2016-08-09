var glob = require('glob');
var jsonfile = require('jsonfile');
var lineReader = require('line-reader');

jsonfile.spaces = 4;
glob('csv/*.csv', (err, csvFiles) => csvFiles.forEach(csvFile => {
    let days = {};
    lineReader.eachLine(csvFile, { encoding: 'utf8' }, (line, last) => {
        let data = line.split(';').map(v => v.substring(1, v.length - 1));
        let [day, word, meaning, level] = [parseInt(data[1], 10), data[2], data[3], parseInt(data[4], 10)];

        days[day] = days[day] || { _id: day, words: [] };
        days[day].words.push({ word, meaning, level });

        if(last){
            let json = { collections: [] };
            for(let i of Object.keys(days)) json.collections.push(days[i]);
            jsonfile.writeFileSync(csvFile.replace(/csv/, "json"), json);
        }
    });
}));
