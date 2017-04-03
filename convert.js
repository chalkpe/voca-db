import Promise from 'bluebird'

import { fs } from 'mz'
import glob from 'glob-promise'
import readline from 'readline-promise'

import names from './names'
import { MongoClient } from 'mongodb'

let log = (...args) => console.log(new Date().toISOString(), ...args)
let error = (error) => console.error(new Date().toISOString(), error)

async function convert () {
  const db = await MongoClient.connect('mongodb://localhost:27017/voca')
  log(`MongoClient.connect`)

  const [books, days] = await Promise.map(['books', 'days'], async (name) => {
    const col = await db.createCollection(name)
    log(`db.createCollection`, name)

    await col.deleteMany({})
    log(`col.deleteMany`, name)

    return col
  })

  const files = await glob('csv/*.csv')
  log(`glob`, files.join())

  await Promise.map(files, async (file) => {
    let dayMap = {}
    let bookId = file.replace(/(csv|\.|\/)/g, '')

    let reader = readline.createInterface({
      terminal: false, input: fs.createReadStream(file) })

    await reader.each(line => {
      let data = line.split(';').map(v => v.substring(1, v.length - 1))
      let [dayId, wordId, meaning, level] = [
        parseInt(data[1], 10),
        data[2].trim().toLowerCase(),
        data[3].trim(),
        parseInt(data[4], 10)
      ]

      dayMap[dayId] = dayMap[dayId] || { book: bookId, id: dayId, words: [] }
      dayMap[dayId].words.push({ id: wordId, meaning, level })
    })
    log(`readline.each`, file)

    const { insertedCount: count } = await days.insert(Object.values(dayMap))
    log(`days.insert`, bookId, count)

    const image = 'data:image/jpegbase64,' +
      new Buffer(await fs.readFile(`img/${bookId}.jpg`)).toString('base64')

    log(`fs.readFile`, bookId)

    const res = await books.insert({ id: bookId, name: names[bookId], count, image })
    log(`books.insert`, bookId, res.insertedCount)
  })

  await db.close()
  console.log('Everything done!')
}

try { convert() } catch (e) { error(e) }
