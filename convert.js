import Promise from 'bluebird'

import { fs } from 'mz'
import glob from 'glob-promise'
import readline from 'readline-promise'

import names from './names'
import { MongoClient } from 'mongodb'

let log = (...args) => console.log(new Date().toISOString(), ...args)
let error = (error) => console.error(new Date().toISOString(), error)

async function convert () {
  const start = new Date()

  // 로컬 MongoDB에 연결합니다.
  const db = await MongoClient.connect('mongodb://localhost:27017/voca')
  log(`MongoClient.connect`)

  // books, days 컬렉션을 초기화합니다
  const [books, days] = await Promise.map(['books', 'days'], async (name) => {
    const col = await db.createCollection(name)
    log(`db.createCollection`, name)

    await col.deleteMany({})
    log(`col.deleteMany`, name)

    return col
  })

  // 단어장 CSV 파일 목록을 가져옵니다.
  const files = await glob('csv/*.csv')
  log(`glob`, files.length, files.join())

  // 각각의 파일들로부터 책 데이터를 읽어 옵니다.
  const booksMap = await Promise.map(files, async (file) => {
    // 단어들을 모아 두는 맵
    const dayMap = new Map()

    // 파일 경로에서 책 이름을 추출합니다. ex) "csv/abc.csv" -> "abc"
    const bookId = file.replace(/(csv|\.|\/)/g, '')

    // 파일 스트림을 엽니다.
    const reader = readline.createInterface({
      terminal: false,
      input: fs.createReadStream(file)
    })

    // 파일을 한 줄씩 읽어요!
    await reader.each(line => {
      // "a";"b";"c" 형식의 CSV 파일입니다.
      const data = line.split(';').map(v => v.substring(1, v.length - 1))

      const [dayId, wordId, meaning, level] = [
        parseInt(data[1], 10), // 회차
        data[2].trim().toLowerCase(), // 단어 (소문자)
        data[3].trim().toLowerCase(), // 뜻 (한국어)
        parseInt(data[4], 10) // 난이도 (1-3)
      ]

      if (!dayMap.has(dayId)) {
        // 회차 객체를 초기화합니다.
        dayMap.set(dayId, { book: bookId, id: dayId, words: [] })
      }

      // 단어들을 같은 회차인 것들끼리 모읍니다.
      dayMap.get(dayId).words.push({ id: wordId, meaning, level })
    })
    log(`readline.each`, file)

    // 책의 모든 회차를 DB에 집어넣습니다.
    const { insertedCount: count } = await days.insert([...dayMap.values()])
    log(`days.insert`, bookId, count)

    // 책 커버 이미지를 base64로 인코딩합니다.
    const buffer = new Buffer(await fs.readFile(`img/${bookId}.jpg`))
    const image = 'data:image/jpeg;base64,' + buffer.toString('base64')
    log(`fs.readFile`, bookId, buffer.length)

    // 책 객체를 만들어서 리턴합니다.
    return { id: bookId, name: names[bookId], count, image }
  })

  // 책 정보를 DB에 집어넣습니다.
  const res = await books.insert(booksMap)
  log(`books.insert`, res.insertedCount)

  // 끝!
  await db.close()
  log(`db.close`, new Date() - start)
}

try { convert() } catch (e) { error(e) }
