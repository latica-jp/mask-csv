const fs = require('fs')
const csv = require('fast-csv')
const iconv = require('iconv-lite')

const BOM = '\uFEFF'

module.exports = class Masker {
  constructor(options) {
    this.files = options.files
    this.encodings = options.encodings
    this.columnMaskWords = options.columnMaskWords
    this._initColumnMaskWordFoundMaps()
  }

  _initColumnMaskWordFoundMaps() {
    this.columnMaskWordFoundMaps = {}
    Object.keys(this.columnMaskWords).forEach(columnName => {
      this.columnMaskWordFoundMaps[columnName] = {}
    })
  }

  async mask() {
    // sort files to mask master data first
    this.files.sort(
      (a, b) =>
        Number(a.dataType !== 'master') - Number(b.dataType !== 'master')
    )
    // Mask sequentially, not in parallel
    for (const file of this.files) {
      await this._maskFile(file)
    }
  }

  async _maskFile(file) {
    const hasBOM = await this._checkHasBOM(file.path)
    return new Promise((resolve, reject) => {
      const matches = file.path.match(/(.*\/)(.*?)\.csv$/)
      const writeFileName = `${matches[1]}__${matches[2]}.csv`
      const encodings = { ...this.encodings, ...file.encodings }
      const readEncoding = encodings.input || 'utf8'
      const writeEncoding = encodings.output || 'utf8'

      const ws = fs.createWriteStream(writeFileName)
      // write BOM before pipe if needed
      if ((hasBOM || encodings.outputWithBOM) && writeEncoding === 'utf8')
        ws.write(BOM)
      const csvWs = csv.format({ headers: true, quoteColumns: true })
      csvWs
        .pipe(iconv.decodeStream('utf8'))
        .pipe(iconv.encodeStream(writeEncoding))
        .pipe(ws)

      fs.createReadStream(file.path)
        .pipe(iconv.decodeStream(readEncoding))
        .pipe(csv.parse({ headers: true }))
        .on('data', row => {
          this._maskRow(file, row)
          if (file.blankColumns) this._blankOutRow(file, row)
          if (file.rowTransformer) file.rowTransformer(this, row)
          csvWs.write(row)
        })
        .on('end', () => {
          csvWs.end()
          if (file.postProcessor) file.postProcessor(this)
          resolve()
        })
        .on('error', error => reject(error))
    })
  }

  async _checkHasBOM(filePath) {
    return new Promise((resolve, reject) => {
      const rs = fs.createReadStream(filePath)
      rs.on('readable', () => {
        const maybeBOM = rs.read(3)
        rs.close()
        const hasBOM = maybeBOM.compare(Buffer.from([0xef, 0xbb, 0xbf])) === 0
        resolve(hasBOM)
      }).on('error', error => reject(error))
    })
  }

  _maskRow(file, row) {
    Object.entries(row).forEach(([columnName, word]) => {
      if (word) {
        const columnNameAlias =
          (file.columnNameAliases || {})[columnName] || columnName
        if (typeof columnNameAlias === 'string') {
          this._maskRowColumn(file, row, columnName, columnNameAlias, word)
        } else if (Array.isArray(columnNameAlias)) {
          // column name alias could be multiple
          this._maskRowColumnWithMultipleAliases(
            file,
            row,
            columnName,
            columnNameAlias,
            word
          )
        } else {
          throw new Error(
            `Invalid columnNameAlias ${columnNameAlias} type ${typeof columnNameAlias}`
          )
        }
      }
    })
  }

  _blankOutRow(file, row) {
    Object.keys(row).forEach(columnName => {
      if (file.blankColumns.includes(columnName)) row[columnName] = ''
    })
  }

  _maskRowColumn(file, row, columnName, columnNameAlias, word) {
    if (this._columnHasMaskWords(columnNameAlias)) {
      const maskWord = this._findMaskWord(file, columnNameAlias, word)
      if (maskWord !== null) row[columnName] = maskWord
    }
  }

  _columnHasMaskWords(columnNameAlias) {
    return typeof this.columnMaskWordFoundMaps[columnNameAlias] === 'object'
  }

  _findMaskWord(file, columnNameAlias, word, allowNotFound) {
    const foundMap = this.columnMaskWordFoundMaps[columnNameAlias]
    let maskWord = foundMap[word]
    // if the file type is 'detail', mask word should be taken from found map values
    // found and stored by non-detail type file processed prior to this file
    if (typeof maskWord === 'undefined' && file.dataType !== 'detail') {
      maskWord = this.columnMaskWords[columnNameAlias].shift()
      if (this.columnMaskWords[columnNameAlias].length === 0)
        throw new Error(`Empty words on ${file.path}:${columnNameAlias}`)
      if (typeof maskWord !== 'undefined') foundMap[word] = maskWord
    }
    if (!allowNotFound && typeof maskWord === 'undefined')
      throw new Error(
        `No mask word found for ${word} on ${file.path}:${columnNameAlias}`
      )
    if (maskWord === null)
      throw new Error(
        `Null is not allowed on mask word: ${file.path}:${columnNameAlias}`
      )
    return maskWord
  }

  _maskRowColumnWithMultipleAliases(
    file,
    row,
    columnName,
    columnNameAliases,
    word
  ) {
    const maskWord = columnNameAliases.reduce(
      (foundMaskWord, columnNameAlias) => {
        if (typeof foundMaskWord !== 'undefined') return foundMaskWord
        if (this._columnHasMaskWords(columnNameAlias))
          return this._findMaskWord(file, columnNameAlias, word, true)
        return foundMaskWord
      },
      undefined
    )
    row[columnName] = maskWord
  }
}
