'use strict'
const { pipelinePromise } = require('streamx')
const { pathToFileURL } = require('url-file-url')
const Localdrive = require('localdrive')
const Realm = require('bare-realm')
const { Interact } = require('pear-terminal')
const Opstream = require('pear-opstream')
const stamp = require('pear-stamp')
const dump = require('pear-dump')
const plink = require('pear-link')
const isTextFile = require('is-text-filetype')
const {
  ERR_PERMISSION_REQUIRED,
  ERR_OPERATION_FAILED,
  ERR_DIR_NONEMPTY,
  ERR_INVALID_TEMPLATE
} = require('pear-errors')

class Init extends Opstream {
  constructor(...args) {
    super((...args) => this.#op(...args), ...args)
  }

  async #op(opts = {}) {
    const ipc = global.Pear?.[global.Pear?.constructor.IPC]
    const { dir, cwd, header, autosubmit, defaults, force = false, pkg } = opts
    let { link = 'default', ask = true } = opts
    const isPear = link.startsWith('pear://')
    const isFile = link.startsWith('file://')
    const isPath = link[0] === '.' || link[0] === '/' || link[1] === ':' || link.startsWith('\\')
    const isName = !isPear && !isFile && !isPath
    if (isName) {
      if (link === 'default') link = 'pear://templates/terminal/default'
      else if (link === 'ui') link = 'pear://templates/desktop/electron'
      else if (link === 'node-compat') link = 'pear://templates/terminal/compat'
      else if (link[0] !== '.') {
        const stream = new Init({ ...opts, link: './' + link })
        return pipelinePromise(stream, this)
      } else throw ERR_INVALID_TEMPLATE('Invalid template link', { link })
    }
    if (link.startsWith('pear://templates')) ask = false

    let params = null
    if (isPear && ask) {
      if ((await ipc.trusted(link)) === false) {
        const { drive } = plink.parse(link)
        throw ERR_PERMISSION_REQUIRED('Permission required to use template', {
          key: drive.key
        })
      }
    }

    if (isPath) {
      let url = pathToFileURL(cwd).toString()
      if (url.slice(1) !== '/') url += '/'
      link = new URL(link, url).toString()
    }

    for await (const { tag, data } of dump(link + '/_template.json', {
      dir: '-'
    })) {
      if (tag === 'error' && data.code === 'ERR_PERMISSION_REQUIRED') {
        throw ERR_PERMISSION_REQUIRED(data.message, data.info)
      }
      if (tag !== 'file') continue
      try {
        const definition = JSON.parse(data.value)
        params = definition.params
        for (const prompt of params) {
          defaults[prompt.name] = Array.isArray(prompt.override)
            ? prompt.override.reduce((o, k) => o?.[k], pkg)
            : (prompt.default ?? defaults[prompt.name])
          if (typeof prompt.validation !== 'string') continue
          const realm = new Realm()
          prompt.validation = realm.evaluate(prompt.validation)
          realm.destroy()
        }
      } catch {
        params = null
      }
      break
    }
    if (params === null) throw ERR_INVALID_TEMPLATE('Invalid Template or Unreachable Link')
    const dst = new Localdrive(dir)
    if (force === false) {
      let empty = true
      for await (const entry of dst.list()) {
        if (entry) {
          empty = false
          break
        }
      }
      if (empty === false) throw ERR_DIR_NONEMPTY('Dir is not empty. To overwrite: --force')
    }
    const prompt = new Interact(header, params, { defaults })
    const { fields, shave } = await prompt.run({ autosubmit })
    this.push({ tag: 'writing' })
    const promises = []
    for await (const { tag, data } of dump(link, { dir: '-' })) {
      if (tag === 'error') {
        throw ERR_OPERATION_FAILED('Dump Failed: ' + data.stack)
      }
      if (tag !== 'file') continue
      const { key, value = null } = data
      if (key === '/_template.json') continue
      if (value === null) continue // dir
      const file = stamp.sync(key, fields)
      const fileStream = isTextFile(file) ? stamp.stream(value, fields, shave) : Readable.from([value])
      const writeStream = dst.createWriteStream(file)
      const promise = pipelinePromise(
        fileStream,
        writeStream
      )
      promise.catch((err) => {
        this.push({ tag: 'error', data: err })
      })
      promise.then(() => {
        this.push({ tag: 'wrote', data: { path: file } })
      })
      promises.push(promise)
    }

    const results = await Promise.allSettled(promises)
    const success = results.every(({ status }) => status === 'fulfilled')
    this.final = { success }
    this.push({ tag: 'written' })
  }
}

module.exports = function init(link, opts) {
  return new Init({ ...opts, link })
}
