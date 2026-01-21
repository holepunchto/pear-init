'use strict'
const { Readable, pipelinePromise } = require('streamx')
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
      const fileStream = isTextFile(file)
        ? stamp.stream(value, fields, shave)
        : Readable.from([value])
      const writeStream = dst.createWriteStream(file)
      const promise = pipelinePromise(fileStream, writeStream)
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

  async *_interact(link, prompt, opts) {
    const stack = [{ link }]
    const seen = new Set()

    for await (const evt of prompt.run(opts)) {
      if (!evt || !evt.data) continue
      const { tag, data } = evt
      const { trail, answer } = data
      if (!Array.isArray(trail) || answer === undefined) continue

      let resolved = answer
      if ((tag === 'enter' || tag === 'exit') && typeof answer === 'string') {
        const baseForResolve =
          tag === 'exit'
            ? stack.length > 1
              ? stack[stack.length - 2].link
              : stack[0].link
            : stack[stack.length - 1].link || link

        let base = answer.startsWith('/') ? this.base : baseForResolve
        if (!base.endsWith('/')) base += '/'

        const rel = answer.startsWith('/') ? answer.slice(1) : answer
        resolved = new URL(rel, base).href
      }

      if (tag === 'select' && typeof answer === 'string') {
        let base = answer.startsWith('/') ? this.base : stack[stack.length - 1].link || link
        if (!base.endsWith('/')) base += '/'

        const rel = answer.startsWith('/') ? answer.slice(1) : answer
        const frame = stack[stack.length - 1]
        frame.template = new URL(rel, base).href
        if (frame.prefixTrail == null) frame.prefixTrail = trail
        continue
      }

      if (tag === 'enter' && typeof answer === 'string' && answer.startsWith('/')) {
        const parent = stack[stack.length - 1]
        if (parent.template == null) parent.template = parent.link
        if (parent.prefixTrail == null) parent.prefixTrail = trail.slice(0, -1) // parent scope
        parent.fields = parent.fields ?? {}

        if (!seen.has(parent.template)) {
          seen.add(parent.template)
          yield [parent.template, { trail: parent.prefixTrail, fields: parent.fields }]
        }
      }

      if (tag === 'input') {
        const frame = stack[stack.length - 1]
        if (frame.template == null) frame.template = frame.link
        if (frame.prefixTrail == null) frame.prefixTrail = trail.slice(0, -1)
        frame.fields = frame.fields ?? {}

        const prefix = frame.prefixTrail || []
        const relTrail =
          trail.length >= prefix.length && prefix.every((v, i) => trail[i] === v)
            ? trail.slice(prefix.length)
            : trail

        let cur = frame.fields
        for (let i = 0; i < relTrail.length; i++) {
          const k = relTrail[i]
          const last = i === relTrail.length - 1
          if (last) {
            cur[k] = answer
          } else {
            const next = cur[k]
            if (!next || typeof next !== 'object') cur[k] = {}
            cur = cur[k]
          }
        }
      }

      if (tag === 'enter') {
        stack.push({
          link: resolved,
          isMixin: typeof answer === 'string' && answer.startsWith('/')
        })
        continue
      }

      if (tag === 'exit') {
        const frame = stack.pop() || { link: resolved }

        if (frame.template == null) frame.template = frame.link
        frame.fields = frame.fields ?? {}

        if (frame.isMixin && !seen.has(frame.template)) {
          seen.add(frame.template)
          yield [frame.template, { trail: frame.prefixTrail || trail, fields: frame.fields }]
        }

        if (!frame.isMixin && stack.length === 0 && !seen.has(frame.template)) {
          seen.add(frame.template)
          yield [frame.template, { trail: frame.prefixTrail || trail, fields: frame.fields }]
        }

        continue
      }
    }
  }

  async _load(key, link = this.root) {
    const isAbs = key[0] === '/'
    key = isAbs ? key.slice(1) : key
    const base = isAbs ? this.base : this.root
    key = key[0] === '.' ? new URL(key, link).href : new URL(key, base).href
    if (key.endsWith('/') === false) key += '/'
    this.cwl = key
    for await (const { tag, data } of dump(key + '_template.json', {
      dir: '-'
    })) {
      if (tag !== 'file') continue
      return JSON.parse(data.value)
    }
  }

  async params(link) {
    const { defaults, pkg } = this
    let params = null
    for await (const { tag, data } of dump(link + '/_template.json', {
      dir: '-'
    })) {
      if (tag === 'error' && data.code === 'ERR_PERMISSION_REQUIRED') {
        throw ERR_PERMISSION_REQUIRED(data.message, data.info)
      }

      if (tag !== 'file') continue
      try {
        const definition = JSON.parse(data.value)
        params =
          Array.isArray(definition) || typeof definition === 'string'
            ? definition
            : definition.params
        for (const prompt of params) {
          defaults[prompt.name] = Array.isArray(prompt.override)
            ? prompt.override.reduce((o, k) => o?.[k], pkg)
            : (prompt.default ?? defaults[prompt.name])
          if (typeof prompt.validation !== 'string') continue
          const realm = new Realm()
          prompt.validation = realm.evaluate(prompt.validation)
        }
      } catch (e) {
        params = null
      }
      break
    }

    return params
  }
}

module.exports = function init(link, opts) {
  return new Init({ ...opts, link })
}
