'use strict'
const { pipelinePromise } = require('streamx')
const { pathToFileURL } = require('url-file-url')
const Localdrive = require('localdrive')
const { Interact } = require('pear-terminal')
const Opstream = require('pear-opstream')
const stamp = require('pear-stamp')
const dump = require('pear-dump')
const plink = require('pear-link')
const PearError = require('pear-errors')
const { ERR_PERMISSION_REQUIRED, ERR_OPERATION_FAILED, ERR_DIR_NONEMPTY, ERR_INVALID_TEMPLATE } =
  PearError

class Init extends Opstream {
  constructor(...args) {
    super((...args) => this.#op(...args), ...args)
  }

  async #op(opts = {}) {
    const ipc = global.Pear?.[global.Pear?.constructor.IPC]
    const { dir, cwd, header, autosubmit, defaults, force = false, pkg } = opts
    this.defaults = defaults
    this.pkg = pkg
    let { link = 'default', ask = true } = opts
    const isPear = link.startsWith('pear://')
    const isFile = link.startsWith('file://')
    const isPath = link[0] === '.' || link[0] === '/' || link[1] === ':' || link.startsWith('\\')
    const isName = !isPear && !isFile && !isPath
    if (isName) {
      if (link === 'default') link = Pear.app.applink + '/projects/terminal/default'
      else if (link === 'ui') link = Pear.app.applink + '/projects/desktop/electron'
      else if (link === 'node-compat') link = Pear.app.applink + '/projects/terminal/node-compat'
      else if (link[0] !== '.') {
        const stream = new Init({ ...opts, link: './' + link })
        return pipelinePromise(stream, this)
      } else throw ERR_INVALID_TEMPLATE('Invalid template link', { link })
    }
    if (link.startsWith(Pear.app.applink)) ask = false
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
    this.root = link.endsWith('/') ? link : link + '/'
    const appRoot =
      Pear?.app?.applink && link.startsWith(Pear.app.applink)
        ? Pear.app.applink.endsWith('/')
          ? Pear.app.applink
          : Pear.app.applink + '/'
        : null
    this.base = appRoot || this.root
    this.cwl = this.root // current working link
    const params = await this.params(link)
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

    const prompt = new Interact(header, params, {
      defaults,
      load: async (key) => {
        return this._load(key, this.cwl)
      }
    })
    const promises = []
    for await (const [target, { fields }] of this._interact(link, prompt, { autosubmit })) {
      this.push({ tag: 'writing' })
      for await (const { tag, data } of dump(target, { dir: '-' })) {
        if (tag === 'error') {
          throw ERR_OPERATION_FAILED('Dump Failed: ' + data.stack)
        }
        if (tag !== 'file') continue
        let { key, value = null } = data
        if (key === '/_template.json') continue
        if (value === null) continue // dir
        const file = stamp.sync(key, fields)
        if (file.endsWith('.json')) {
          value = JSON.stringify(
            deepMerge(JSON.parse((await dst.get(file)) + ''), JSON.parse(value)),
            0,
            2
          )
        }

        const writeStream = dst.createWriteStream(file)
        const promise = pipelinePromise(stamp.stream(value, fields, []), writeStream)
        promise.catch((err) => {
          this.push({ tag: 'error', data: err })
        })
        promise.then(() => {
          const data = { path: file }
          this.push({ tag: 'wrote', data })
          return data
        })
        promises.push(promise)
      }
    }

    const results = await Promise.allSettled(promises)
    const success = results.every(({ status }) => status === 'fulfilled')
    this.final = { success, data: results.map(({ value }) => value) }
    this.push({ tag: 'written' })
  }

  async *_interact(link, prompt, opts) {
    const stack = [{ link, template: null, prefixTrail: null, fields: null }]
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
        if (frame.prefixTrail === null) frame.prefixTrail = trail
        continue
      }

      if (tag === 'enter' && typeof answer === 'string' && answer.startsWith('/')) {
        const parent = stack[stack.length - 1]
        if (parent.template === null) parent.template = parent.link
        if (parent.prefixTrail === null) parent.prefixTrail = trail.slice(0, -1) // parent scope
        parent.fields = parent.fields ?? {}

        if (!seen.has(parent.template)) {
          seen.add(parent.template)
          yield [parent.template, { trail: parent.prefixTrail, fields: parent.fields }]
        }
      }

      if (tag === 'input') {
        const frame = stack[stack.length - 1]
        if (frame.template === null) frame.template = frame.link
        if (frame.prefixTrail === null) frame.prefixTrail = trail.slice(0, -1)
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
          template: null,
          prefixTrail: null,
          fields: null,
          isMixin: typeof answer === 'string' && answer.startsWith('/')
        })
        continue
      }

      if (tag === 'exit') {
        const frame =
          stack.pop() || { link: resolved, template: null, prefixTrail: null, fields: null }

        if (frame.template === null) frame.template = frame.link
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

function deepMerge(a, b) {
  if (typeof a !== 'object' || a === null) return clone(b)
  if (typeof b !== 'object' || b === null) return clone(b)

  if (Array.isArray(a) || Array.isArray(b)) return mergeArray(a, b, deepMerge)

  const out = {}
  for (const k in a) {
    if (Object.hasOwnProperty.call(a, k) === false) continue
    out[k] = clone(a[k])
  }
  for (const k in b) {
    if (Object.hasOwnProperty.call(b, k) === false) continue
    const bv = b[k]
    if (Object.hasOwnProperty.call(out, k) === false) {
      out[k] = clone(bv)
      continue
    }
    const av = out[k]
    if (typeof av !== 'object' || av === null || typeof bv !== 'object' || bv === null) {
      out[k] = clone(bv)
    } else if (Array.isArray(av) || Array.isArray(bv)) {
      out[k] = mergeArray(av, bv, deepMerge)
    } else {
      out[k] = deepMerge(av, bv)
    }
  }
  return out
}

function mergeArray(a, b, fn) {
  const aIsArr = Array.isArray(a)
  const bIsArr = Array.isArray(b)
  if (!aIsArr && !bIsArr) return clone(b)
  if (!aIsArr) return clone(b)
  if (!bIsArr) return clone(b)

  // merges by keys (incl. sparse/extra props); b overwrites; objects recurse
  const keys = Object.keys(a)
  const keysB = Object.keys(b)
  for (let i = 0; i < keysB.length; i++) keys.push(keysB[i])

  const seen = Object.create(null)
  const uniq = []
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]
    if (seen[k]) continue
    seen[k] = true
    uniq.push(k)
  }

  const out = new Array(Math.max(a.length, b.length))
  for (let i = 0; i < uniq.length; i++) {
    const k = uniq[i]
    if (Object.hasOwnProperty.call(b, k)) {
      const bv = b[k]
      if (Object.hasOwnProperty.call(a, k)) {
        const av = a[k]
        if (typeof av === 'object' && av !== null && typeof bv === 'object' && bv !== null) {
          out[k] = fn(av, bv)
        } else {
          out[k] = clone(bv)
        }
      } else {
        out[k] = clone(bv)
      }
    } else {
      out[k] = clone(a[k])
    }
  }
  return out
}

function clone(o) {
  if (typeof o !== 'object' || o === null) return o
  if (Array.isArray(o)) return cloneArray(o, clone)
  const o2 = {}
  for (const k in o) {
    if (Object.hasOwnProperty.call(o, k) === false) continue
    const cur = o[k]
    if (typeof cur !== 'object' || cur === null) {
      o2[k] = cur
    } else {
      o2[k] = clone(cur)
    }
  }
  return o2
}

function cloneArray(a, fn) {
  const keys = Object.keys(a)
  const a2 = new Array(keys.length)
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]
    const cur = a[k]
    if (typeof cur !== 'object' || cur === null) {
      a2[k] = cur
    } else {
      a2[k] = fn(cur)
    }
  }
  return a2
}
