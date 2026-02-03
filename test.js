'use strict'
const test = require('brittle')
const fs = require('fs')
const process = require('process')
const path = require('path')
const { fileURLToPath } = require('url')
const tmp = require('test-tmp')
const { Readable } = require('streamx')

const fsp = fs.promises
const cwd = process.cwd()
const fixturesDir = path.join(__dirname, 'test', 'fixtures', 'template')

const prevPear = global.Pear
const IPC = Symbol('IPC')
global.Pear = {
  app: { applink: 'pear://app' },
  config: { swapDir: cwd },
  constructor: { IPC, RTI: { checkout: 'test', mount: cwd } }
}
global.Pear[IPC] = {
  dump: ({ link }) => createDumpStream(link)
}

const init = require('./')

test('init merges json and stamps files', async (t) => {
  t.teardown(() => {
    global.Pear = prevPear
  })

  const root = await tmp(t)
  const destDir = path.join(root, 'dest')

  await fsp.mkdir(destDir, { recursive: true })

  await fsp.writeFile(
    path.join(destDir, 'config.json'),
    JSON.stringify(
      {
        existing: true,
        nested: { a: 1, b: 2 },
        arr: [1, 2],
        objArr: [{ a: 1 }]
      },
      null,
      2
    )
  )

  const stream = init(fixturesDir, {
    dir: destDir,
    cwd,
    defaults: {},
    pkg: {},
    force: true,
    header: ''
  })

  stream._interact = async function* () {
    yield [this.root, { fields: { name: 'Pear' } }]
  }

  const events = []
  for await (const evt of stream) events.push(evt)

  t.ok(events.some((evt) => evt.tag === 'wrote' && evt.data.path === 'config.json'))
  t.ok(events.some((evt) => evt.tag === 'wrote' && evt.data.path === 'hello.txt'))

  const hello = await fsp.readFile(path.join(destDir, 'hello.txt'), 'utf8')
  t.is(hello.trim(), 'hello Pear')

  const config = JSON.parse(await fsp.readFile(path.join(destDir, 'config.json'), 'utf8'))
  t.alike(config, {
    existing: true,
    nested: { a: 1, b: 3, c: 4 },
    arr: [9, 2],
    objArr: [{ a: 1, b: 2 }],
    fresh: true
  })
})

function toPath(link) {
  if (link.startsWith('file://')) return fileURLToPath(link)
  return link
}

function createDumpStream(link) {
  return new Readable({
    objectMode: true,
    async read(cb) {
      try {
        for await (const evt of dumpEntries(link)) this.push(evt)
        this.push(null)
        cb(null)
      } catch (err) {
        cb(err)
      }
    }
  })
}

async function* dumpEntries(link) {
  const targetPath = toPath(link)
  const stat = await fsp.stat(targetPath)

  if (stat.isDirectory()) {
    yield* walk(targetPath, targetPath)
    return
  }

  const value = await fsp.readFile(targetPath)
  yield { tag: 'file', data: { key: path.basename(targetPath), value } }
}

async function* walk(root, dir) {
  const entries = await fsp.readdir(dir, { withFileTypes: true })
  for (const entry of entries) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) {
      yield* walk(root, full)
      continue
    }
    if (entry.name === '_template.json') continue
    const rel = path.relative(root, full).split(path.sep).join('/')
    const value = await fsp.readFile(full)
    yield { tag: 'file', data: { key: rel, value } }
  }
}
