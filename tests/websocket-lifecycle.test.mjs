// Run with: node --experimental-vm-modules --test tests/websocket-lifecycle.test.mjs
// All transport, Companion host calls and timers stay in memory.
import assert from 'node:assert/strict'
import { EventEmitter } from 'node:events'
import { readFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import vm from 'node:vm'
import test from 'node:test'

const sourceRoot = process.env.COMPANION_SOURCE_ROOT || fileURLToPath(new URL('../', import.meta.url))

async function createHarness(sessionID = 'session-A') {
	let Instance
	let timerId = 0
	const timers = new Map()
	const sockets = []
	const closes = []
	class Base {
		constructor() {
			this.values = {}
			this.statuses = []
			this.feedbackChecks = []
		}
		updateStatus(...args) {
			this.statuses.push(args)
		}
		log() {}
		setVariableDefinitions() {}
		setVariableValues(values) {
			Object.assign(this.values, values)
		}
		setActionDefinitions(actions) {
			this.actions = actions
		}
		setFeedbackDefinitions(feedbacks) {
			this.feedbacks = feedbacks
		}
		setPresetDefinitions() {}
		checkFeedbacks(...args) {
			this.feedbackChecks.push(args)
		}
		async parseVariablesInString(value) {
			return value
		}
	}
	class Socket extends EventEmitter {
		constructor() {
			super()
			this.readyState = 0
			this.sent = []
			this.closeCalls = []
			sockets.push(this)
		}
		send(value) {
			assert.equal(this.readyState, 1, 'send requires an open socket')
			this.sent.push(JSON.parse(value))
		}
		close(code = 1000) {
			this.closeCalls.push(code)
			if (this.readyState >= 2) return
			this.readyState = 2
			closes.push(() => this.disconnect(code))
		}
		open() {
			this.readyState = 1
			this.emit('open')
		}
		disconnect(code = 1006) {
			this.readyState = 3
			this.emit('close', code)
		}
	}
	const context = vm.createContext({
		setInterval(callback) {
			const id = ++timerId
			timers.set(id, callback)
			return id
		},
		clearInterval(id) {
			timers.delete(id)
		},
	})
	// eslint-disable-next-line n/no-unsupported-features/node-builtins -- Tests explicitly enable --experimental-vm-modules.
	const base = new vm.SyntheticModule(
		['InstanceBase', 'InstanceStatus', 'runEntrypoint', 'combineRgb'],
		function () {
			this.setExport('InstanceBase', Base)
			this.setExport('InstanceStatus', {
				Connecting: 'Connecting',
				Ok: 'Ok',
				BadConfig: 'BadConfig',
				ConnectionFailure: 'ConnectionFailure',
			})
			this.setExport('runEntrypoint', (value) => {
				Instance = value
			})
			this.setExport('combineRgb', (r, g, b) => (r << 16) | (g << 8) | b)
		},
		{ context },
	)
	// eslint-disable-next-line n/no-unsupported-features/node-builtins -- Tests explicitly enable --experimental-vm-modules.
	const ws = new vm.SyntheticModule(
		['default'],
		function () {
			this.setExport('default', Socket)
		},
		{ context },
	)
	const cache = new Map()
	async function load(file) {
		if (cache.has(file)) return cache.get(file)
		// eslint-disable-next-line n/no-unsupported-features/node-builtins -- Tests explicitly enable --experimental-vm-modules.
		const module = new vm.SourceTextModule(await readFile(file, 'utf8'), { context, identifier: file })
		cache.set(file, module)
		await module.link((specifier) => {
			if (specifier === '@companion-module/base') return base
			if (specifier === 'ws') return ws
			assert(specifier.startsWith('./'))
			return load(path.resolve(path.dirname(file), specifier))
		})
		return module
	}
	await (await load(path.resolve(sourceRoot, 'main.js'))).evaluate()
	const instance = new Instance()
	await instance.init({ sessionID })
	return {
		instance,
		sockets,
		timers,
		flushCloses() {
			while (closes.length) closes.shift()()
		},
		tick() {
			for (const callback of [...timers.values()]) callback()
		},
	}
}

for (const code of [1000, 1001, 1006]) {
	test(`ordinary remote close ${code} reconnects once and remains stable`, async () => {
		const h = await createHarness()
		h.sockets[0].open()
		h.sockets[0].disconnect(code)
		assert.equal(h.timers.size, 1)
		h.tick()
		assert.equal(h.sockets.length, 2)
		h.sockets[1].open()
		h.flushCloses()
		h.tick()
		assert.equal(h.sockets.length, 2)
		assert.equal(h.timers.size, 0)
		assert.equal(h.sockets[1].sent[0].join, 'session-A')
	})
}

test('destroy ignores asynchronous socket close', async () => {
	const h = await createHarness()
	h.sockets[0].open()
	await h.instance.destroy()
	h.flushCloses()
	h.tick()
	assert.equal(h.sockets.length, 1)
	assert.equal(h.timers.size, 0)
	assert.equal(h.instance.ws, undefined)
})

test('destroy cancels pending retry and ignores its already captured callback', async () => {
	const h = await createHarness()
	h.sockets[0].open()
	h.sockets[0].disconnect()
	const pending = [...h.timers.values()][0]
	await h.instance.destroy()
	assert.equal(h.timers.size, 0)
	pending()
	assert.equal(h.sockets.length, 1)
})

test('configuration replacement does not reconnect from the old close', async () => {
	const h = await createHarness()
	h.sockets[0].open()
	await h.instance.configUpdated({ sessionID: 'session-B' })
	h.sockets[1].open()
	h.flushCloses()
	for (let i = 0; i < 3; i++) {
		h.tick()
		h.flushCloses()
	}
	assert.equal(h.sockets.length, 2)
	assert.equal(h.sockets[1].closeCalls.length, 0)
	assert.equal(h.sockets[1].sent[0].join, 'session-B')
	assert.equal(h.timers.size, 0)
})

test('clearing Session ID closes the prior connection and ignores its messages', async () => {
	const h = await createHarness()
	const old = h.sockets[0]
	old.open()
	await h.instance.configUpdated({ sessionID: '' })
	assert.equal(old.closeCalls.length, 1)
	assert.equal(h.instance.ws, undefined)
	old.emit('message', JSON.stringify({ queueLength: 8 }))
	assert.equal(h.instance.values.queue_size, undefined)
	h.flushCloses()
	h.tick()
	assert.equal(h.sockets.length, 1)
	assert.equal(h.timers.size, 0)
	assert.equal(h.instance.statuses.at(-1)[0], 'BadConfig')
})

test('blank configuration can subsequently connect to the new session', async () => {
	const h = await createHarness('')
	assert.equal(h.sockets.length, 0)
	await h.instance.configUpdated({ sessionID: 'session-B' })
	h.sockets[0].open()
	assert.equal(h.sockets[0].sent[0].join, 'session-B')
})

test('rapid configuration replacements ignore stale open and error events', async () => {
	const h = await createHarness()
	await h.instance.configUpdated({ sessionID: 'session-B' })
	await h.instance.configUpdated({ sessionID: 'session-C' })
	for (const old of h.sockets.slice(0, 2)) {
		old.emit('open')
		old.emit('error', new Error('old connection closed during setup'))
	}
	const current = h.sockets[2]
	assert.equal(current.closeCalls.length, 0)
	current.open()
	h.flushCloses()
	h.tick()
	assert.equal(h.sockets.length, 3)
	assert.equal(current.sent[0].join, 'session-C')
	assert.equal(h.timers.size, 0)
})

test('configuration change cancels a captured old retry', async () => {
	const h = await createHarness()
	h.sockets[0].open()
	h.sockets[0].disconnect()
	const pending = [...h.timers.values()][0]
	await h.instance.configUpdated({ sessionID: 'session-B' })
	pending()
	assert.equal(h.sockets.length, 2)
	assert.equal(h.sockets[1].closeCalls.length, 0)
})

test('stale messages cannot replace current session feedback', async () => {
	const h = await createHarness()
	const old = h.sockets[0]
	old.open()
	await h.instance.configUpdated({ sessionID: 'session-B' })
	const current = h.sockets[1]
	current.open()
	current.emit('message', JSON.stringify({ queueLength: 4 }))
	old.emit('message', JSON.stringify({ queueLength: 99 }))
	assert.equal(h.instance.values.queue_size, 4)
})

test('repeated destroy remains idle with delayed events', async () => {
	const h = await createHarness()
	const old = h.sockets[0]
	await h.instance.destroy()
	await h.instance.destroy()
	old.emit('open')
	old.emit('error', new Error('closed during setup'))
	old.emit('message', JSON.stringify({ queueLength: 99 }))
	h.flushCloses()
	h.tick()
	assert.equal(h.sockets.length, 1)
	assert.equal(h.timers.size, 0)
	assert.equal(h.instance.values.queue_size, undefined)
})
