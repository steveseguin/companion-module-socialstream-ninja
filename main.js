import { InstanceBase, InstanceStatus, runEntrypoint } from '@companion-module/base'
import { getActions } from './actions.js'
import { getPresets } from './presets.js'
import { getVariables, updateVariables } from './variables.js'
import { getFeedbacks } from './feedbacks.js'
import { upgradeScripts } from './upgrades.js'

import WebSocket from 'ws'

class SocialStreamInstance extends InstanceBase {
	constructor(internal) {
		super(internal)

		this.updateVariables = updateVariables
	}

	async init(config) {
		this.updateStatus(InstanceStatus.Connecting)

		this.config = config

		this.connected = null
		this.states = {}

		this.initWebSocket()
		this.initActions()
		this.initFeedbacks()
		this.initVariables()
	}

	async destroy() {
		this.states = {}

		const ws = this.ws
		delete this.ws
		if (ws !== undefined) {
			ws.close(1000)
		}
		if (this.reconnect) {
			clearInterval(this.reconnect)
			delete this.reconnect
		}
	}

	getConfigFields() {
		return [
			{
				type: 'textinput',
				id: 'sessionID',
				label: 'Session ID',
				width: 6,
			},
		]
	}

	async configUpdated(config) {
		this.config = config
		if (this.reconnect) {
			clearInterval(this.reconnect)
			delete this.reconnect
		}
		this.initWebSocket()
	}

	initVariables() {
		const variables = getVariables.bind(this)()
		this.setVariableDefinitions(variables)
	}

	initFeedbacks() {
		const feedbacks = getFeedbacks.bind(this)()
		this.setFeedbackDefinitions(feedbacks)
	}

	initPresets() {
		const presets = getPresets.bind(this)()
		this.setPresetDefinitions(presets)
	}

	initActions() {
		const actions = getActions.bind(this)()
		this.setActionDefinitions(actions)
	}

	initWebSocket() {
		if (this.reconnect) {
			clearInterval(this.reconnect)
			delete this.reconnect
		}
		const previousWs = this.ws
		delete this.ws
		if (previousWs !== undefined) {
			previousWs.close(1000)
		}
		if (this.config.sessionID) {
			const ws = new WebSocket(`wss://io.socialstream.ninja:443`)
			this.ws = ws

			ws.on('open', () => {
				if (this.ws !== ws) return
				if (!this.connected) {
					this.log('info', `Connection opened to SocialStream.Ninja`)
					this.connected = true
				}
				this.updateStatus(InstanceStatus.Ok)
				this.ws.send(`{"join": "${this.config.sessionID}", "out":1, "in":2}`)
				this.sendRequest('getQueueSize')
			})

			ws.on('close', (code) => {
				if (this.ws !== ws) return
				if (code !== 1000 && code !== 1006) {
					this.connected = false
					this.log('debug', `Websocket closed:  ${code}`)
				}
				this.reconnect = setInterval(() => {
					if (this.ws !== ws) return
					this.initWebSocket()
				}, 1000)
			})

			ws.on('message', (data) => {
				if (this.ws !== ws) return
				this.messageReceivedFromWebSocket(data)
			})

			ws.on('error', (data) => {
				if (this.ws !== ws) return
				if (this.connected !== false) {
					this.connected = false
					this.updateStatus(InstanceStatus.ConnectionFailure)
					if (data?.code == 'ENOTFOUND') {
						this.log('error', `Unable to reach Social Stream Ninja`)
					} else {
						this.log('error', `WebSocket ${data}`)
					}
				}
				if (this.ws !== undefined) {
					this.ws.close()
				}
			})
		} else {
			this.log('warn', `Session ID required to connect to Social Stream Ninja, please add one in the module settings`)
			this.updateStatus(InstanceStatus.BadConfig, 'Missing Session ID')
		}
	}

	messageReceivedFromWebSocket(data) {
		let message = JSON.parse(data)
		//console.log(message)

		if (message?.queueLength > -1) {
			this.states.queueLength = message.queueLength
			this.setVariableValues({ queue_size: message.queueLength })
			this.checkFeedbacks('queueSize')
		} else if (message === false) {
			this.states.featured = false
			this.setVariableValues({
				featured_username: 'None',
				featured_message: 'None',
			})
			this.checkFeedbacks('featuredActive')
		} else if (message?.clicked) {
			this.states.featured = true
			this.setVariableValues({
				featured_username: message?.chatname ?? 'Unknown',
				featured_message: message?.chatmessage ?? 'Unknown',
			})
			this.checkFeedbacks('featuredActive')
		} else {
			//console.log(message)
		}
	}

	sendRequest(action, value, channel) {
		let message = {
			action: action ?? 'null',
			apiid: this.config.sessionID,
		}
		if (value !== null) {
			message.value = value
		}
		if (channel !== null) {
			message.out = channel
		}
		//console.log(JSON.stringify(message))
		this.ws.send(JSON.stringify(message))
	}
}

runEntrypoint(SocialStreamInstance, upgradeScripts)
