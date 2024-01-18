export class BrowserFS {
    ws: Websocket
    constructor(pathname: string, pubfile: File, authtoken: string) {
        this.ws = new WebSocket('wss://' + window.location.host + '/browserfs/' + pathname)
        this.ws.onmessage = this.process_reply
        this.ws.onopen = (e) => {
            console.log(e)
            this.ws.send(authtoken)
            console.log(pubfile.size.toString())
            this.ws.send(pubfile.size.toString())
        }
    }

    process_reply(req) {
        console.log(req)

    }
}
