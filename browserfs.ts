// @ts-nocheck
export class BrowserFS {
    ws: Websocket
    wrappedfile: File
    readcallback: Function

    constructor(pathname: string, pubfile: File, authtoken: string, activitycallback: Function) {
        this.mountready = new Promise((resolve, reject) => {
            this.resolvemount = resolve
        })
        this.wrappedfile = pubfile
        this.readcallback = activitycallback
        this.ws = new WebSocket('wss://' + window.location.host + '/browserfs/' + pathname)
        this.ws.binaryType = 'arraybuffer'
        this.ws.onmessage = this.process_reply
        this.ws.onopen = (e) => {
            this.ws.send(authtoken)
            let ab = new ArrayBuffer(8)
            let dv: Dataview = new DataView(ab)
            dv.setBigUint64(0, BigInt(pubfile.size))
            this.ws.send(ab)
            this.resolvemount()
        }
    }

    close = async () => {
        await this.ws.close()
    }

    process_reply = (req) => {

        var dvw = new DataView(req.data)
        let opcode = dvw.getUint32()
        if (opcode == 1) {
            if (this.readcallback) {
                this.readcallback(false)
            }
            let txid = Number(dvw.getUint32(4))
            let offset = Number(dvw.getBigUint64(8))
            let sz = Number(dvw.getBigUint64(16))
            let chnk = this.wrappedfile.slice(offset, offset + sz)
            let rsp = new ArrayBuffer(8)
            dvw = new DataView(rsp)
            dvw.setUint32(0, 2)
            dvw.setUint32(4, txid)
            const rspblob = new Blob([rsp, chnk])
            this.ws.send(rspblob)            
            if (this.readcallback) {
                this.readcallback(true)
            }
        }
    }
}
